import json
import os

import pytest

pyshim = None
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db = tmp_path / "test_api.db"
    monkeypatch.setenv("INTERLOCKING_DB", str(db))
    # 重新载入模块以使用新的 DB 路径
    import importlib
    import app.db as dbmod
    importlib.reload(dbmod)
    import app.interlocking as ilmod
    importlib.reload(ilmod)
    import app.main as mainmod
    importlib.reload(mainmod)
    mainmod.init_db = dbmod.init_db
    with TestClient(mainmod.app) as c:
        yield c


@pytest.fixture()
def yard_dict():
    path = os.path.join(os.path.dirname(__file__), "..", "examples", "yard.json")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def test_full_flow_and_state(client, yard_dict):
    # 健康检查与首页
    assert client.get("/api/health").json()["status"] == "ok"
    assert "联锁推演台" in client.get("/").text

    # 非法导入
    bad = dict(yard_dict)
    bad["routes"] = [{"id": "X", "sections": ["NOPE"], "switches": {}, "flank": []}]
    r = client.post("/api/yard", json=bad)
    assert r.status_code == 422

    # 正常导入
    assert client.post("/api/yard", json=yard_dict).status_code == 200

    # 放行
    r = client.post("/api/routes/request", json={"route_id": "R-MAIN"})
    assert r.json()["signal"] == "green"
    r2 = client.post("/api/routes/request", json={"route_id": "R-SIDING"})
    assert r2.json()["signal"] == "red"
    assert r2.json()["conflicts"]

    # 完整跑一趟
    seq = 1
    dets = ["D0", "D1", "D2", "D3"]
    for i, d in enumerate(dets):
        assert client.post("/api/events", json={"seq": seq, "detector_id": d, "kind": "occupy"}).status_code == 200
        seq += 1
        if i > 0:
            assert client.post("/api/events", json={"seq": seq, "detector_id": dets[i - 1], "kind": "clear"}).status_code == 200
            seq += 1
    fin = client.post("/api/events", json={"seq": seq, "detector_id": "D3", "kind": "clear"}).json()
    assert fin["processed"][-1]["state"]["event"] == "complete"

    state = client.get("/api/state").json()
    assert state["next_seq"] == 9
    assert state["instances"][-1]["status"] == "completed"

    # 回放
    rep = client.get("/api/replay?after_id=0&limit=10").json()
    assert rep["has_more"]
    kinds = {d["kind"] for d in rep["items"]}
    assert {"import", "approval", "event", "complete"} & kinds

    # 裁决分页
    page = client.get("/api/decisions?limit=3").json()
    assert len(page["items"]) == 3 and page["has_more"]


def test_gap_and_fault_api(client, yard_dict):
    client.post("/api/yard", json=yard_dict)
    client.post("/api/routes/request", json={"route_id": "R-MAIN"})
    # 缺号：#2 为 D1 越序出清（尚未占用），先缓冲
    assert client.post("/api/events", json={"seq": 2, "detector_id": "D1", "kind": "clear"}).json()["buffered"]
    denied = client.post("/api/routes/request", json={"route_id": "R-MAIN"})
    assert denied.status_code == 409 and denied.json()["error"]["code"] == "sequence_gap"
    # #1 正常占用后排空缓冲，#2 越序出清触发故障锁闭
    client.post("/api/events", json={"seq": 1, "detector_id": "D0", "kind": "occupy"})
    state = client.get("/api/state").json()
    faulted = [i for i in state["instances"] if i["status"] == "faulted"]
    assert faulted, "排空缓冲后应出现故障实例"
    iid = faulted[0]["id"]
    # 占用中不能复位
    blocked = client.post("/api/reset", json={"instance_id": iid})
    assert blocked.status_code == 409
