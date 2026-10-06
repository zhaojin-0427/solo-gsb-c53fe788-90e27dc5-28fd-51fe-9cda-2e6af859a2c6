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


def test_pending_queue_api(client, yard_dict):
    client.post("/api/yard", json=yard_dict)
    # 空闲时提交待行 -> 立即自动放行
    r = client.post("/api/pending", json={"route_id": "R-MAIN"}).json()
    assert r["status"] == "granted" and r["instance_id"]
    # 冲突时提交 -> 等待；重复提交幂等返回原申请
    r2 = client.post("/api/pending", json={"route_id": "R-SIDING"}).json()
    assert r2["status"] == "waiting"
    dup = client.post("/api/pending", json={"route_id": "R-SIDING"}).json()
    assert dup["duplicate"] is True and dup["id"] == r2["id"]
    # 快照包含队列
    state = client.get("/api/state").json()
    assert [p["status"] for p in state["pending"]] == ["granted", "waiting"]
    # 未决待行项存在时拒绝重导站场（此处同时有活动实例，具体码以运行态为准；
    # 仅有未决待行项时的 pending_waiting 拒绝见单元测试）
    busy = client.post("/api/yard", json=yard_dict)
    assert busy.status_code == 409
    # 主进路跑完后等待项自动放行
    seq = 1
    dets = ["D0", "D1", "D2", "D3"]
    for i, d in enumerate(dets):
        client.post("/api/events", json={"seq": seq, "detector_id": d, "kind": "occupy"})
        seq += 1
        if i > 0:
            client.post("/api/events", json={"seq": seq, "detector_id": dets[i - 1], "kind": "clear"})
            seq += 1
    fin = client.post("/api/events", json={"seq": seq, "detector_id": "D3", "kind": "clear"}).json()
    assert [g["pending_id"] for g in fin["granted_pending"]] == [r2["id"]]
    state = client.get("/api/state").json()
    assert [p["status"] for p in state["pending"]] == ["granted", "granted"]
    # 已放行项不可撤销
    r3 = client.post("/api/pending/cancel", json={"pending_id": r2["id"]})
    assert r3.status_code == 409


def test_pending_cancel_api(client, yard_dict):
    client.post("/api/yard", json=yard_dict)
    client.post("/api/routes/request", json={"route_id": "R-MAIN"})
    r = client.post("/api/pending", json={"route_id": "R-SIDING"}).json()
    assert r["status"] == "waiting"
    c = client.post("/api/pending/cancel", json={"pending_id": r["id"]}).json()
    assert c["status"] == "cancelled"
    again = client.post("/api/pending/cancel", json={"pending_id": r["id"]})
    assert again.status_code == 409
    state = client.get("/api/state").json()
    assert state["pending"][0]["status"] == "cancelled"
    # 未知进路 404
    assert client.post("/api/pending", json={"route_id": "GHOST"}).status_code == 404
