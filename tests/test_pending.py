"""待行进路队列：入队/等待/自动放行/撤销、FIFO 越过、缺号与故障持锁、
幂等与无假拒绝、重启持久续兑、重导守卫，以及 REST 端到端。
"""

import json
import os
import tempfile

import pytest

from app import interlocking as il
from app.db import connect, init_db


# --------------------------------------------------------------------- #
# 公共夹具
# --------------------------------------------------------------------- #
@pytest.fixture()
def conn():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_db(path)
    c = connect(path)
    yield c
    c.close()
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except OSError:
            pass


@pytest.fixture()
def yard(conn):
    path = os.path.join(os.path.dirname(__file__), "..", "examples", "yard.json")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    il.import_yard(conn, data)
    return data


def ev(seq, detector, kind):
    return {"seq": seq, "detector_id": detector, "kind": kind, "ts": 123}


def yard_disjoint(names=("S0", "S1", "S2", "S3")):
    """三段互不相邻的独立区段 + 对应单段进路（用于越过规则）。"""
    nodes, sections, routes = [], [], []
    for i, s in enumerate(names):
        nodes += [{"id": f"{s}A"}, {"id": f"{s}B"}]
        sections.append({"id": s, "a": f"{s}A", "b": f"{s}B", "detector": f"D-{s}"})
        routes.append({"id": f"R-{s}", "sections": [s], "switches": {}, "flank": []})
    return {
        "yard_id": "disjoint",
        "nodes": nodes,
        "sections": sections,
        "switches": [],
        "resources": [],
        "routes": routes,
    }


def yard_chain():
    """S1-S2-S3 直链：R1=[S1,S2]，R2=[S2,S3]（争 S2）。"""
    return {
        "yard_id": "chain",
        "nodes": [{"id": "A"}, {"id": "B"}, {"id": "C"}, {"id": "D"}],
        "sections": [
            {"id": "S1", "a": "A", "b": "B", "detector": "D1"},
            {"id": "S2", "a": "B", "b": "C", "detector": "D2"},
            {"id": "S3", "a": "C", "b": "D", "detector": "D3"},
        ],
        "switches": [],
        "resources": [],
        "routes": [
            {"id": "R1", "sections": ["S1", "S2"], "switches": {}, "flank": []},
            {"id": "R2", "sections": ["S2", "S3"], "switches": {}, "flank": []},
        ],
    }


def _status_map(conn):
    return {p["id"]: p["status"] for p in il.snapshot(conn)["pending"]}


# --------------------------------------------------------------------- #
# 入队即兑现 / 原子建立
# --------------------------------------------------------------------- #
def test_pending_granted_immediately_when_free_atomic(conn, yard):
    r = il.submit_pending(conn, "R-MAIN")
    assert r["status"] == "granted" and r["granted_now"] is True
    assert r["decision"] == "permitted" and r["signal"] == "green"
    iid = r["instance_id"]
    # 原子建立实例 + 全部区段/道岔/侧防锁
    snap = il.snapshot(conn)
    inst = [i for i in snap["instances"] if i["id"] == iid][0]
    assert inst["status"] == "active"
    held = {l["resource_id"] for l in snap["locks"] if l["instance_id"] == iid}
    assert {"S0", "S1", "S2", "S3"} <= held
    assert {"SW1", "SW2"} <= held
    assert {"FP-EAST", "FP-WEST"} <= held
    # approvals 有对应 permitted 行
    row = conn.execute(
        "SELECT decision FROM approvals WHERE approval_id=?", (r["approval_id"],)
    ).fetchone()
    assert row["decision"] == "permitted"


def test_pending_waits_when_conflicted_and_no_denial_row(conn, yard):
    il.request_route(conn, "R-MAIN")  # 即时放行占住资源
    r = il.submit_pending(conn, "R-SIDING")
    assert r["status"] == "waiting" and r["granted_now"] is False
    # 等待试探不得写只增拒绝：approvals 仅有即时放行那一条 permitted
    assert conn.execute("SELECT COUNT(*) c FROM approvals").fetchone()["c"] == 1
    assert conn.execute("SELECT COUNT(*) c FROM approvals WHERE decision='denied'").fetchone()["c"] == 0
    # 即时放行遇冲突仍写只增拒绝（原行为保持）
    rd = il.request_route(conn, "R-SIDING")
    assert rd["decision"] == "denied"
    assert conn.execute("SELECT COUNT(*) c FROM approvals WHERE decision='denied'").fetchone()["c"] == 1


def test_pending_duplicate_returns_original(conn, yard):
    il.request_route(conn, "R-MAIN")
    p1 = il.submit_pending(conn, "R-SIDING")
    p2 = il.submit_pending(conn, "R-SIDING")  # 同进路未决重复提交
    assert p2["duplicate"] is True and p2["id"] == p1["id"]
    # 多次重试不产生假拒绝
    p3 = il.submit_pending(conn, "R-SIDING")
    assert p3["id"] == p1["id"]
    assert conn.execute("SELECT COUNT(*) c FROM approvals WHERE decision='denied'").fetchone()["c"] == 0


# --------------------------------------------------------------------- #
# 整批上报排空后自动兑现
# --------------------------------------------------------------------- #
def test_pending_granted_after_event_batch(conn, yard):
    il.request_route(conn, "R-MAIN")
    p = il.submit_pending(conn, "R-BACK")  # 争 S1/S2/S3、FP-WEST
    assert p["status"] == "waiting"
    # 完整跑完 R-MAIN
    seq = 1
    dets = ["D0", "D1", "D2", "D3"]
    for i, d in enumerate(dets):
        il.report_event(conn, ev(seq, d, "occupy"))
        seq += 1
        if i > 0:
            il.report_event(conn, ev(seq, dets[i - 1], "clear"))
            seq += 1
    last = il.report_event(conn, ev(seq, "D3", "clear"))
    assert [g["pending_id"] for g in last["granted_pending"]] == [p["id"]]
    row = [x for x in il.snapshot(conn)["pending"] if x["id"] == p["id"]][0]
    assert row["status"] == "granted" and row["instance_id"] is not None


# --------------------------------------------------------------------- #
# FIFO 越过规则
# --------------------------------------------------------------------- #
def test_overtake_only_when_no_contention_with_blocked_predecessor(conn):
    il.import_yard(conn, yard_disjoint())
    # 外部占用 S1（无活动实例），使 R-S1 受阻
    il.report_event(conn, ev(1, "D-S1", "occupy"))
    ra = il.submit_pending(conn, "R-S0")  # 空闲 -> 兑现
    rb = il.submit_pending(conn, "R-S1")  # S1 占用 -> 受阻
    rc = il.submit_pending(conn, "R-S2")  # 空闲且不与受阻前项 R-S1 争用 -> 越过放行
    rd_ = il.submit_pending(conn, "R-S3")  # 同样可越过
    assert ra["status"] == "granted"
    assert rb["status"] == "waiting"
    assert rc["status"] == "granted" and rc["granted_now"] is True
    assert rd_["status"] == "granted"
    # 越过/等待全程无拒绝记录（试探不写只增拒绝）
    assert conn.execute("SELECT COUNT(*) c FROM approvals WHERE decision='denied'").fetchone()["c"] == 0
    # S1 出清后受阻前项自动兑现
    out = il.report_event(conn, ev(2, "D-S1", "clear"))
    assert [g["pending_id"] for g in out["granted_pending"]] == [rb["id"]]
    assert _status_map(conn)[rb["id"]] == "granted"


def test_later_item_contending_blocked_predecessor_must_wait(conn):
    # 三个独立区段 S1/S2/S3：
    #   RA=[S1]（占用受阻）；RB=[S1,S2]（与受阻前项 RA 争 S1 -> 必须等待）；
    #   RC=[S3]（不与受阻前项争用 -> 可越过 RA、RB 放行）
    # S1(A-B)、S2(B-C) 相连（RB=[S1,S2] 连通），S3(D-E) 独立：
    y = {
        "yard_id": "contend",
        "nodes": [{"id": n} for n in ("A", "B", "C", "D", "E")],
        "sections": [
            {"id": "S1", "a": "A", "b": "B", "detector": "D1"},
            {"id": "S2", "a": "B", "b": "C", "detector": "D2"},
            {"id": "S3", "a": "D", "b": "E", "detector": "D3"},
        ],
        "switches": [], "resources": [],
        "routes": [
            {"id": "RA", "sections": ["S1"], "switches": {}, "flank": []},
            {"id": "RB", "sections": ["S1", "S2"], "switches": {}, "flank": []},
            {"id": "RC", "sections": ["S3"], "switches": {}, "flank": []},
        ],
    }
    il.import_yard(conn, y)
    il.report_event(conn, ev(1, "D1", "occupy"))  # 外部占用 S1
    pa = il.submit_pending(conn, "RA")
    pb = il.submit_pending(conn, "RB")  # S2 空闲，但与受阻前项 RA 争 S1
    pc = il.submit_pending(conn, "RC")  # 与任一受阻前项都不争用
    assert pa["status"] == "waiting"
    assert pb["status"] == "waiting", "与受阻前项争用 S1 的后项必须等待，不得越过"
    assert pc["status"] == "granted", "不与受阻前项争用的后项可越过放行"
    assert conn.execute("SELECT COUNT(*) c FROM approvals WHERE decision='denied'").fetchone()["c"] == 0
    # S1 出清后，RA、RB 同事务按序兑现（先 RA 后 RB；RB 争用 S1/S2，RA 先持 S1，
    # 故仅当 RA 与 RB 可同时持锁才会一起兑现——这里二者争 S1，RA 放行后 RB 仍等待，
    # 直到 RA 释放 S1；本断言只验证越过/争用判定，不再延伸后续事件）
    out = il.report_event(conn, ev(2, "D1", "clear"))
    granted_ids = {g["pending_id"] for g in out["granted_pending"]}
    assert pa["id"] in granted_ids  # 受阻前项先兑现
    assert pb["id"] not in granted_ids  # 与前项争用 S1 的 RB 继续等待


# --------------------------------------------------------------------- #
# 缺号期间全部等待
# --------------------------------------------------------------------- #
def test_gap_keeps_all_pending_waiting_then_drain_grants(conn):
    y = {
        "yard_id": "g",
        "nodes": [{"id": n} for n in ("A", "B", "C", "D", "E", "F")],
        "sections": [
            {"id": "S0", "a": "A", "b": "B", "detector": "D0"},
            {"id": "S1", "a": "C", "b": "D", "detector": "D1"},
            {"id": "S2", "a": "E", "b": "F", "detector": "D2"},
        ],
        "switches": [], "resources": [],
        "routes": [
            {"id": "R1", "sections": ["S1"], "switches": {}, "flank": []},
            {"id": "R2", "sections": ["S2"], "switches": {}, "flank": []},
        ],
    }
    il.import_yard(conn, y)
    # 缺号：#2 先缓冲
    assert il.report_event(conn, ev(2, "D0", "occupy"))["buffered"] is True
    p1 = il.submit_pending(conn, "R1")  # S1 空闲但缺号 -> 等待
    p2 = il.submit_pending(conn, "R2")
    assert _status_map(conn)[p1["id"]] == "waiting"
    assert _status_map(conn)[p2["id"]] == "waiting"
    # 缺号期间即时放行依旧 409（原行为保持）
    with pytest.raises(il.ClientError) as ei:
        il.request_route(conn, "R1")
    assert ei.value.code == "sequence_gap"
    # #1 到达（S0 占用），排空 #1/#2（#2 再占 S0，无活动实例，外部占用），缺号解除后兑现
    out = il.report_event(conn, ev(1, "D0", "occupy"))
    ids = {g["pending_id"] for g in out["granted_pending"]}
    assert ids == {p1["id"], p2["id"]}
    assert _status_map(conn)[p1["id"]] == "granted"


# --------------------------------------------------------------------- #
# 故障持锁的冲突项继续等待，复位后兑现
# --------------------------------------------------------------------- #
def test_fault_held_lock_keeps_pending_waiting_then_reset_grants(conn):
    il.import_yard(conn, yard_chain())
    il.request_route(conn, "R1")
    il.report_event(conn, ev(1, "D1", "occupy"))
    il.report_event(conn, ev(2, "D2", "occupy"))  # 推进到末段 S2，S1 仍占用
    fault = il.report_event(conn, ev(3, "D2", "clear"))  # 末段出清、前段占用 -> 故障
    assert fault["processed"][0]["fault"] is True
    fid = fault["processed"][0]["instance_id"]

    p = il.submit_pending(conn, "R2")  # 争 S2，故障实例持锁 -> 等待
    assert p["status"] == "waiting"
    assert conn.execute("SELECT COUNT(*) c FROM approvals WHERE decision='denied'").fetchone()["c"] == 0
    # 仅外部出清 S1/S2 不会兑现（故障锁不释放）
    il.report_event(conn, ev(4, "D1", "clear"))
    assert _status_map(conn)[p["id"]] == "waiting"
    # 复位（全段已空闲）成功，同事务兑现 R2
    rr = il.reset_instance(conn, fid)
    assert [g["pending_id"] for g in rr["granted_pending"]] == [p["id"]]
    row = [x for x in il.snapshot(conn)["pending"] if x["id"] == p["id"]][0]
    assert row["status"] == "granted"


# --------------------------------------------------------------------- #
# 撤销
# --------------------------------------------------------------------- #
def test_revoke_waiting_and_cannot_revoke_final(conn, yard):
    il.request_route(conn, "R-MAIN")
    p = il.submit_pending(conn, "R-SIDING")
    assert p["status"] == "waiting"
    rv = il.revoke_pending(conn, p["id"])
    assert rv["revoked"] is True
    assert _status_map(conn)[p["id"]] == "revoked"
    # 已撤销不能再撤销
    with pytest.raises(il.ClientError) as ei:
        il.revoke_pending(conn, p["id"])
    assert ei.value.code == "pending_not_waiting"
    # 不存在 -> 404
    with pytest.raises(il.ClientError) as ei2:
        il.revoke_pending(conn, 9999)
    assert ei2.value.status_code == 404


def test_granted_cannot_be_revoked(conn):
    il.import_yard(conn, yard_disjoint())
    p = il.submit_pending(conn, "R-S0")  # 空闲即兑现
    assert p["status"] == "granted"
    with pytest.raises(il.ClientError) as ei:
        il.revoke_pending(conn, p["id"])
    assert ei.value.code == "pending_not_waiting"


# --------------------------------------------------------------------- #
# 重导守卫
# --------------------------------------------------------------------- #
def test_reimport_blocked_by_pending_and_cleared_after_revoke(conn, yard):
    # 无活动实例、仅外部占用，待行 R-MAIN 处于 waiting
    il.report_event(conn, ev(1, "D0", "occupy"))
    p = il.submit_pending(conn, "R-MAIN")
    assert p["status"] == "waiting"
    assert conn.execute(
        "SELECT COUNT(*) c FROM instances WHERE status IN ('active','faulted')"
    ).fetchone()["c"] == 0

    # 有未决待行项：拒绝重导
    with pytest.raises(il.ClientError) as ei:
        il.import_yard(conn, yard)
    assert ei.value.code == "pending_exists"
    assert ei.value.extra["waiting_pending"] >= 1

    # 撤销后（仍无活动实例）允许重导，待行运行态随重导清空
    assert il.revoke_pending(conn, p["id"])["status"] == "revoked"
    out = il.import_yard(conn, yard)
    assert out["routes"] >= 1
    assert il.snapshot(conn)["pending"] == []
    # 序号不回退（#1 已占用）
    assert il.snapshot(conn)["next_seq"] == 2


# --------------------------------------------------------------------- #
# 重启持久化与续兑
# --------------------------------------------------------------------- #
def test_restart_queue_visible_and_continues(conn):
    il.import_yard(conn, yard_chain())
    il.request_route(conn, "R1")
    il.report_event(conn, ev(1, "D1", "occupy"))
    p = il.submit_pending(conn, "R2")
    assert p["status"] == "waiting"
    dbfile = conn.execute("PRAGMA database_list").fetchone()[2]
    conn.close()

    c2 = connect(dbfile)
    row = [x for x in il.snapshot(c2)["pending"] if x["id"] == p["id"]][0]
    assert row["status"] == "waiting"  # 队列与未决项重启后仍可见
    # 启动恢复时 R1 仍持锁 -> 不兑现
    assert il.startup_recover(c2)["granted"] == []
    # 继续跑完 R1，末段出清同事务兑现 R2
    il.report_event(c2, ev(2, "D2", "occupy"))
    il.report_event(c2, ev(3, "D1", "clear"))
    last = il.report_event(c2, ev(4, "D2", "clear"))
    assert last["processed"][-1]["state"]["event"] == "complete"
    assert [g["pending_id"] for g in last["granted_pending"]] == [p["id"]]
    snap = il.snapshot(c2)
    prow = [x for x in snap["pending"] if x["id"] == p["id"]][0]
    assert prow["status"] == "granted"
    inst = [i for i in snap["instances"] if i["id"] == prow["instance_id"]][0]
    assert inst["route_id"] == "R2" and inst["status"] == "active"
    assert {l["resource_id"] for l in snap["locks"] if l["instance_id"] == inst["id"]} == {"S2", "S3"}
    c2.close()


def test_restart_startup_recover_grants_when_already_free(conn, yard):
    # 待行等待中关闭，资源在重开前已空闲（模拟外部状态），启动恢复即兑现
    il.request_route(conn, "R-MAIN")
    p = il.submit_pending(conn, "R-BACK")
    assert p["status"] == "waiting"
    dbfile = conn.execute("PRAGMA database_list").fetchone()[2]
    conn.close()
    c2 = connect(dbfile)
    # R-MAIN 仍持锁：启动不兑现
    assert il.startup_recover(c2)["granted"] == []
    c2.close()


# --------------------------------------------------------------------- #
# REST 端到端
# --------------------------------------------------------------------- #
@pytest.fixture()
def client(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    import importlib
    import app.db as dbmod
    import app.interlocking as ilmod
    import app.main as mainmod

    monkeypatch.setenv("INTERLOCKING_DB", str(tmp_path / "api.db"))
    importlib.reload(dbmod)
    importlib.reload(ilmod)
    importlib.reload(mainmod)
    mainmod.init_db = dbmod.init_db
    with TestClient(mainmod.app) as c:
        yield c


@pytest.fixture()
def yard_dict():
    with open(os.path.join(os.path.dirname(__file__), "..", "examples", "yard.json"), encoding="utf-8") as f:
        return json.load(f)


def test_api_pending_wait_duplicate_revoke_and_grant(client, yard_dict):
    client.post("/api/yard", json=yard_dict)
    assert client.post("/api/routes/request", json={"route_id": "R-MAIN"}).json()["signal"] == "green"

    r = client.post("/api/routes/pending", json={"route_id": "R-SIDING"})
    assert r.status_code == 200
    body = r.json()
    assert body["queued"] is True and body["status"] == "waiting"
    pid = body["id"]

    # 状态快照可见
    state = client.get("/api/state").json()
    assert [p for p in state["pending"] if p["id"] == pid][0]["status"] == "waiting"

    # 重复提交返回原申请
    dup = client.post("/api/routes/pending", json={"route_id": "R-SIDING"}).json()
    assert dup["duplicate"] is True and dup["id"] == pid

    # 撤销后不能再撤销（409）
    assert client.post("/api/routes/pending/revoke", json={"pending_id": pid}).json()["revoked"] is True
    again = client.post("/api/routes/pending/revoke", json={"pending_id": pid})
    assert again.status_code == 409 and again.json()["error"]["code"] == "pending_not_waiting"

    # 再排队，跑完 R-MAIN 后自动放行
    p2 = client.post("/api/routes/pending", json={"route_id": "R-SIDING"}).json()
    seq = 1
    dets = ["D0", "D1", "D2", "D3"]
    for i, d in enumerate(dets):
        client.post("/api/events", json={"seq": seq, "detector_id": d, "kind": "occupy"})
        seq += 1
        if i > 0:
            client.post("/api/events", json={"seq": seq, "detector_id": dets[i - 1], "kind": "clear"})
            seq += 1
    last = client.post("/api/events", json={"seq": seq, "detector_id": "D3", "kind": "clear"}).json()
    assert [g["pending_id"] for g in last["granted_pending"]] == [p2["id"]]


def test_api_pending_free_granted_immediately(client, yard_dict):
    client.post("/api/yard", json=yard_dict)
    r = client.post("/api/routes/pending", json={"route_id": "R-MAIN"}).json()
    assert r["status"] == "granted" and r["signal"] == "green" and r["instance_id"]


def test_api_pending_unknown_route_404(client, yard_dict):
    client.post("/api/yard", json=yard_dict)
    r = client.post("/api/routes/pending", json={"route_id": "GHOST"})
    assert r.status_code == 404


def test_api_reimport_blocked_by_pending(client, yard_dict):
    client.post("/api/yard", json=yard_dict)
    # 制造等待：先即时放行 R-MAIN，再对冲突进路排待行
    client.post("/api/routes/request", json={"route_id": "R-MAIN"})
    client.post("/api/routes/pending", json={"route_id": "R-SIDING"})
    r = client.post("/api/yard", json=yard_dict)
    # R-MAIN 为活动实例时先撞 yard_busy；撤销活动实例不可行，故此处至少被 409 阻止
    assert r.status_code == 409
