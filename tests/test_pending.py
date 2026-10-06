"""待行进路队列：入队/兑现/越过/撤销/幂等/重启恢复。"""

import json
import os
import tempfile

import pytest

from app import interlocking as il
from app.db import connect, init_db


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
    return {"seq": seq, "detector_id": detector, "kind": kind, "ts": 1}


def run_main_to_completion(conn, seq=1):
    """R-MAIN 从 S0 到 S3 完整跑一趟，返回 (结果列表, 下一序号)。"""
    out = []
    dets = ["D0", "D1", "D2", "D3"]
    for i, d in enumerate(dets):
        out.append(il.report_event(conn, ev(seq, d, "occupy"))); seq += 1
        if i > 0:
            out.append(il.report_event(conn, ev(seq, dets[i - 1], "clear"))); seq += 1
    out.append(il.report_event(conn, ev(seq, "D3", "clear"))); seq += 1
    return out, seq


def two_lane_yard():
    """两条互不争用的独立线 + 一条与 A 线争用的反向进路。"""
    return {
        "yard_id": "two-lane",
        "nodes": [{"id": n} for n in ("A0", "A1", "A2", "B0", "B1", "B2")],
        "sections": [
            {"id": "SA1", "a": "A0", "b": "A1", "detector": "DA1"},
            {"id": "SA2", "a": "A1", "b": "A2", "detector": "DA2"},
            {"id": "SB1", "a": "B0", "b": "B1", "detector": "DB1"},
            {"id": "SB2", "a": "B1", "b": "B2", "detector": "DB2"},
        ],
        "switches": [],
        "resources": [],
        "routes": [
            {"id": "RA", "sections": ["SA1", "SA2"], "switches": {}, "flank": []},
            {"id": "RA2", "sections": ["SA2", "SA1"], "switches": {}, "flank": []},
            {"id": "RB", "sections": ["SB1", "SB2"], "switches": {}, "flank": []},
        ],
    }


# ---------------- 入队与即时兑现 ----------------
def test_submit_grants_immediately_when_free(conn, yard):
    r = il.submit_pending(conn, "R-MAIN")
    assert r["duplicate"] is False
    assert r["status"] == "granted"
    assert r["approval_id"] and r["instance_id"]
    snap = il.snapshot(conn)
    # 原子建立实例与区段/道岔/侧防锁
    inst = [i for i in snap["instances"] if i["id"] == r["instance_id"]][0]
    assert inst["status"] == "active"
    held = {(l["resource_type"], l["resource_id"]) for l in snap["locks"]}
    for s in ("S0", "S1", "S2", "S3"):
        assert ("section", s) in held
    assert ("switch", "SW1") in held and ("switch", "SW2") in held
    assert ("flank", "FP-EAST") in held and ("flank", "FP-WEST") in held
    # 队列在快照中可见且状态为已放行
    assert snap["pending"][0]["status"] == "granted"
    # 批准记录已写只增日志
    row = conn.execute(
        "SELECT decision, signal FROM approvals WHERE approval_id=?", (r["approval_id"],)
    ).fetchone()
    assert row["decision"] == "permitted" and row["signal"] == "green"


def test_submit_unknown_route_404(conn, yard):
    with pytest.raises(il.ClientError) as ei:
        il.submit_pending(conn, "NOPE")
    assert ei.value.status_code == 404


def test_waiting_on_conflict_and_duplicate_returns_original(conn, yard):
    il.request_route(conn, "R-MAIN")  # 即时放行先占资源
    r = il.submit_pending(conn, "R-SIDING")
    assert r["status"] == "waiting"
    # 同一进路已有未决项：重复提交返回原申请
    dup = il.submit_pending(conn, "R-SIDING")
    assert dup["duplicate"] is True and dup["id"] == r["id"]
    # 只产生一条待行记录
    assert len(il.snapshot(conn)["pending"]) == 1


def test_probe_retries_write_no_false_denials(conn, yard):
    il.request_route(conn, "R-MAIN")
    il.submit_pending(conn, "R-SIDING")  # 等待
    # 多次触发兑现（上报推进但未完成），试探不得写任何拒绝记录
    il.report_event(conn, ev(1, "D0", "occupy"))
    il.report_event(conn, ev(2, "D0", "clear"))
    il.report_event(conn, ev(3, "D1", "occupy"))
    denied = conn.execute(
        "SELECT COUNT(*) c FROM approvals WHERE decision='denied'"
    ).fetchone()["c"]
    assert denied == 0
    kinds = [
        d["summary"]
        for d in il.list_decisions(conn, kind="approval")["items"]
    ]
    assert all("拒绝" not in s for s in kinds)
    # 待行项仍在等待
    assert il.snapshot(conn)["pending"][0]["status"] == "waiting"


# ---------------- 触发兑现：整批上报 / 复位 ----------------
def test_fulfilled_after_conflicting_route_completes(conn, yard):
    il.request_route(conn, "R-MAIN")
    il.submit_pending(conn, "R-SIDING")
    out, _ = run_main_to_completion(conn)
    last = out[-1]
    assert last["processed"][-1]["state"]["event"] == "complete"
    # 整批上报后按最新状态兑现
    assert last["granted_pending"], "末段出清后待行项应自动放行"
    snap = il.snapshot(conn)
    assert snap["pending"][0]["status"] == "granted"
    inst = [i for i in snap["instances"] if i["route_id"] == "R-SIDING"]
    assert inst and inst[0]["status"] == "active"
    assert any(l["route_id"] == "R-SIDING" and l["resource_id"] == "S1U" for l in snap["locks"])


def test_gap_blocks_fulfillment_until_drained(conn, yard):
    # 缺号期间全部等待（即使 R-MAIN 资源空闲；缓冲报文只涉及侧线区段）
    il.report_event(conn, ev(2, "D1U", "clear"))  # 缺 #1，进入缓冲
    r = il.submit_pending(conn, "R-MAIN")
    assert r["status"] == "waiting"
    # 缺号补齐、整批排空后兑现
    r2 = il.report_event(conn, ev(1, "D1U", "occupy"))
    assert r2["drained"] == 2
    assert r2["granted_pending"]
    assert il.snapshot(conn)["pending"][0]["status"] == "granted"


def test_fault_locks_keep_waiting_then_reset_fulfills(conn, yard):
    il.request_route(conn, "R-MAIN")
    il.report_event(conn, ev(1, "D0", "occupy"))
    il.report_event(conn, ev(2, "D2", "occupy"))  # 越序占用 -> 故障锁闭
    assert il.snapshot(conn)["instances"][-1]["status"] == "faulted"
    r = il.submit_pending(conn, "R-SIDING")
    assert r["status"] == "waiting"  # 故障持锁的冲突项继续等待
    inst_id = il.snapshot(conn)["instances"][-1]["id"]
    il.report_event(conn, ev(3, "D0", "clear"))
    il.report_event(conn, ev(4, "D2", "clear"))
    res = il.reset_instance(conn, inst_id)  # 成功复位后兑现
    assert res["reset"] is True and res["granted_pending"]
    snap = il.snapshot(conn)
    assert snap["pending"][0]["status"] == "granted"
    assert any(l["route_id"] == "R-SIDING" for l in snap["locks"])


# ---------------- 越过规则 ----------------
def test_later_item_overtakes_only_without_contention(conn):
    il.import_yard(conn, two_lane_yard())
    il.request_route(conn, "RA")            # RA 占用 SA1/SA2
    r1 = il.submit_pending(conn, "RA2")     # 与 RA 冲突 -> 等待
    assert r1["status"] == "waiting"
    # RB 与受阻前项 RA2 不争用任何资源 -> 可越过，立即兑现
    r2 = il.submit_pending(conn, "RB")
    assert r2["status"] == "granted"
    # 再申请 RA：与受阻前项 RA2 争用 SA1/SA2 -> 不得越过
    r3 = il.submit_pending(conn, "RA")
    assert r3["status"] == "waiting"
    snap = il.snapshot(conn)
    by_route = {p["route_id"]: p["status"] for p in snap["pending"]}
    assert by_route == {"RA2": "waiting", "RB": "granted", "RA": "waiting"}
    # RA 跑完后 RA2 兑现；RA 仍被 RA2 的新锁阻挡
    il.report_event(conn, ev(1, "DA1", "occupy"))
    il.report_event(conn, ev(2, "DA2", "occupy"))
    il.report_event(conn, ev(3, "DA1", "clear"))
    il.report_event(conn, ev(4, "DA2", "clear"))
    by_route = {p["route_id"]: p["status"] for p in il.snapshot(conn)["pending"]}
    assert by_route["RA2"] == "granted"
    assert by_route["RA"] == "waiting"


def test_blocked_chain_extends_to_later_contenders(conn):
    """后项与任一受阻前项争用即等待（受阻链传递）。"""
    il.import_yard(conn, two_lane_yard())
    il.request_route(conn, "RA")
    assert il.submit_pending(conn, "RA2")["status"] == "waiting"   # 受阻于 RA
    # RB 与 RA2 无争用 -> 越过兑现
    assert il.submit_pending(conn, "RB")["status"] == "granted"
    # 取消 RA2 后，RA 的等待项不再与之争用，但 RA 仍被活动实例锁阻挡 -> 等待
    pid = il.snapshot(conn)["pending"][0]["id"]
    il.cancel_pending(conn, pid)
    by_route = {p["route_id"]: p["status"] for p in il.snapshot(conn)["pending"]}
    assert by_route["RA2"] == "cancelled"


# ---------------- 撤销 ----------------
def test_cancel_waiting_and_not_fulfilled_later(conn, yard):
    il.request_route(conn, "R-MAIN")
    r = il.submit_pending(conn, "R-SIDING")
    res = il.cancel_pending(conn, r["id"])
    assert res["status"] == "cancelled"
    # 已撤销不可再撤销
    with pytest.raises(il.ClientError) as ei:
        il.cancel_pending(conn, r["id"])
    assert ei.value.code == "not_waiting"
    # 主进路跑完后，已撤销项保持撤销，不会兑现
    run_main_to_completion(conn)
    pend = il.snapshot(conn)["pending"][0]
    assert pend["status"] == "cancelled" and pend["instance_id"] is None


def test_cancel_granted_rejected(conn, yard):
    r = il.submit_pending(conn, "R-MAIN")  # 空闲 -> 立即兑现
    assert r["status"] == "granted"
    with pytest.raises(il.ClientError) as ei:
        il.cancel_pending(conn, r["id"])
    assert ei.value.code == "not_waiting"
    with pytest.raises(il.ClientError) as ei2:
        il.cancel_pending(conn, 9999)
    assert ei2.value.status_code == 404


# ---------------- 重导与重启 ----------------
def test_reimport_rejected_with_waiting_pending(conn, yard):
    # 以外部占用（非实例锁）使待行项等待，隔离验证“未决待行拒绝重导”
    il.report_event(conn, ev(1, "D0", "occupy"))  # S0 外部占用
    r = il.submit_pending(conn, "R-SIDING")
    assert r["status"] == "waiting"
    with pytest.raises(il.ClientError) as ei:
        il.import_yard(conn, yard)
    assert ei.value.status_code == 409
    assert ei.value.code == "pending_waiting"
    # 撤销未决项后即可重导（运行态清空，序号不回退）
    il.cancel_pending(conn, r["id"])
    il.import_yard(conn, yard)
    assert il.snapshot(conn)["pending"] == []
    assert il.snapshot(conn)["next_seq"] == 2


def test_restart_queue_persists_and_fulfills(conn, yard):
    il.request_route(conn, "R-MAIN")
    il.submit_pending(conn, "R-SIDING")
    path = conn.execute("PRAGMA database_list").fetchone()[2]
    conn.close()

    c2 = connect(path)
    snap = il.snapshot(c2)
    assert len(snap["pending"]) == 1 and snap["pending"][0]["status"] == "waiting"
    # 重启后未决项仍可继续兑现
    run_main_to_completion(c2)
    snap = il.snapshot(c2)
    assert snap["pending"][0]["status"] == "granted"
    assert snap["pending"][0]["instance_id"]
    c2.close()


# ---------------- 与即时放行/事件幂等共存 ----------------
def test_immediate_request_and_event_idempotency_still_work(conn, yard):
    # 即时放行拒绝仍写只增记录
    il.request_route(conn, "R-MAIN")
    r = il.request_route(conn, "R-SIDING")
    assert r["decision"] == "denied"
    denied = conn.execute(
        "SELECT COUNT(*) c FROM approvals WHERE decision='denied'"
    ).fetchone()["c"]
    assert denied == 1
    # 事件幂等保持有效
    il.report_event(conn, ev(1, "D0", "occupy"))
    dup = il.report_event(conn, ev(1, "D0", "occupy"))
    assert dup["duplicate"] is True
    with pytest.raises(il.ClientError) as ei:
        il.report_event(conn, {"seq": 1, "detector_id": "D0", "kind": "clear"})
    assert ei.value.code == "content_conflict"
