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
    with open(os.path.join(os.path.dirname(__file__), "..", "examples", "yard.json"), encoding="utf-8") as f:
        il.import_yard(conn, json.load(f))
    return json.load(open(os.path.join(os.path.dirname(__file__), "..", "examples", "yard.json"), encoding="utf-8"))


def ev(seq, detector, kind):
    return {"seq": seq, "detector_id": detector, "kind": kind, "ts": 123}


# ---------------- 导入校验 ----------------
def test_import_rejects_disconnected(conn):
    bad = {
        "yard_id": "x",
        "nodes": [{"id": "A"}, {"id": "B"}, {"id": "C"}, {"id": "D"}],
        "sections": [
            {"id": "S1", "a": "A", "b": "B", "detector": "D1"},
            {"id": "S2", "a": "C", "b": "D", "detector": "D2"},
        ],
        "switches": [],
        "resources": [],
        "routes": [{"id": "R1", "sections": ["S1", "S2"], "switches": {}, "flank": []}],
    }
    with pytest.raises(il.ClientError) as ei:
        il.import_yard(conn, bad)
    assert ei.value.status_code == 422
    assert any("不连通" in e for e in ei.value.extra["errors"])


def test_import_rejects_missing_section_reference(conn):
    bad = {
        "yard_id": "x",
        "nodes": [{"id": "A"}, {"id": "B"}],
        "sections": [{"id": "S1", "a": "A", "b": "B", "detector": "D1"}],
        "switches": [], "resources": [],
        "routes": [{"id": "R1", "sections": ["S1", "GHOST"], "switches": {}, "flank": []}],
    }
    with pytest.raises(il.ClientError) as ei:
        il.import_yard(conn, bad)
    assert any("不存在" in e for e in ei.value.extra["errors"])


def test_import_rejects_bad_switch_position(conn, yard):
    y = json.loads(json.dumps(yard))
    y["routes"][0]["switches"]["SW1"] = "reverse"
    with pytest.raises(il.ClientError) as ei:
        il.import_yard(conn, y)
    assert any("位置" in e for e in ei.value.extra["errors"])


def test_import_rejects_switch_on_path_but_undeclared(conn, yard):
    y = json.loads(json.dumps(yard))
    del y["routes"][0]["switches"]["SW1"]
    with pytest.raises(il.ClientError) as ei:
        il.import_yard(conn, y)
    assert any("未声明位置" in e for e in ei.value.extra["errors"])


# ---------------- 放行与互斥 ----------------
def test_route_permit_and_conflict(conn, yard):
    r = il.request_route(conn, "R-MAIN")
    assert r["decision"] == "permitted" and r["signal"] == "green"
    # 共用 S0/S3、侧防 FP-EAST
    r2 = il.request_route(conn, "R-SIDING")
    assert r2["decision"] == "denied" and r2["signal"] == "red"
    kinds = {c["type"] for c in r2["conflicts"]}
    assert "section_locked" in kinds and "flank_locked" in kinds
    # 快照能反映锁闭
    snap = il.snapshot(conn)
    assert any(l["resource_id"] == "S1" and l["route_id"] == "R-MAIN" for l in snap["locks"])


def test_gap_blocks_approval(conn, yard):
    il.report_event(conn, ev(2, "D0", "occupy"))  # 缺 #1
    with pytest.raises(il.ClientError) as ei:
        il.request_route(conn, "R-MAIN")
    assert ei.value.code == "sequence_gap"


# ---------------- 顺序占用/出清与逐段释放 ----------------
def _full_run_s0_to_s3(conn):
    """完整跑一趟 R-MAIN，返回各序结果。"""
    out = []
    seq = 1
    dets = ["D0", "D1", "D2", "D3"]
    for i, d in enumerate(dets):
        out.append(il.report_event(conn, ev(seq, d, "occupy"))); seq += 1
        if i > 0:
            prev = dets[i - 1]
            out.append(il.report_event(conn, ev(seq, prev, "clear"))); seq += 1
    out.append(il.report_event(conn, ev(seq, "D3", "clear")))
    return out


def test_section_release_requires_successor(conn, yard):
    il.request_route(conn, "R-MAIN")
    il.report_event(conn, ev(1, "D0", "occupy"))
    # S0 已占用但后继 S1 未占用 => S0 不释放
    r = il.report_event(conn, ev(2, "D0", "clear"))
    assert r["processed"][0]["state"]["event"] == "clear_waiting"
    snap = il.snapshot(conn)
    assert any(l["resource_id"] == "S0" for l in snap["locks"])
    # 后继 S1 占用，S0 仍未实际再次出清；补一条 S0 出清无效（已 clear）
    il.report_event(conn, ev(3, "D1", "occupy"))  # 推进时 S0 已 clear -> 释放
    snap = il.snapshot(conn)
    assert not any(l["resource_id"] == "S0" for l in snap["locks"])


def test_full_route_completion_releases_switches_and_flank(conn, yard):
    il.request_route(conn, "R-MAIN")
    _full_run_s0_to_s3(conn)
    snap = il.snapshot(conn)
    assert snap["instances"][-1]["status"] == "completed"
    # 道岔与侧防锁到末段出清后全部解除
    assert [l for l in snap["locks"] if l["route_id"] == "R-MAIN"] == []


def test_reapproval_after_completion(conn, yard):
    il.request_route(conn, "R-MAIN")
    _full_run_s0_to_s3(conn)
    r = il.request_route(conn, "R-SIDING")
    assert r["decision"] == "permitted"


# ---------------- 乱序缓冲与幂等 ----------------
def test_out_of_order_buffering_and_drain(conn, yard):
    il.request_route(conn, "R-MAIN")
    r3 = il.report_event(conn, ev(3, "D1", "occupy"))
    assert r3["buffered"] and r3["expected"] == 1
    r2 = il.report_event(conn, ev(2, "D0", "clear"))
    assert r2["buffered"] and r2["expected"] == 1
    # #1 到达，连续排空
    r1 = il.report_event(conn, ev(1, "D0", "occupy"))
    assert r1["status"] == "processed" and r1["drained"] == 3
    assert [x["seq"] for x in r1["processed"]] == [1, 2, 3]
    # 同序同内容幂等
    dup = il.report_event(conn, ev(1, "D0", "occupy"))
    assert dup["duplicate"] is True and dup["status"] == "processed"
    # 同序异内容拒绝
    with pytest.raises(il.ClientError) as ei:
        il.report_event(conn, {"seq": 1, "detector_id": "D0", "kind": "clear"})
    assert ei.value.code == "content_conflict"


# ---------------- 故障锁闭与复位 ----------------
def test_out_of_order_occupy_faults(conn, yard):
    il.request_route(conn, "R-MAIN")
    il.report_event(conn, ev(1, "D0", "occupy"))
    # 直接占用 S2（跳过 S1）=> 越序占用，故障锁闭
    r = il.report_event(conn, ev(2, "D2", "occupy"))
    assert r["processed"][0]["fault"] is True
    snap = il.snapshot(conn)
    assert snap["instances"][-1]["status"] == "faulted"
    # 缺号不存在但有故障：任何共用资源的放行被拒（锁仍在）
    r2 = il.request_route(conn, "R-SIDING")
    assert r2["decision"] == "denied"


def test_out_of_order_clear_faults_and_reset(conn, yard):
    il.request_route(conn, "R-MAIN")
    # 未占用即报出清 => 越序出清
    r = il.report_event(conn, ev(1, "D1", "clear"))
    assert r["processed"][0]["fault"] is True
    inst_id = r["processed"][0]["instance_id"]
    # 全段空闲（S1 出清态，其余空闲），可以复位
    res = il.reset_instance(conn, inst_id)
    assert res["reset"] is True
    snap = il.snapshot(conn)
    assert snap["instances"][-1]["status"] == "completed"


def test_reset_requires_all_clear(conn, yard):
    il.request_route(conn, "R-MAIN")
    il.report_event(conn, ev(1, "D0", "occupy"))
    il.report_event(conn, ev(2, "D2", "occupy"))  # 故障
    inst = il.snapshot(conn)["instances"][-1]
    assert inst["status"] == "faulted"
    with pytest.raises(il.ClientError) as ei:
        il.reset_instance(conn, inst["id"])  # D0、D2 仍占用
    assert ei.value.code == "reset_blocked"
    il.report_event(conn, ev(3, "D0", "clear"))
    il.report_event(conn, ev(4, "D2", "clear"))
    # 故障实例不再推进；清空后复位成功
    res = il.reset_instance(conn, inst["id"])
    assert res["reset"] is True


def test_gap_blocks_reset(conn, yard):
    il.request_route(conn, "R-MAIN")
    il.report_event(conn, ev(1, "D1", "clear"))  # 故障
    inst = il.snapshot(conn)["instances"][-1]["id"]
    il.report_event(conn, ev(3, "D2", "occupy"))  # 制造缺号 #2
    with pytest.raises(il.ClientError) as ei:
        il.reset_instance(conn, inst)
    assert ei.value.code == "sequence_gap"


def test_premature_last_section_clear_faults(conn, yard):
    """列车未按序到达末段：末段被占用即越序占用故障；之后出清不得误判完成。"""
    il.request_route(conn, "R-MAIN")
    il.report_event(conn, ev(1, "D0", "occupy"))
    # 末段 S3 在列车远未到达时被占用，且该段锁属于活动进路 => 越序占用故障
    r2 = il.report_event(conn, ev(2, "D3", "occupy"))
    assert r2["processed"][0]["fault"] is True
    snap = il.snapshot(conn)
    assert snap["instances"][-1]["status"] == "faulted"
    # 之后末段出清：故障进路不会被推进，也不得变成 completed
    il.report_event(conn, ev(3, "D3", "clear"))
    snap = il.snapshot(conn)
    assert snap["instances"][-1]["status"] == "faulted"
    assert all(l["instance_id"] == snap["instances"][-1]["id"] for l in snap["locks"])


def test_last_clear_with_uncleared_front_sections_faults(conn, yard):
    """末段按序出清，但前段仍由本实例锁定且实际占用 => 故障锁闭，保留全部锁。

    模拟长列车/车组：D0 始终未出清（S0 释放条件始终不满足），其余段按序
    推进到末段 S3 出清。旧实现仅凭末段已按序占用即删除全部锁并标记完成，
    导致仍占用 S0 的情况下冲突进路可被放行。
    """
    il.request_route(conn, "R-MAIN")
    seq = 1
    # S0 占用，永不出清
    il.report_event(conn, ev(seq, "D0", "occupy")); seq += 1
    # S1 占用：S0 仍占用，S0 锁保留；S1 再出清时后继 S2 未占用 => 等待
    il.report_event(conn, ev(seq, "D1", "occupy")); seq += 1
    il.report_event(conn, ev(seq, "D1", "clear")); seq += 1
    # S2 占用：S1 已出清 => 释放 S1 锁；S0 仍占用 => S0 锁保留
    il.report_event(conn, ev(seq, "D2", "occupy")); seq += 1
    il.report_event(conn, ev(seq, "D2", "clear")); seq += 1
    # S3（末段）占用：S2 已出清 => 释放 S2 锁；S0 锁仍在
    il.report_event(conn, ev(seq, "D3", "occupy")); seq += 1
    snap = il.snapshot(conn)
    assert any(l["resource_id"] == "S0" and l["route_id"] == "R-MAIN" for l in snap["locks"])
    # 末段出清：S0 仍由本实例锁定且占用 => 故障锁闭，不得完成
    r = il.report_event(conn, ev(seq, "D3", "clear"))
    assert r["processed"][0]["fault"] is True
    assert r["processed"][0]["state"]["event"] == "fault"
    assert r["processed"][0]["state"]["uncleared_sections"] == ["S0"]
    snap = il.snapshot(conn)
    inst = snap["instances"][-1]
    assert inst["status"] == "faulted"
    assert inst["final_cleared"] is False
    # 区段（S0、末段 S3）、道岔、侧防锁全部保留
    held = {l["resource_id"]: l for l in snap["locks"] if l["instance_id"] == inst["id"]}
    assert "S0" in held and "S3" in held
    assert "SW1" in held and "SW2" in held
    assert "FP-EAST" in held and "FP-WEST" in held
    # 冲突进路不得放行（共用区段被故障实例锁/占用）
    assert il.request_route(conn, "R-SIDING")["decision"] == "denied"
    # 全段空闲前不得复位
    with pytest.raises(il.ClientError) as ei:
        il.reset_instance(conn, inst["id"])
    assert ei.value.code == "reset_blocked"
    # S0 出清（外部检测，不推进故障实例）后可人工复位
    il.report_event(conn, ev(seq + 1, "D0", "clear"))
    res = il.reset_instance(conn, inst["id"])
    assert res["reset"] is True
    snap = il.snapshot(conn)
    assert snap["instances"][-1]["status"] == "completed"
    assert [l for l in snap["locks"] if l["instance_id"] == inst["id"]] == []
    # 复位后冲突进路可正常放行
    assert il.request_route(conn, "R-SIDING")["decision"] == "permitted"


def test_reoccupied_released_section_not_fault_on_completion(conn, yard):
    """已合法释放的前段之后被外部检测再次报占用，末段出清不得误判本实例故障。"""
    il.request_route(conn, "R-MAIN")
    seq = 1
    # 正常逐段推进：S0 出清并在 S1 占用时合法释放
    il.report_event(conn, ev(seq, "D0", "occupy")); seq += 1
    il.report_event(conn, ev(seq, "D0", "clear")); seq += 1
    il.report_event(conn, ev(seq, "D1", "occupy")); seq += 1  # S0 锁在此释放
    # 外部再次占用 S0：本实例已无 S0 锁，仅更新全站占用，不故障
    r = il.report_event(conn, ev(seq, "D0", "occupy")); seq += 1
    assert r["processed"][0]["fault"] is False
    assert r["processed"][0]["state"]["event"] == "external_occupy"
    # 继续推进到末段出清
    il.report_event(conn, ev(seq, "D1", "clear")); seq += 1
    il.report_event(conn, ev(seq, "D2", "occupy")); seq += 1
    il.report_event(conn, ev(seq, "D2", "clear")); seq += 1
    il.report_event(conn, ev(seq, "D3", "occupy")); seq += 1
    r = il.report_event(conn, ev(seq, "D3", "clear"))
    assert r["processed"][0]["fault"] is False
    assert r["processed"][0]["state"]["event"] == "complete"
    snap = il.snapshot(conn)
    assert snap["instances"][-1]["status"] == "completed"
    # 进路锁全解；S0 的外部占用独立保留在 occupancy 中
    assert [l for l in snap["locks"] if l["route_id"] == "R-MAIN"] == []
    assert snap["occupancy"]["S0"] is True


# ---------------- 只增与重启 ----------------
def test_logs_are_immutable(conn, yard):
    il.request_route(conn, "R-MAIN")
    il.report_event(conn, ev(1, "D0", "occupy"))
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE events SET kind='clear' WHERE seq=1")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM events WHERE seq=1")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE approvals SET decision='denied' WHERE approval_id=1")


def test_restart_state_persists_and_replay(conn, yard, tmp_path):
    il.request_route(conn, "R-MAIN")
    il.report_event(conn, ev(1, "D0", "occupy"))
    path = conn.execute("PRAGMA database_list").fetchone()[2]
    conn.close()

    c2 = connect(path)
    snap = il.snapshot(c2)
    assert any(l["resource_id"] == "S0" for l in snap["locks"])
    assert snap["next_seq"] == 2
    rep = il.replay(c2, 0, 100)
    kinds = [d["kind"] for d in rep["items"]]
    assert "import" in kinds and "approval" in kinds and "event" in kinds
    c2.close()


def test_reimport_clears_runtime_but_keeps_sequence(conn, yard):
    il.request_route(conn, "R-MAIN")
    _full_run_s0_to_s3(conn)
    il.import_yard(conn, yard)  # 无活动实例，允许重导
    snap = il.snapshot(conn)
    assert snap["locks"] == []
    assert snap["next_seq"] == 9  # 8 条上报后下一序号为 9，序号不回退
