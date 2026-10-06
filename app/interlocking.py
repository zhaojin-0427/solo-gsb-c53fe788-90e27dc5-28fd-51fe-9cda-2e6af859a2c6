"""核心联锁推演服务。

所有写操作在单连接、BEGIN IMMEDIATE 事务内完成；读接口按需用主键或索引，
保证日志累计到百万级后在线反馈仍在亚秒~秒级。

关键联锁规则
------------
* 放行事务：核对进路每段“已知空闲”、道岔/侧防/区段均未被其他进路锁定，
  共用任一资源的进路互斥；成功后锁定全部资源并建立进路实例。
* 列车按进路顺序逐段占用；仅当后继段确认占用后本段方可出清释放。
* 道岔与侧防锁定到末段出清（无论中间段释放顺序）。
* 末段出清不是当然完成：须结合实例位置、锁归属与当前占用复核——仍由本实例
  持锁且当前占用的前段即为未出清段，此时故障锁闭并保留全部锁闭，待全段空闲
  后人工复位；已合法释放段（锁已不在本实例名下）即使被外部再次报占用，也不
  计为本实例故障，不阻止进路完成。
* 检测序号从 1 连续：缺号时报文缓冲，禁止新放行与解锁；缓冲按序补处理。
* 同序同内容幂等返回原裁决；同序异内容拒绝。
* 越序占用/出清使相关进路故障锁闭；故障复位须确认全段空闲。
* 事件、批准、裁决均为只增表，不可追溯改写。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from typing import Any

from .db import transaction
from .validation import YardValidationError, validate_yard


class ClientError(Exception):
    """返回给调用方的 4xx 业务错误。"""

    def __init__(self, status_code: int, code: str, message: str, extra: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.extra = extra or {}


# --------------------------------------------------------------------- #
# 基础
# --------------------------------------------------------------------- #
def _now() -> float:
    return time.time()


def _get_meta(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def _set_meta(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )


def _add_decision(
    conn: sqlite3.Connection,
    kind: str,
    summary: str,
    detail: dict | None = None,
    ref: str | None = None,
    route_id: str | None = None,
    instance_id: int | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO decisions(kind, ref, route_id, instance_id, summary, detail, created_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (
            kind,
            ref,
            route_id,
            instance_id,
            summary,
            json.dumps(detail or {}, ensure_ascii=False),
            _now(),
        ),
    )
    return int(cur.lastrowid)


def _route_data(conn: sqlite3.Connection, yard_id: str, route_id: str) -> dict:
    row = conn.execute(
        "SELECT data FROM routes WHERE yard_id=? AND route_id=?", (yard_id, route_id)
    ).fetchone()
    if not row:
        raise ClientError(404, "route_not_found", f"进路 {route_id} 不存在")
    return json.loads(row["data"])


def _active_yard(conn: sqlite3.Connection) -> str:
    yard_id = _get_meta(conn, "active_yard")
    if not yard_id:
        raise ClientError(409, "no_yard", "尚未导入站场")
    return yard_id


def _buffered_count(conn: sqlite3.Connection) -> int:
    """已收到但尚未处理（seq >= next_seq）的报文数，即缺号缓冲数。"""
    next_seq = int(_get_meta(conn, "next_seq", "1"))
    return conn.execute(
        "SELECT COUNT(*) c FROM events WHERE seq >= ?", (next_seq,)
    ).fetchone()["c"]


def _assert_no_gap(conn: sqlite3.Connection) -> int:
    """有缺号（存在缓冲报文）时禁止放行/解锁/复位。"""
    buffered = _buffered_count(conn)
    if buffered:
        next_seq = int(_get_meta(conn, "next_seq", "1"))
        raise ClientError(
            409,
            "sequence_gap",
            f"检测序号缺号：等待 #{next_seq}，已缓冲 {buffered} 条乱序报文，禁止放行与解锁",
            {"expected": next_seq, "buffered": buffered},
        )
    return int(_get_meta(conn, "next_seq", "1"))


def _fault_instance(conn: sqlite3.Connection, inst: sqlite3.Row, reason: str, detail: dict) -> None:
    """把进路实例转为故障锁闭（状态不可逆，只有复位可解除）。"""
    if inst["status"] == "faulted":
        return
    conn.execute("UPDATE instances SET status='faulted' WHERE id=?", (inst["id"],))
    _add_decision(
        conn,
        "fault",
        f"进路 {inst['route_id']} 故障锁闭：{reason}",
        detail,
        ref=f"instance-{inst['id']}",
        route_id=inst["route_id"],
        instance_id=inst["id"],
    )


# --------------------------------------------------------------------- #
# 站场导入
# --------------------------------------------------------------------- #
def import_yard(conn: sqlite3.Connection, raw: Any) -> dict:
    try:
        normal = validate_yard(raw)
    except YardValidationError as exc:
        raise ClientError(422, "yard_invalid", "站场校验失败，已拒绝导入", {"errors": exc.errors})

    yard_id = normal["yard_id"]
    with transaction(conn):
        existing = _get_meta(conn, "active_yard")
        if existing:
            active = conn.execute(
                "SELECT COUNT(*) AS c FROM instances WHERE yard_id=? AND status IN ('active','faulted')",
                (existing,),
            ).fetchone()["c"]
            if active:
                raise ClientError(
                    409,
                    "yard_busy",
                    f"当前站场存在 {active} 条未完成进路，拒绝重新导入",
                    {"active_instances": active},
                )
            if _buffered_count(conn):
                raise ClientError(
                    409,
                    "sequence_gap",
                    "存在缺号缓冲报文，拒绝重新导入",
                    {"buffered": _buffered_count(conn)},
                )
            # 清运行态与旧配置（日志保留、序号永不回退）
            conn.execute("DELETE FROM locks")
            conn.execute("DELETE FROM occupancy")
            conn.execute("DELETE FROM instances")
            conn.execute("DELETE FROM detectors")
            conn.execute("DELETE FROM routes")
            conn.execute("DELETE FROM resources")
            conn.execute("DELETE FROM switches")
            conn.execute("DELETE FROM sections")
            conn.execute("DELETE FROM yards")

        now = _now()
        conn.execute(
            "INSERT INTO yards(yard_id, config, imported_at) VALUES(?,?,?)",
            (yard_id, json.dumps(raw, ensure_ascii=False), now),
        )
        for s in normal["sections"]:
            conn.execute(
                "INSERT INTO sections(yard_id, section_id, a, b) VALUES(?,?,?,?)",
                (yard_id, s["id"], s["a"], s["b"]),
            )
        for did, sid in normal["detector_to_section"].items():
            conn.execute(
                "INSERT INTO detectors(detector_id, yard_id, section_id) VALUES(?,?,?)",
                (did, yard_id, sid),
            )
        for sw in normal["switches"]:
            conn.execute(
                "INSERT INTO switches(yard_id, switch_id, node, positions) VALUES(?,?,?,?)",
                (yard_id, sw["id"], sw["node"], json.dumps(sw["positions"], ensure_ascii=False)),
            )
        for r in normal["resources"]:
            conn.execute(
                "INSERT INTO resources(yard_id, resource_id, kind, ref) VALUES(?,?,?,?)",
                (yard_id, r["id"], r["kind"], r["ref"]),
            )
        for rt in normal["routes"]:
            conn.execute(
                "INSERT INTO routes(yard_id, route_id, data) VALUES(?,?,?)",
                (yard_id, rt["id"], json.dumps(rt, ensure_ascii=False)),
            )
        for s in normal["sections"]:
            conn.execute(
                "INSERT INTO occupancy(yard_id, section_id, occupied) VALUES(?,?,0) "
                "ON CONFLICT(yard_id, section_id) DO UPDATE SET occupied=0",
                (yard_id, s["id"]),
            )
        _set_meta(conn, "active_yard", yard_id)
        _add_decision(
            conn,
            "import",
            f"导入站场 {yard_id}：{len(normal['sections'])} 区段、"
            f"{len(normal['switches'])} 道岔、{len(normal['routes'])} 进路",
            {
                "yard_id": yard_id,
                "sections": len(normal["sections"]),
                "routes": len(normal["routes"]),
            },
            ref=yard_id,
        )
    return {"yard_id": yard_id, "routes": len(normal["routes"])}


# --------------------------------------------------------------------- #
# 放行裁决
# --------------------------------------------------------------------- #
def request_route(conn: sqlite3.Connection, route_id: str) -> dict:
    with transaction(conn):
        yard_id = _active_yard(conn)
        _assert_no_gap(conn)
        route = _route_data(conn, yard_id, route_id)
        sections: list[str] = route["sections"]
        switches: dict[str, str] = route["switches"]
        flanks: list[str] = route["flank"]
        now = _now()

        # 1) 各区段已知空闲
        occupied_rows = conn.execute(
            f"SELECT section_id, occupied FROM occupancy WHERE yard_id=? AND section_id IN "
            f"({','.join('?' * len(sections))})",
            (yard_id, *sections),
        ).fetchall()
        conflicts: list[dict] = []
        for row in occupied_rows:
            if row["occupied"]:
                conflicts.append({"type": "section_occupied", "resource": row["section_id"]})

        # 2) 资源锁互斥（区段 / 道岔 / 侧防）
        wanted: list[tuple[str, str, str | None]] = []
        for sid in sections:
            wanted.append(("section", sid, None))
        for wid, pos in switches.items():
            wanted.append(("switch", wid, pos))
        for fid in flanks:
            wanted.append(("flank", fid, None))

        for rtype, rid, _pos in wanted:
            row = conn.execute(
                "SELECT route_id, instance_id, switch_position FROM locks "
                "WHERE yard_id=? AND resource_type=? AND resource_id=?",
                (yard_id, rtype, rid),
            ).fetchone()
            if row:
                conflicts.append(
                    {
                        "type": f"{rtype}_locked",
                        "resource": rid,
                        "held_by_route": row["route_id"],
                        "held_by_instance": row["instance_id"],
                    }
                )

        # 3) 故障实例保留其全部锁闭，上面的资源锁检查已逐项体现为 *_locked 冲突依据。

        reasons = _dedup_conflicts(conflicts)
        if reasons:
            cur = conn.execute(
                "INSERT INTO approvals(yard_id, route_id, decision, signal, reasons, created_at) "
                "VALUES(?,?, 'denied', 'red', ?, ?)",
                (yard_id, route_id, json.dumps(reasons, ensure_ascii=False), now),
            )
            approval_id = int(cur.lastrowid)
            _add_decision(
                conn,
                "approval",
                f"拒绝放行进路 {route_id}：{len(reasons)} 项冲突",
                {"decision": "denied", "reasons": reasons},
                ref=str(approval_id),
                route_id=route_id,
            )
            return {
                "decision": "denied",
                "signal": "red",
                "approval_id": approval_id,
                "route_id": route_id,
                "conflicts": reasons,
            }

        # 4) 批准：建实例、锁资源
        cur = conn.execute(
            "INSERT INTO approvals(yard_id, route_id, decision, signal, reasons, created_at) "
            "VALUES(?,?, 'permitted', 'green', '[]', ?)",
            (yard_id, route_id, now),
        )
        approval_id = int(cur.lastrowid)
        cur = conn.execute(
            "INSERT INTO instances(yard_id, route_id, status, position, final_cleared, approval_id, created_at) "
            "VALUES(?,?, 'active', -1, 0, ?, ?)",
            (yard_id, route_id, approval_id, now),
        )
        instance_id = int(cur.lastrowid)
        for rtype, rid, pos in wanted:
            conn.execute(
                "INSERT INTO locks(yard_id, resource_type, resource_id, route_id, instance_id, "
                "switch_position, locked_at) VALUES(?,?,?,?,?,?,?)",
                (yard_id, rtype, rid, route_id, instance_id, pos, now),
            )
        _add_decision(
            conn,
            "approval",
            f"批准放行进路 {route_id}（实例 #{instance_id}），信号绿灯",
            {
                "decision": "permitted",
                "signal": "green",
                "instance_id": instance_id,
                "sections": sections,
                "switches": switches,
                "flank": flanks,
            },
            ref=str(approval_id),
            route_id=route_id,
            instance_id=instance_id,
        )
        return {
            "decision": "permitted",
            "signal": "green",
            "approval_id": approval_id,
            "instance_id": instance_id,
            "route_id": route_id,
            "locked": [
                {"type": t, "resource": r, **({"switch_position": p} if p else {})}
                for t, r, p in wanted
            ],
        }


def _dedup_conflicts(conflicts: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for c in conflicts:
        key = json.dumps(c, sort_keys=True, ensure_ascii=False)
        if key not in seen:
            seen.add(key)
            out.append(c)
    return out


# --------------------------------------------------------------------- #
# 检测上报
# --------------------------------------------------------------------- #
def _content_hash(detector_id: str, kind: str) -> str:
    # 同序同内容幂等、异内容拒绝：参与比对的是检测器与事件语义。
    # 时间戳等非语义字段不参与（报文中的 ts 原样存档）。
    return hashlib.sha256(f"{detector_id}|{kind}".encode("utf-8")).hexdigest()


def report_event(conn: sqlite3.Connection, payload: dict) -> dict:
    seq = payload.get("seq")
    detector_id = payload.get("detector_id")
    kind = payload.get("kind")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
        raise ClientError(422, "bad_event", "seq 必须为 >=1 的整数")
    if not isinstance(detector_id, str) or not detector_id:
        raise ClientError(422, "bad_event", "detector_id 缺失")
    if kind not in ("occupy", "clear"):
        raise ClientError(422, "bad_event", "kind 必须为 occupy 或 clear")

    with transaction(conn):
        yard_id = _active_yard(conn)
        drow = conn.execute(
            "SELECT section_id FROM detectors WHERE detector_id=?", (detector_id,)
        ).fetchone()
        if not drow:
            raise ClientError(404, "detector_unknown", f"检测器 {detector_id} 不存在")
        section_id = drow["section_id"]

        next_seq = int(_get_meta(conn, "next_seq", "1"))
        chash = _content_hash(detector_id, kind)

        # 已处理/已缓冲：幂等或异内容拒绝
        prev = conn.execute(
            "SELECT content_hash, payload FROM events WHERE seq=?", (seq,)
        ).fetchone()
        if prev:
            if prev["content_hash"] != chash:
                _add_decision(
                    conn,
                    "event",
                    f"拒绝报文 #{seq}：同序号内容不一致",
                    {"seq": seq, "detector_id": detector_id, "kind": kind, "reason": "content_mismatch"},
                    ref=str(seq),
                )
                raise ClientError(
                    409,
                    "content_conflict",
                    f"序号 #{seq} 已存在不同内容，拒绝",
                    {"seq": seq, "expected_hash": prev["content_hash"], "got_hash": chash},
                )
            if seq < next_seq:
                r = conn.execute("SELECT * FROM event_results WHERE seq=?", (seq,)).fetchone()
                return {
                    "seq": seq,
                    "duplicate": True,
                    "status": r["status"],
                    "fault": bool(r["fault"]),
                    "route_id": r["route_id"],
                    "instance_id": r["instance_id"],
                    "state": json.loads(r["state"]) if r["state"] else None,
                }
            # seq >= next_seq：仍在缓冲区的同内容重发，幂等等待缺号补齐
            return {
                "seq": seq,
                "buffered": True,
                "duplicate": True,
                "status": "buffered",
                "expected": next_seq,
            }

        if seq < next_seq:
            # 不是重复行却序号回退：历史位置已被不同事件占用，等同异内容
            raise ClientError(
                409,
                "content_conflict",
                f"序号 #{seq} 已被使用且内容不同，拒绝",
                {"seq": seq},
            )

        # 落事件日志（缓冲与否都先行存档，日志只增）
        conn.execute(
            "INSERT INTO events(seq, detector_id, kind, content_hash, payload, received_at) "
            "VALUES(?,?,?,?,?,?)",
            (
                seq,
                detector_id,
                kind,
                chash,
                json.dumps(payload, ensure_ascii=False),
                _now(),
            ),
        )

        results: list[dict] = []
        if seq > next_seq:
            return {
                "seq": seq,
                "buffered": True,
                "status": "buffered",
                "expected": next_seq,
                "gap_size": seq - next_seq,
                "buffered_total": _buffered_count(conn),
                "processed": results,
            }

        # seq == next_seq：处理并尽可能排空缓冲
        _process_one(conn, yard_id, seq, detector_id, kind, section_id, results)
        _set_meta(conn, "next_seq", next_seq + 1)
        cursor = next_seq + 1
        while True:
            row = conn.execute(
                "SELECT seq, detector_id, kind FROM events WHERE seq=?",
                (cursor,),
            ).fetchone()
            if not row:
                break
            d = conn.execute(
                "SELECT section_id FROM detectors WHERE detector_id=?", (row["detector_id"],)
            ).fetchone()
            sid = d["section_id"] if d else "?"
            _process_one(conn, yard_id, row["seq"], row["detector_id"], row["kind"], sid, results)
            _set_meta(conn, "next_seq", cursor + 1)
            cursor += 1

        return {
            "seq": seq,
            "status": "processed",
            "expected": cursor,
            "drained": len(results),
            "processed": results,
        }


def _process_one(
    conn: sqlite3.Connection,
    yard_id: str,
    seq: int,
    detector_id: str,
    kind: str,
    section_id: str,
    results: list[dict],
) -> None:
    """按序处理一条报文，推进占用状态机并给出裁决。"""
    # 找到以该段为“当前等待段”的活动进路
    inst = conn.execute(
        "SELECT i.* FROM instances i WHERE i.yard_id=? AND i.status IN ('active','faulted') "
        "ORDER BY i.id",
        (yard_id,),
    ).fetchall()
    target: sqlite3.Row | None = None
    target_index: int | None = None
    route_cache: dict[int, dict] = {}
    for it in inst:
        rd = json.loads(
            conn.execute("SELECT data FROM routes WHERE yard_id=? AND route_id=?", (yard_id, it["route_id"])).fetchone()["data"]
        )
        route_cache[it["id"]] = rd
        if it["status"] == "active":
            expected_idx = it["position"] + 1
            if expected_idx < len(rd["sections"]) and rd["sections"][expected_idx] == section_id:
                target = it
                target_index = expected_idx
                break

    fault = False
    fault_reason: str | None = None
    owner_route: str | None = None
    owner_instance: int | None = None
    state: dict = {"section": section_id, "kind": kind}

    if kind == "occupy":
        conn.execute(
            "INSERT INTO occupancy(yard_id, section_id, occupied) VALUES(?,?,1) "
            "ON CONFLICT(yard_id, section_id) DO UPDATE SET occupied=1",
            (yard_id, section_id),
        )
        if target is not None:
            # 正常推进：后继段确认占用 -> 释放前一段（若其已出清）
            prev_idx = target["position"]
            conn.execute(
                "UPDATE instances SET position=? WHERE id=?", (target_index, target["id"])
            )
            released: list[str] = []
            if prev_idx >= 0:
                prev_section = route_cache[target["id"]]["sections"][prev_idx]
                prow = conn.execute(
                    "SELECT occupied FROM occupancy WHERE yard_id=? AND section_id=?",
                    (yard_id, prev_section),
                ).fetchone()
                if prow and not prow["occupied"]:
                    conn.execute(
                        "DELETE FROM locks WHERE yard_id=? AND resource_type='section' "
                        "AND resource_id=? AND instance_id=?",
                        (yard_id, prev_section, target["id"]),
                    )
                    released.append(prev_section)
            state.update({"event": "advance", "index": target_index, "released": released})
        else:
            # 非任何活动进路等待段的占用：
            # 若该段被活动/故障进路锁定且不是其下一等待段 => 越序占用
            lock_row = conn.execute(
                "SELECT * FROM locks WHERE yard_id=? AND resource_type='section' AND resource_id=?",
                (yard_id, section_id),
            ).fetchone()
            if lock_row:
                owner = conn.execute(
                    "SELECT * FROM instances WHERE id=?", (lock_row["instance_id"],)
                ).fetchone()
                if owner and owner["status"] == "active":
                    fault = True
                    fault_reason = f"区段 {section_id} 越序占用（非进路下一等待段）"
                    owner_route = owner["route_id"]
                    owner_instance = owner["id"]
                    state.update({"event": "fault", "reason": fault_reason})
                    _fault_instance(conn, owner, fault_reason, {
                        "seq": seq, "section": section_id, "kind": "occupy"
                    })
                else:
                    state.update({"event": "external_occupy", "note": "区段由故障/外部进路持有"})
            else:
                state.update({"event": "external_occupy", "note": "区段不属于任何活动进路"})
    else:  # clear
        occ = conn.execute(
            "SELECT occupied FROM occupancy WHERE yard_id=? AND section_id=?",
            (yard_id, section_id),
        ).fetchone()
        if not occ or not occ["occupied"]:
            # 未占用却报出清 => 越序出清
            conn.execute(
                "INSERT INTO occupancy(yard_id, section_id, occupied) VALUES(?,?,0) "
                "ON CONFLICT(yard_id, section_id) DO UPDATE SET occupied=0",
                (yard_id, section_id),
            )
            fault = True
            fault_reason = f"区段 {section_id} 未占用即报出清（越序出清）"
            state.update({"event": "fault", "reason": fault_reason})
            owner = conn.execute(
                "SELECT i.* FROM instances i JOIN locks l ON l.instance_id=i.id "
                "WHERE i.yard_id=? AND l.resource_type='section' AND l.resource_id=? AND i.status='active'",
                (yard_id, section_id),
            ).fetchone()
            if owner:
                owner_route = owner["route_id"]
                owner_instance = owner["id"]
                _fault_instance(conn, owner, fault_reason, {"seq": seq, "section": section_id, "kind": "clear"})
        else:
            conn.execute(
                "UPDATE occupancy SET occupied=0 WHERE yard_id=? AND section_id=?",
                (yard_id, section_id),
            )
            # 找到该段属于哪个活动进路及其位置
            owner = conn.execute(
                "SELECT i.* FROM instances i JOIN locks l ON l.instance_id=i.id "
                "WHERE i.yard_id=? AND l.resource_type='section' AND l.resource_id=? AND i.status='active' "
                "ORDER BY i.id",
                (yard_id, section_id),
            ).fetchone()
            if owner is None:
                state.update({"event": "external_clear", "note": "无活动进路持有该区段"})
            else:
                owner_route = owner["route_id"]
                owner_instance = owner["id"]
                rd = route_cache.get(owner["id"]) or json.loads(
                    conn.execute("SELECT data FROM routes WHERE yard_id=? AND route_id=?",
                                 (yard_id, owner["route_id"])).fetchone()["data"]
                )
                idx = rd["sections"].index(section_id)
                is_last = idx == len(rd["sections"]) - 1
                if idx <= owner["position"]:
                    # 本段已按序占用：
                    # idx < position（后继段已确认占用）时出清即释放本段；
                    # idx == position 且非末段时后继未占用，本段保持锁闭等待；
                    # 末段出清须先按“实例位置 + 锁归属 + 当前占用”复核前段，
                    # 防止前段仍占用时误删全部锁闭、误判进路完成。
                    if is_last:
                        # position 已推进到末段；仍由本实例持锁的前段即“未到释放
                        # 条件”的段（后继占用时它尚未出清，故锁被保留）。
                        held_rows = conn.execute(
                            f"SELECT resource_id FROM locks WHERE yard_id=? "
                            f"AND resource_type='section' AND instance_id=? "
                            f"AND resource_id IN ({','.join('?' * len(rd['sections']))})",
                            (yard_id, owner["id"], *rd["sections"]),
                        ).fetchall()
                        held_prev = [r["resource_id"] for r in held_rows if r["resource_id"] != section_id]
                        if held_prev:
                            occ_rows = conn.execute(
                                f"SELECT section_id FROM occupancy WHERE yard_id=? "
                                f"AND occupied=1 AND section_id IN "
                                f"({','.join('?' * len(held_prev))})",
                                (yard_id, *held_prev),
                            ).fetchall()
                            uncleared = [r["section_id"] for r in occ_rows]
                        else:
                            uncleared = []
                        if uncleared:
                            # 前段仍属本实例（锁未释放）且实际占用：列车/车组未出清，
                            # 末段出清不能完成进路。故障锁闭，保留全部区段、道岔、
                            # 侧防锁，阻止冲突进路放行，待全段空闲后人工复位。
                            fault = True
                            fault_reason = (
                                f"末段 {section_id} 已出清，但前段 {'、'.join(uncleared)} "
                                "仍由本进路锁定且尚未出清，进路故障锁闭（保留全部锁闭，"
                                "待全段空闲后人工复位）"
                            )
                            state.update({
                                "event": "fault",
                                "reason": fault_reason,
                                "index": idx,
                                "position": owner["position"],
                                "uncleared_sections": uncleared,
                                "note": "已合法释放段的外部再占用不在此列（无本实例锁归属）",
                            })
                            _fault_instance(conn, owner, fault_reason, {
                                "seq": seq,
                                "section": section_id,
                                "kind": "clear",
                                "index": idx,
                                "position": owner["position"],
                                "uncleared_sections": uncleared,
                            })
                        else:
                            # 本实例仍持锁的前段均已空闲：列车离开，释放全部剩余
                            # 锁闭（道岔/侧防/末段），进路完成。已合法释放段即使
                            # 被外部检测再次报占用，因无本实例锁归属而不影响完成。
                            conn.execute(
                                "DELETE FROM locks WHERE yard_id=? AND instance_id=?",
                                (yard_id, owner["id"]),
                            )
                            conn.execute(
                                "UPDATE instances SET status='completed', final_cleared=1 WHERE id=?",
                                (owner["id"],),
                            )
                            _add_decision(
                                conn,
                                "complete",
                                f"进路 {owner['route_id']}（实例 #{owner['id']}）末段出清，进路完成",
                                {"seq": seq, "last_section": section_id},
                                ref=str(seq),
                                route_id=owner["route_id"],
                                instance_id=owner["id"],
                            )
                            state.update({"event": "complete", "index": idx, "instance_id": owner["id"]})
                    elif idx < owner["position"]:
                        conn.execute(
                            "DELETE FROM locks WHERE yard_id=? AND resource_type='section' "
                            "AND resource_id=? AND instance_id=?",
                            (yard_id, section_id, owner["id"]),
                        )
                        state.update({"event": "release", "index": idx})
                    else:
                        state.update({
                            "event": "clear_waiting",
                            "index": idx,
                            "note": "后继段未确认占用，本段暂不释放",
                        })
                else:
                    # 出清一个尚未（按序）占用的段 => 越序出清
                    fault = True
                    fault_reason = f"区段 {section_id} 在按序占用前报出清（越序出清）"
                    state.update({"event": "fault", "reason": fault_reason})
                    _fault_instance(conn, owner, fault_reason, {
                        "seq": seq, "section": section_id, "kind": "clear", "index": idx,
                        "position": owner["position"],
                    })

    status = "processed"
    conn.execute(
        "INSERT INTO event_results(seq, status, route_id, instance_id, fault, state, processed_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (
            seq,
            status,
            target["route_id"] if target is not None else owner_route,
            target["id"] if target is not None else owner_instance,
            1 if fault else 0,
            json.dumps(state, ensure_ascii=False),
            _now(),
        ),
    )
    results.append(
        {
            "seq": seq,
            "detector_id": detector_id,
            "section": section_id,
            "kind": kind,
            "fault": fault,
            "route_id": target["route_id"] if target is not None else owner_route,
            "instance_id": target["id"] if target is not None else owner_instance,
            "state": state,
        }
    )
    _add_decision(
        conn,
        "event",
        (f"#{seq} {detector_id} {kind}" + ("（故障锁闭）" if fault else "")),
        {"seq": seq, "detector_id": detector_id, "section": section_id, "kind": kind, **state},
        ref=str(seq),
        route_id=target["route_id"] if target is not None else owner_route,
        instance_id=target["id"] if target is not None else owner_instance,
    )


# --------------------------------------------------------------------- #
# 故障复位
# --------------------------------------------------------------------- #
def reset_instance(conn: sqlite3.Connection, instance_id: int) -> dict:
    with transaction(conn):
        yard_id = _active_yard(conn)
        _assert_no_gap(conn)
        inst = conn.execute(
            "SELECT * FROM instances WHERE yard_id=? AND id=?", (yard_id, instance_id)
        ).fetchone()
        if not inst:
            raise ClientError(404, "instance_not_found", f"进路实例 #{instance_id} 不存在")
        if inst["status"] != "faulted":
            raise ClientError(409, "not_faulted", f"实例 #{instance_id} 非故障锁闭状态，无需复位")

        rd = json.loads(
            conn.execute("SELECT data FROM routes WHERE yard_id=? AND route_id=?",
                         (yard_id, inst["route_id"])).fetchone()["data"]
        )
        sections = rd["sections"]
        rows = conn.execute(
            f"SELECT section_id, occupied FROM occupancy WHERE yard_id=? AND section_id IN "
            f"({','.join('?' * len(sections))})",
            (yard_id, *sections),
        ).fetchall()
        busy = [r["section_id"] for r in rows if r["occupied"]]
        # 还需确认没有列车压在进路相关区段上（ occupancy 已覆盖），
        # 以及所有进路段当前不再被其他锁引用（本实例外）。
        foreign = conn.execute(
            f"SELECT resource_id FROM locks WHERE yard_id=? AND resource_type='section' "
            f"AND instance_id<>? AND resource_id IN ({','.join('?' * len(sections))})",
            (yard_id, instance_id, *sections),
        ).fetchall()
        if busy or foreign:
            reasons = []
            if busy:
                reasons.append({"type": "section_occupied", "sections": busy})
            if foreign:
                reasons.append({"type": "foreign_lock", "resources": [r["resource_id"] for r in foreign]})
            raise ClientError(
                409,
                "reset_blocked",
                "复位须确认全段空闲",
                {"reasons": reasons},
            )

        conn.execute("DELETE FROM locks WHERE yard_id=? AND instance_id=?", (yard_id, instance_id))
        conn.execute("UPDATE instances SET status='completed' WHERE id=?", (instance_id,))
        _add_decision(
            conn,
            "reset",
            f"故障复位实例 #{instance_id}（进路 {inst['route_id']}）：全段空闲，锁闭解除",
            {"instance_id": instance_id, "sections": sections},
            ref=f"instance-{instance_id}",
            route_id=inst["route_id"],
            instance_id=instance_id,
        )
        return {"instance_id": instance_id, "reset": True}


# --------------------------------------------------------------------- #
# 快照与回放（重启后页面仍显示当前锁闭）
# --------------------------------------------------------------------- #
def snapshot(conn: sqlite3.Connection) -> dict:
    yard_id = _get_meta(conn, "active_yard")
    buffered = _buffered_count(conn)
    if not yard_id:
        return {
            "yard": None,
            "next_seq": int(_get_meta(conn, "next_seq", "1")),
            "buffered": buffered,
        }
    yrow = conn.execute("SELECT config, imported_at FROM yards WHERE yard_id=?", (yard_id,)).fetchone()
    yard = json.loads(yrow["config"])

    occupancy = {
        r["section_id"]: bool(r["occupied"])
        for r in conn.execute("SELECT section_id, occupied FROM occupancy WHERE yard_id=?", (yard_id,))
    }
    locks = [
        dict(r)
        for r in conn.execute(
            "SELECT resource_type, resource_id, route_id, instance_id, switch_position "
            "FROM locks WHERE yard_id=?",
            (yard_id,),
        ).fetchall()
    ]
    instances = [
        {
            "id": r["id"],
            "route_id": r["route_id"],
            "status": r["status"],
            "position": r["position"],
            "final_cleared": bool(r["final_cleared"]),
            "approval_id": r["approval_id"],
            "created_at": r["created_at"],
        }
        for r in conn.execute(
            "SELECT * FROM instances WHERE yard_id=? ORDER BY id", (yard_id,)
        ).fetchall()
    ]
    total_events = conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"]
    return {
        "yard_id": yard_id,
        "yard": yard,
        "next_seq": int(_get_meta(conn, "next_seq", "1")),
        "buffered": buffered,
        "total_events": total_events,
        "occupancy": occupancy,
        "locks": locks,
        "instances": instances,
    }


def list_decisions(
    conn: sqlite3.Connection,
    before_id: int | None = None,
    limit: int = 100,
    kind: str | None = None,
    route_id: str | None = None,
) -> dict:
    limit = max(1, min(limit, 500))
    where, params = [], []
    if before_id:
        where.append("id < ?")
        params.append(before_id)
    if kind:
        where.append("kind = ?")
        params.append(kind)
    if route_id:
        where.append("route_id = ?")
        params.append(route_id)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(
        f"SELECT * FROM decisions{clause} ORDER BY id DESC LIMIT ?", (*params, limit + 1)
    ).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    return {
        "items": [
            {
                "id": r["id"],
                "kind": r["kind"],
                "ref": r["ref"],
                "route_id": r["route_id"],
                "instance_id": r["instance_id"],
                "summary": r["summary"],
                "detail": json.loads(r["detail"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ],
        "has_more": has_more,
        "next_before_id": rows[-1]["id"] if has_more and rows else None,
    }


def replay(conn: sqlite3.Connection, after_id: int = 0, limit: int = 100) -> dict:
    """按时间正序回放裁决（重启后可回放历史裁决）。"""
    limit = max(1, min(limit, 1000))
    rows = conn.execute(
        "SELECT * FROM decisions WHERE id > ? ORDER BY id ASC LIMIT ?", (after_id, limit)
    ).fetchall()
    return {
        "items": [
            {
                "id": r["id"],
                "kind": r["kind"],
                "ref": r["ref"],
                "route_id": r["route_id"],
                "instance_id": r["instance_id"],
                "summary": r["summary"],
                "detail": json.loads(r["detail"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ],
        "next_after_id": rows[-1]["id"] if rows else after_id,
        "has_more": len(rows) == limit,
    }
