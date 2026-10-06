"""性能压测：累计 N 条上报后，新上报与放行的响应时间仍 < 1s。

用法：
    python3 scripts/perf_bench.py [N]      # 默认 200_000（快速档），可传 1000000

压测模型：在 demo 站场上反复批准并完整跑完 R-MAIN，生成连续上报；
随后测量最新一批上报、缺号缓冲、放行裁决与状态快照的延迟。
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import interlocking as il  # noqa: E402
from app.db import connect, init_db  # noqa: E402

DETS = ["D0", "D1", "D2", "D3"]


def run(n_events: int = 200_000) -> None:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_db(path)
    conn = connect(path)
    yard_path = Path(__file__).resolve().parent.parent / "examples" / "yard.json"
    il.import_yard(conn, json.loads(yard_path.read_text(encoding="utf-8")))

    seq = 1
    t0 = time.perf_counter()
    trips = 0
    while seq <= n_events:
        il.request_route(conn, "R-MAIN")
        for i, d in enumerate(DETS):
            il.report_event(conn, {"seq": seq, "detector_id": d, "kind": "occupy"})
            seq += 1
            if i > 0:
                il.report_event(conn, {"seq": seq, "detector_id": DETS[i - 1], "kind": "clear"})
                seq += 1
        il.report_event(conn, {"seq": seq, "detector_id": "D3", "kind": "clear"})
        seq += 1
        trips += 1
    load_secs = time.perf_counter() - t0
    total = conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"]
    print(f"灌入 {total} 条上报 / {trips} 趟进路，用时 {load_secs:.1f}s，均值 {load_secs / total * 1000:.3f} ms/条")

    # 新一批进路上报的在线延迟
    samples_event, samples_approval = [], []
    il.request_route(conn, "R-MAIN")  # 已完成进路不互斥
    for i, d in enumerate(DETS):
        t = time.perf_counter()
        il.report_event(conn, {"seq": seq, "detector_id": d, "kind": "occupy"})
        samples_event.append((time.perf_counter() - t) * 1000)
        seq += 1
        if i > 0:
            t = time.perf_counter()
            il.report_event(conn, {"seq": seq, "detector_id": DETS[i - 1], "kind": "clear"})
            samples_event.append((time.perf_counter() - t) * 1000)
            seq += 1
    t = time.perf_counter()
    il.report_event(conn, {"seq": seq, "detector_id": "D3", "kind": "clear"})
    samples_event.append((time.perf_counter() - t) * 1000)
    seq += 1

    # 放行延迟（被拒与批准各取）
    il.request_route(conn, "R-MAIN")
    t = time.perf_counter()
    il.request_route(conn, "R-SIDING")  # 冲突拒绝
    samples_approval.append((time.perf_counter() - t) * 1000)
    # 跑完释放
    for i, d in enumerate(DETS):
        il.report_event(conn, {"seq": seq, "detector_id": d, "kind": "occupy"})
        seq += 1
        if i > 0:
            il.report_event(conn, {"seq": seq, "detector_id": DETS[i - 1], "kind": "clear"})
            seq += 1
    il.report_event(conn, {"seq": seq, "detector_id": "D3", "kind": "clear"})
    seq += 1
    t = time.perf_counter()
    il.request_route(conn, "R-MAIN")  # 批准
    samples_approval.append((time.perf_counter() - t) * 1000)

    t = time.perf_counter()
    il.snapshot(conn)
    snap_ms = (time.perf_counter() - t) * 1000

    print("\n=== 累计 %d 条上报后的在线延迟 ===" % total)
    print(f"上报 p50 {statistics.median(samples_event):.2f} ms / max {max(samples_event):.2f} ms")
    print(f"放行 {['%.2f' % x for x in samples_approval]} ms（拒绝/批准）")
    print(f"状态快照 {snap_ms:.2f} ms")
    assert max(samples_event) < 1000 and max(samples_approval) < 1000 and snap_ms < 1000, "存在 >=1s 的反馈！"
    print("✓ 全部在线反馈 < 1s")
    conn.close()


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000
    run(n)
