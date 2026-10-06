"""站场 JSON 结构校验与连通性校验。

任一规则不满足都拒绝导入，返回错误明细：
* 必需字段缺失、ID 重复；
* 区段端点必须是已声明节点；
* 道岔节点存在、位置键非空；
* 进路区段必须首尾相接（连通），经过的每个道岔必须声明位置，
  声明的道岔必须在进路路径上、位置键必须存在；
* 侧防资源必须引用已存在的区段/道岔/通用资源；
* 进路区段必须全部配有检测器（否则联锁无法感知占用/出清）。
"""

from __future__ import annotations

from typing import Any


class YardValidationError(ValueError):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


def validate_yard(data: Any) -> dict:
    errors: list[str] = []

    if not isinstance(data, dict):
        raise YardValidationError(["站场配置必须是 JSON 对象"])

    yard_id = data.get("yard_id")
    if not isinstance(yard_id, str) or not yard_id:
        errors.append("yard_id 缺失或非字符串")

    nodes = data.get("nodes")
    coords: dict[str, dict[str, float]] = {}
    node_set: set[str] = set()
    if not isinstance(nodes, list) or not nodes:
        errors.append("nodes 必须是非空数组")
    else:
        for n in nodes:
            if not isinstance(n, dict) or not isinstance(n.get("id"), str):
                errors.append(f"节点格式非法: {n!r}")
                continue
            nid = n["id"]
            if nid in node_set:
                errors.append(f"节点 id 重复: {nid}")
            node_set.add(nid)
            if isinstance(n.get("x"), (int, float)) and isinstance(n.get("y"), (int, float)):
                coords[nid] = {"x": float(n["x"]), "y": float(n["y"])}

    sections = data.get("sections", [])
    section_map: dict[str, dict] = {}
    if not isinstance(sections, list) or not sections:
        errors.append("sections 必须是非空数组")
    else:
        for s in sections:
            if not isinstance(s, dict) or not isinstance(s.get("id"), str):
                errors.append(f"区段格式非法: {s!r}")
                continue
            sid = s["id"]
            if sid in section_map:
                errors.append(f"区段 id 重复: {sid}")
                continue
            a, b = s.get("a"), s.get("b")
            if not isinstance(a, str) or not isinstance(b, str):
                errors.append(f"区段 {sid} 端点 a/b 必须为字符串")
            else:
                if a not in node_set:
                    errors.append(f"区段 {sid} 端点 {a} 不存在")
                if b not in node_set:
                    errors.append(f"区段 {sid} 端点 {b} 不存在")
                if a == b:
                    errors.append(f"区段 {sid} 端点不能相同")
            section_map[sid] = {"id": sid, "a": a, "b": b}

    # 检测器：接受 sections[].detector 或顶层 detectors[]
    detector_to_section: dict[str, str] = {}
    top_detectors = data.get("detectors", [])
    if not isinstance(top_detectors, list):
        errors.append("detectors 必须是数组")
    else:
        for d in top_detectors:
            if not isinstance(d, dict) or not isinstance(d.get("id"), str):
                errors.append(f"检测器格式非法: {d!r}")
                continue
            did = d["id"]
            sid = d.get("section")
            if did in detector_to_section:
                errors.append(f"检测器 id 重复: {did}")
            if not isinstance(sid, str):
                errors.append(f"检测器 {did} 缺少 section 引用")
            elif sid not in section_map:
                errors.append(f"检测器 {did} 引用不存在的区段 {sid}")
            else:
                detector_to_section[did] = sid
    raw_sections = {s["id"]: s for s in sections if isinstance(s, dict) and isinstance(s.get("id"), str)}
    for sid, s in raw_sections.items():
        did = s.get("detector")
        if did is not None:
            if not isinstance(did, str):
                errors.append(f"区段 {sid} 的 detector 必须为字符串")
            elif did in detector_to_section and detector_to_section[did] != sid:
                errors.append(f"检测器 {did} 被区段 {sid} 与 {detector_to_section[did]} 重复绑定")
            else:
                detector_to_section.setdefault(did, sid)

    switches = data.get("switches", [])
    switch_by_node: dict[str, dict] = {}
    switch_ids: set[str] = set()
    if not isinstance(switches, list):
        errors.append("switches 必须是数组")
    else:
        for sw in switches:
            if not isinstance(sw, dict) or not isinstance(sw.get("id"), str):
                errors.append(f"道岔格式非法: {sw!r}")
                continue
            wid = sw["id"]
            if wid in switch_ids:
                errors.append(f"道岔 id 重复: {wid}")
            switch_ids.add(wid)
            node = sw.get("node")
            if node not in node_set:
                errors.append(f"道岔 {wid} 节点 {node!r} 不存在")
                continue
            if node in switch_by_node:
                errors.append(f"节点 {node} 上存在多个道岔（{switch_by_node[node]['id']} 与 {wid}）")
            positions = sw.get("positions")
            if not isinstance(positions, list) or not positions or not all(isinstance(p, str) and p for p in positions):
                errors.append(f"道岔 {wid} 的 positions 必须为非空字符串数组")
            else:
                switch_by_node[node] = {"id": wid, "positions": positions}

    resources = data.get("resources", [])
    resource_map: dict[str, dict] = {}
    if not isinstance(resources, list):
        errors.append("resources 必须是数组")
    else:
        for r in resources:
            if not isinstance(r, dict) or not isinstance(r.get("id"), str):
                errors.append(f"侧防资源格式非法: {r!r}")
                continue
            rid = r["id"]
            if rid in resource_map:
                errors.append(f"资源 id 重复: {rid}")
                continue
            kind = r.get("kind")
            ref = r.get("ref")
            if kind == "section":
                if ref not in section_map:
                    errors.append(f"资源 {rid} 引用不存在的区段 {ref}")
            elif kind == "switch":
                if not isinstance(ref, str) or not any(sw["id"] == ref for sw in switch_by_node.values()):
                    errors.append(f"资源 {rid} 引用不存在的道岔 {ref}")
            elif kind == "generic":
                if not isinstance(ref, str) or not ref:
                    errors.append(f"通用资源 {rid} 的 ref 必须为非空字符串")
            else:
                errors.append(f"资源 {rid} 的 kind 非法（section|switch|generic）")
                continue
            resource_map[rid] = {"id": rid, "kind": kind, "ref": ref}

    routes = data.get("routes", [])
    route_list: list[dict] = []
    route_ids: set[str] = set()
    if not isinstance(routes, list) or not routes:
        errors.append("routes 必须是非空数组")
    else:
        for rt in routes:
            if not isinstance(rt, dict) or not isinstance(rt.get("id"), str):
                errors.append(f"进路格式非法: {rt!r}")
                continue
            rid = rt["id"]
            if rid in route_ids:
                errors.append(f"进路 id 重复: {rid}")
            route_ids.add(rid)

            secs = rt.get("sections", [])
            if not isinstance(secs, list) or not secs or not all(isinstance(x, str) for x in secs):
                errors.append(f"进路 {rid} 的 sections 必须为非空字符串数组")
                continue
            for sid in secs:
                if sid not in section_map:
                    errors.append(f"进路 {rid} 引用不存在的区段 {sid}")
            if len(set(secs)) != len(secs):
                errors.append(f"进路 {rid} 的区段不允许重复")

            # 连通性：相邻区段必须共享端点，且该共享端点处的道岔位置须声明
            chain: list[list[str]] = []  # 每段 [a, b]
            missing_ref = False
            for sid in secs:
                s = raw_sections.get(sid)
                if s is None:
                    missing_ref = True
                    continue
                chain.append([s["a"], s["b"]])
            if missing_ref or len(chain) != len(secs):
                # 已有“引用不存在的区段”错误，跳过后续连通性检查
                continue
            used_switches: set[str] = set()
            for i in range(len(chain) - 1):
                a1, b1 = chain[i]
                a2, b2 = chain[i + 1]
                shared = {a1, b1} & {a2, b2}
                if not shared:
                    errors.append(
                        f"进路 {rid} 不连通：区段 {secs[i]} 与 {secs[i + 1]} 不共享端点"
                    )
                    break
                joint = next(iter(shared))
                if joint in switch_by_node:
                    used_switches.add(switch_by_node[joint]["id"])

            # 进路首尾端点若为道岔，也算经过该道岔
            for end in (chain[0][0], chain[0][1], chain[-1][0], chain[-1][1]):
                # 仅当端点同时是与“进路方向”相关的道岔节点时才要求位置：
                # 首段上除首节点外、末段上除末节点外的道岔已在 joint 覆盖；
                # 首节点/末节点为道岔同样需要选位。
                if end in switch_by_node:
                    # 判定是否真为进路端点（首段的 a 或末段的 b，按朝向不确定，两端都认）
                    used_switches.add(switch_by_node[end]["id"])

            switches_decl = rt.get("switches", {})
            if not isinstance(switches_decl, dict):
                errors.append(f"进路 {rid} 的 switches 必须为对象 {{switch_id: position}}")
                switches_decl = {}
            decl_ids = set(switches_decl.keys())
            for wid in decl_ids:
                if not any(sw["id"] == wid for sw in switch_by_node.values()):
                    errors.append(f"进路 {rid} 声明了不存在的道岔 {wid}")
                    continue
                swdef = next(sw for sw in switch_by_node.values() if sw["id"] == wid)
                if switches_decl[wid] not in swdef["positions"]:
                    errors.append(
                        f"进路 {rid} 的道岔 {wid} 位置 {switches_decl[wid]!r} 不存在，"
                        f"可选: {swdef['positions']}"
                    )
            for wid in used_switches - decl_ids:
                errors.append(f"进路 {rid} 经过道岔 {wid} 但未声明位置")
            for wid in decl_ids - used_switches:
                errors.append(f"进路 {rid} 声明了路径之外的道岔 {wid}")

            flanks = rt.get("flank", [])
            if not isinstance(flanks, list) or not all(isinstance(x, str) for x in flanks):
                errors.append(f"进路 {rid} 的 flank 必须为字符串数组")
            else:
                for fid in flanks:
                    if fid not in resource_map:
                        errors.append(f"进路 {rid} 引用不存在的侧防资源 {fid}")

            for sid in secs:
                s = raw_sections.get(sid, {})
                bound = sid in detector_to_section.values() or isinstance(s.get("detector"), str)
                if not bound:
                    errors.append(f"进路 {rid} 的区段 {sid} 未配置检测器，无法联锁")

            route_list.append(
                {
                    "id": rid,
                    "sections": secs,
                    "switches": switches_decl,
                    "flank": list(flanks) if isinstance(flanks, list) else [],
                }
            )

    if errors:
        raise YardValidationError(errors)

    return {
        "yard_id": yard_id,
        "coords": coords,
        "sections": [section_map[s] for s in section_map],
        "detector_to_section": detector_to_section,
        "switches": [
            {"id": sw["id"], "node": node, "positions": sw["positions"]}
            for node, sw in switch_by_node.items()
        ],
        "resources": list(resource_map.values()),
        "routes": route_list,
    }
