# 站场进路联锁推演台（Yard Route Interlocking Sandbox）

调度员在站场图上申请进路放行、上报列车检测器占用/出清，服务端按铁路联锁规则进行
事务裁决：资源互斥锁闭、按序逐段占用与释放、缺号缓冲、幂等、故障锁闭与复位、
只增裁决日志与重启回放。

- **后端**：Python 3.11+ / FastAPI / SQLite（WAL，触发器保证日志不可改写）
- **前端**：原生 HTML/CSS/JavaScript + 内联 SVG，无任何外部依赖
- **部署**：Docker Compose 一键启动，数据持久化到命名卷

---

## 1. 一键启动（Docker Compose）

```bash
docker compose up -d --build
```

启动后浏览器访问：

- 推演台页面：<http://localhost:8000/>
- API 文档（Swagger）：<http://localhost:8000/docs>
- 健康检查：<http://localhost:8000/api/health>

数据库位于容器内 `/data/interlocking.db`，映射到 Docker 命名卷
`interlocking-data`；容器重启后当前锁闭、占用与历史裁决均保留。

停止：

```bash
docker compose down          # 保留数据
docker compose down -v       # 同时删除数据卷
```

## 2. 本地运行（不用 Docker）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

数据库默认在 `./data/interlocking.db`，可用环境变量覆盖：

```bash
export INTERLOCKING_DB=/var/lib/interlocking/db.sqlite
```

---

## 3. 站场 JSON 导入

点击页面左上角 **“导入站场 JSON”**，选择 `examples/yard.json`（或自有文件）。
也可以用 API：

```bash
curl -s -X POST http://localhost:8000/api/yard \
  -H 'Content-Type: application/json' \
  --data-binary @examples/yard.json
```

### 3.1 JSON 结构

```json
{
  "yard_id": "demo-yard-01",
  "nodes":   [{"id": "N0", "x": 60, "y": 240}],
  "sections": [
    {"id": "S0", "a": "N0", "b": "N1", "detector": "D0"}
  ],
  "switches": [
    {"id": "SW1", "node": "N1", "positions": ["straight", "diverging"]}
  ],
  "resources": [
    {"id": "FP-EAST", "kind": "generic", "ref": "east-approach-protection"},
    {"id": "FP-S3U",   "kind": "section", "ref": "S3U"},
    {"id": "FP-SW1",   "kind": "switch",  "ref": "SW1"}
  ],
  "routes": [
    {
      "id": "R-MAIN",
      "name": "正线通过",
      "sections": ["S0", "S1", "S2", "S3"],
      "switches": {"SW1": "straight", "SW2": "straight"},
      "flank": ["FP-EAST", "FP-WEST"]
    }
  ]
}
```

| 字段 | 说明 |
| --- | --- |
| `nodes[].x/y` | 可选，站场图坐标；缺省时线性自动布局 |
| `sections[].a/b` | 区段两个端点（必须引用已声明节点），`detector` 绑定检测器 |
| `detectors[]` | 也可在顶层声明 `{id, section}`，两种方式等价 |
| `switches[]` | 道岔所在节点与可选位置键（如 `straight`/`diverging`） |
| `resources[]` | 侧防资源：`generic`（任意键）、`section`、`switch` |
| `routes[].switches` | 进路上每个道岔必须且只能声明为路径上道岔的位置 |
| `routes[].flank` | 进路申请时一并锁闭的侧防资源 id |

### 3.2 导入即校验（任一不满足 → 422 拒绝，不写入运行态）

- 必需字段缺失、id 重复、区段端点/检测器/侧防引用不存在；
- **进路不连通**：相邻区段不共享端点；
- 进路经过道岔但未声明位置、位置键不存在、声明路径之外的道岔；
- 进路包含未配置检测器的区段（联锁无法感知占用/出清）。

重新导入仅在 **无活动/故障进路且无缺号缓冲** 时允许；日志与全站序号永不回退。

---

## 4. 联锁规则（与操作说明）

1. **放行裁决（事务）**：服务在单个事务内核验
   - 进路每一区段“已知空闲”（occupancy 表）；
   - 区段、道岔、侧防资源均未被其他进路锁定。
   共用任一区段/道岔/侧防的进路**互斥**。成功返回 `signal: green` 并锁定全部
   资源、建立进路实例；失败返回 `signal: red` 及逐条**冲突依据**（含持有进路）。
   两种结论都写入只增的 `approvals` 与 `decisions` 表。

2. **按序占用、逐段释放**：列车须按进路 `sections` 顺序占用。
   - 后继段确认占用后，已出清的前一段区段锁才释放；后继占用时前段仍占用
     （如长列车/车组跨段），前段锁保留至其真正出清；
   - 本段已出清但后继未占用时保持锁闭（`clear_waiting`）；
   - **末段出清不是当然完成**：联锁结合**实例位置、锁归属、当前占用**裁决——
     仍由本实例持锁（未满足释放条件）且当前仍占用的前段即为未出清段，此时
     **故障锁闭**：保留该实例全部区段、道岔、侧防锁，阻止冲突进路放行，须待
     **全段空闲后人工复位**；
   - 已合法释放段（锁已不属本实例）之后被外部检测再次报占用，只更新全站占用，
     **不误判为本实例故障**；前段均已空闲时末段出清才 `completed`，全部锁释放。

3. **全站连续序号**：检测上报 `seq` 从 **1** 开始、全站唯一连续。
   - 乱序报文（`seq > 下一期望序号`）先行**缓冲**；缺号期间**禁止新放行与解锁**；
     缺号到达时按序连续排空缓冲并逐条裁决。
   - **同序同内容**重发：幂等返回原裁决；**同序异内容**：409 拒绝。
   - 越序占用、未占用即出清、按序占用前出清 → 相关进路**故障锁闭**（`faulted`），
     其锁闭保持，其他进路因资源冲突无法放行。

4. **故障复位**：须确认进路**全段空闲**（无占用、无他人锁），且无缺号；
   复位解除锁闭并关闭故障实例。

5. **不可追溯改写**：`events / event_results / approvals / decisions` 为只增表，
   SQLite 触发器禁止 UPDATE/DELETE。当前锁闭/占用状态独立维护。

6. **重启恢复与回放**：重启后页面自动显示当前锁闭与占用；“**回放裁决**”按时间
   正序回放所有历史裁决并在图上高亮，亦可通过 `/api/replay` 拉取。

### 页面操作

- **进路放行**：右侧“进路放行”卡片 → “选中看图”预览资源 → “申请放行”，
  绿灯/红灯及冲突依据就地显示；
- **检测上报**：选检测器、occupy/clear、填序号（默认自动跟随“下一期望序号”），
  支持故意发乱序/越序报文观察缓冲与故障；
- **锁闭与实例**：实时显示活动/故障/完成实例，故障实例带“故障复位”按钮；
- **裁决记录**：底部表格只增追加，可按类型过滤、加载更早、点击行高亮相关区段。

---

## 5. HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/api/health` | 健康检查 |
| POST | `/api/yard` | 导入/替换站场（返回 422 校验错误明细） |
| GET  | `/api/state` | 当前快照：站场、序号/缓冲、占用、锁、实例 |
| POST | `/api/routes/request` | 申请放行 `{route_id}` |
| POST | `/api/events` | 检测上报 `{seq, detector_id, kind}` |
| POST | `/api/reset` | 故障复位 `{instance_id}` |
| GET  | `/api/decisions?before_id=&limit=&kind=&route_id=` | 裁决倒序分页 |
| GET  | `/api/replay?after_id=&limit=` | 裁决正序回放 |

上报示例：

```bash
curl -s -X POST localhost:8000/api/events -H 'Content-Type: application/json' \
  -d '{"seq":1,"detector_id":"D0","kind":"occupy"}'
```

返回字段中 `buffered:true` 表示进入缺号缓冲；`duplicate:true` 为幂等重放；
`processed[].fault:true` 表示该条引发故障锁闭。

---

## 6. 测试与性能

```bash
pip install -r requirements-dev.txt
python3 -m pytest -q
```

覆盖：导入校验（不连通/引用缺失/道岔位置）、互斥放行、顺序推进与逐段释放、
末段出清时前段仍占用的故障锁闭（保留全部锁、阻止冲突放行、全段空闲后复位）、
已释放段外部再占用不误判故障、缺号缓冲与排空、同序幂等/异内容拒绝、
越序占用/出清故障、复位前置条件、只增触发器拦截、重启状态恢复与裁决回放。

性能压测（SQLite 主键/索引 + 单写事务串行化）：

```bash
python3 scripts/perf_bench.py 1000000   # 百万级
```

脚本在累计 N 条上报后测量最新上报、放行（批准/拒绝）与状态快照延迟，
断言全部在线反馈 < 1s。

---

## 7. 目录结构

```
app/
  main.py          # FastAPI 入口（REST + 静态托管）
  interlocking.py  # 联锁状态机：放行/上报/故障/复位/快照/回放
  validation.py    # 站场 JSON 校验与连通性检查
  db.py            # SQLite schema、只增触发器、事务
static/            # index.html / style.css / app.js（原生 JS + SVG）
examples/yard.json # 示例站场（单渡线、3 条进路）
scripts/perf_bench.py
tests/             # pytest 单元与 API 端到端测试
docker-compose.yml / Dockerfile
```
