/* 站场进路联锁推演台 —— 原生 JavaScript，无外部依赖。 */
'use strict';

const $ = (id) => document.getElementById(id);
const api = async (path, opts = {}) => {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw Object.assign(new Error(body?.error?.message || `HTTP ${res.status}`), { body, status: res.status });
  return body;
};

const state = {
  snap: null,
  selectedRoute: null,
  highlightSections: new Set(),
  log: [],
  logCursor: null,
};

const NS = 'http://www.w3.org/2000/svg';
const svgEl = (name, attrs = {}, text) => {
  const el = document.createElementNS(NS, name);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  if (text !== undefined) el.textContent = text;
  return el;
};

/* ---------------- 布局坐标 ---------------- */
function nodePositions(yard) {
  const pos = {};
  const nodes = yard.nodes || [];
  const withXY = nodes.filter((n) => typeof n.x === 'number' && typeof n.y === 'number');
  if (withXY.length === nodes.length && nodes.length) {
    for (const n of nodes) pos[n.id] = { x: n.x, y: n.y };
    return pos;
  }
  // 无坐标时按区段连通关系做简单线性布局
  const degree = {};
  (yard.sections || []).forEach((s) => {
    degree[s.a] = (degree[s.a] || 0) + 1;
    degree[s.b] = (degree[s.b] || 0) + 1;
  });
  const gap = Math.min(150, 820 / Math.max(1, nodes.length - 1));
  nodes.forEach((n, i) => (pos[n.id] = { x: 60 + i * gap, y: 180 }));
  return pos;
}

/* ---------------- SVG 渲染 ---------------- */
function sectionStatus(snap, sectionId) {
  const lock = (snap.locks || []).find((l) => l.resource_type === 'section' && l.resource_id === sectionId);
  const inst = lock && (snap.instances || []).find((i) => i.id === lock.instance_id);
  if (snap.occupancy && snap.occupancy[sectionId]) return { color: '#d9534f', label: '占用', lock, inst };
  if (lock) {
    if (inst && inst.status === 'faulted') return { color: '#7a2222', label: '故障锁闭', lock, inst };
    return { color: '#e0a800', label: '锁闭', lock, inst };
  }
  return { color: '#5a6b80', label: '空闲', lock: null, inst: null };
}

function renderYard() {
  const snap = state.snap;
  const svg = $('yardSvg');
  svg.innerHTML = '';
  if (!snap || !snap.yard) {
    $('emptyHint').style.display = 'flex';
    return;
  }
  $('emptyHint').style.display = 'none';
  const yard = snap.yard;
  const pos = nodePositions(yard);
  const xs = Object.values(pos).map((p) => p.x);
  const ys = Object.values(pos).map((p) => p.y);
  const pad = 70;
  svg.setAttribute('viewBox', `${Math.min(...xs) - pad} ${Math.min(...ys) - pad} ${Math.max(...xs) - Math.min(...xs) + pad * 2} ${Math.max(...ys) - Math.min(...ys) + pad * 2}`);

  const selectedData = (yard.routes || []).find((r) => r.id === state.selectedRoute);
  const selectedSecs = new Set(selectedData ? selectedData.sections : []);

  // 区段
  (yard.sections || []).forEach((s) => {
    const p1 = pos[s.a], p2 = pos[s.b];
    if (!p1 || !p2) return;
    const st = sectionStatus(snap, s.id);
    const highlighted = state.highlightSections.has(s.id) || selectedSecs.has(s.id);
    const line = svgEl('line', {
      x1: p1.x, y1: p1.y, x2: p2.x, y2: p2.y,
      stroke: highlighted ? '#4ea1ff' : st.color,
      'stroke-width': highlighted ? 9 : 6,
      'stroke-linecap': 'round',
      class: 'route-hit',
    });
    const title = svgEl('title', {}, `${s.id}（检测器 ${s.detector || '?'}）— ${st.label}${st.inst ? `，进路 ${st.lock.route_id} #${st.inst.id}` : ''}`);
    line.appendChild(title);
    svg.appendChild(line);

    const mx = (p1.x + p2.x) / 2, my = (p1.y + p2.y) / 2 - 12;
    const label = svgEl('text', { x: mx, y: my, 'text-anchor': 'middle', class: 'sec-label' }, s.id);
    svg.appendChild(label);
  });

  // 节点与道岔
  const switchByNode = {};
  (yard.switches || []).forEach((sw) => (switchByNode[sw.node] = sw));
  Object.entries(pos).forEach(([nid, p]) => {
    const sw = switchByNode[nid];
    if (sw) {
      const lock = (snap.locks || []).find((l) => l.resource_type === 'switch' && l.resource_id === sw.id);
      const inst = lock && (snap.instances || []).find((i) => i.id === lock.instance_id);
      const fault = inst && inst.status === 'faulted';
      const circle = svgEl('circle', {
        cx: p.x, cy: p.y, r: 9,
        fill: fault ? '#7a2222' : lock ? '#e0a800' : '#2e3a49',
        stroke: '#d8dee9', 'stroke-width': 1.2,
      });
      circle.appendChild(svgEl('title', {}, `${sw.id}${lock ? ` 锁闭:${lock.switch_position}（${lock.route_id}）` : ''}`));
      svg.appendChild(circle);
      const t = svgEl('text', {
        x: p.x, y: p.y - 18, 'text-anchor': 'middle', class: 'switch-mark',
        fill: fault ? '#ff8089' : '#ffd479',
      }, `${sw.id}${lock ? ':' + lock.switch_position[0].toUpperCase() : ''}`);
      svg.appendChild(t);
    } else {
      svg.appendChild(svgEl('circle', { cx: p.x, cy: p.y, r: 5, class: 'node-dot' }));
    }
  });

  // 侧防（通用）资源牌
  const generic = (yard.resources || []).filter((r) => r.kind === 'generic');
  generic.forEach((r, i) => {
    const lock = (snap.locks || []).find((l) => l.resource_type === 'flank' && l.resource_id === r.id);
    const inst = lock && (snap.instances || []).find((x) => x.id === lock.instance_id);
    const gx = Math.min(...xs) - 40, gy = Math.min(...ys) - 40 + i * 26;
    const rect = svgEl('rect', { x: gx, y: gy - 14, width: 120, height: 20, rx: 5,
      fill: inst && inst.status === 'faulted' ? '#7a2222' : lock ? '#8a6a1f' : '#222d3a',
      stroke: '#3b4a5e', strokewidth: 1 });
    const txt = svgEl('text', { x: gx + 6, y: gy, fill: '#d8dee9', 'font-size': 11, 'font-family': 'ui-monospace, Menlo, monospace' },
      `🛡 ${r.id}${lock ? '🔒' : ''}`);
    txt.appendChild(svgEl('title', {}, `侧防 ${r.id}${lock ? `，被 ${lock.route_id} 锁闭` : ''}`));
    svg.appendChild(rect); svg.appendChild(txt);
  });
}

/* ---------------- 进路列表 ---------------- */
function renderRoutes() {
  const snap = state.snap;
  const box = $('routeList');
  box.innerHTML = '';
  if (!snap || !snap.yard) { box.innerHTML = '<p class="muted">无站场</p>'; return; }
  (snap.yard.routes || []).forEach((r) => {
    const div = document.createElement('div');
    div.className = 'route-item' + (state.selectedRoute === r.id ? ' selected' : '');
    div.innerHTML = `<div class="rid">${r.id}</div>
      <div class="meta">${r.name || ''}<br>段: ${r.sections.join(' → ')}<br>道岔: ${Object.entries(r.switches || {}).map(([k, v]) => `${k}=${v}`).join(', ') || '—'}　侧防: ${(r.flank || []).join(', ') || '—'}</div>`;
    const actions = document.createElement('div');
    actions.className = 'actions';
    const sel = document.createElement('button');
    sel.className = 'btn'; sel.textContent = '选中看图';
    sel.onclick = () => { state.selectedRoute = state.selectedRoute === r.id ? null : r.id; renderAll(); };
    const req = document.createElement('button');
    req.className = 'btn primary'; req.textContent = '申请放行';
    req.onclick = () => requestRoute(r.id, req);
    actions.append(sel, req);
    div.appendChild(actions);
    box.appendChild(div);
  });
}

async function requestRoute(routeId, btn) {
  btn.disabled = true;
  try {
    const r = await api('/api/routes/request', { method: 'POST', body: JSON.stringify({ route_id: routeId }) });
    const box = $('approvalResult');
    if (r.decision === 'permitted') {
      box.innerHTML = `<div class="ok">🟢 放行 ${r.route_id}（实例 #${r.instance_id}，批准号 ${r.approval_id}）</div>
        <pre>锁定：${r.locked.map((l) => `${l.type}:${l.resource}${l.switch_position ? '=' + l.switch_position : ''}`).join('\n')}</pre>`;
    } else {
      box.innerHTML = `<div class="bad">🔴 拒绝 ${r.route_id}（批准号 ${r.approval_id}）— 冲突依据：</div>
        <pre>${r.conflicts.map((c) => `• [${c.type}] ${c.resource}${c.held_by_route ? ` 被 ${c.held_by_route}(实例#${c.held_by_instance}) 占用/锁闭` : ''}`).join('\n')}</pre>`;
    }
    await refresh();
  } catch (e) {
    $('approvalResult').innerHTML = `<div class="bad">⛔ ${e.message}</div><pre>${JSON.stringify(e.body?.error || {}, null, 2)}</pre>`;
  } finally {
    btn.disabled = false;
  }
}

/* ---------------- 检测器下拉 ---------------- */
function renderDetectors() {
  const sel = $('detectorSel');
  const current = sel.value;
  sel.innerHTML = '';
  const snap = state.snap;
  if (!snap || !snap.yard) return;
  const found = new Set();
  (snap.yard.sections || []).forEach((s) => {
    if (!s.detector) return;
    found.add(s.detector);
    const opt = document.createElement('option');
    opt.value = s.detector;
    opt.textContent = `${s.detector}（区段 ${s.id}）`;
    sel.appendChild(opt);
  });
  // 兼容顶层 detectors[] 声明形式
  const bound = new Set((snap.yard.sections || []).map((s) => s.detector).filter(Boolean));
  (snap.yard.detectors || []).forEach((d) => {
    const did = d.id, sid = d.section;
    if (bound.has(did)) return;
    const opt = document.createElement('option');
    opt.value = did;
    opt.textContent = `${did}（区段 ${sid}）`;
    sel.appendChild(opt);
  });
  sel.value = current;
}

/* ---------------- 实例/锁闭 ---------------- */
function renderInstances() {
  const box = $('instanceList');
  box.innerHTML = '';
  const snap = state.snap;
  if (!snap || !(snap.instances || []).length) { box.innerHTML = '<p class="muted">无</p>'; return; }
  snap.instances.forEach((i) => {
    const div = document.createElement('div');
    div.className = `inst ${i.status}`;
    const posText = i.status === 'completed' ? '已完成' : `占用位置: 段#${i.position}`;
    div.innerHTML = `<div class="line1"><span>${i.route_id} #${i.id}</span><span class="badge ${i.status}">${i.status === 'active' ? '活动' : i.status === 'faulted' ? '故障' : '完成'}</span></div>
      <div>${posText}　批准号 ${i.approval_id}</div>`;
    if (i.status === 'faulted') {
      const btn = document.createElement('button');
      btn.className = 'btn danger'; btn.style.marginTop = '6px';
      btn.textContent = '故障复位（须全段空闲）';
      btn.onclick = async () => {
        try {
          const r = await api('/api/reset', { method: 'POST', body: JSON.stringify({ instance_id: i.id }) });
          $('eventResult').innerHTML = `<div class="ok">实例 #${r.instance_id} 已复位</div>`;
          await refresh();
        } catch (e) {
          $('eventResult').innerHTML = `<div class="bad">复位被拒：${e.message}</div><pre>${JSON.stringify(e.body?.error || {}, null, 2)}</pre>`;
        }
      };
      div.appendChild(btn);
    }
    box.appendChild(div);
  });
}

/* ---------------- 上报 ---------------- */
$('sendEventBtn')?.addEventListener('click', async () => {
  const payload = {
    seq: Number($('seqInput').value),
    detector_id: $('detectorSel').value,
    kind: $('kindSel').value,
    ts: Date.now(),
  };
  try {
    const r = await api('/api/events', { method: 'POST', body: JSON.stringify(payload) });
    if (r.buffered) {
      $('eventResult').innerHTML = `<div class="bad">⏳ #${r.seq} 已缓冲，等待缺号 #${r.expected}</div>`;
    } else if (r.duplicate) {
      $('eventResult').innerHTML = `<div class="ok">#${r.seq} 幂等重复（${r.status}${r.fault ? '，故障' : ''}）</div>`;
    } else {
      const faults = (r.processed || []).filter((x) => x.fault).length;
      $('eventResult').innerHTML = `<div class="${faults ? 'bad' : 'ok'}">✓ 处理至 #${r.expected - 1}（本批 ${r.drained || 1} 条${faults ? `，${faults} 条触发故障` : ''}）</div>
        <pre>${(r.processed || []).map((x) => `#${x.seq} ${x.detector_id} ${x.kind} → ${x.state?.event || ''}${x.fault ? ' 【故障锁闭 ' + (x.route_id || '') + '】' : ''}`).join('\n')}</pre>`;
      $('seqInput').value = r.expected;
    }
    await refresh();
  } catch (e) {
    $('eventResult').innerHTML = `<div class="bad">⛔ ${e.message}</div><pre>${JSON.stringify(e.body?.error || {}, null, 2)}</pre>`;
  }
});

/* ---------------- 顶栏序号状态 ---------------- */
function renderSeqStatus() {
  const snap = state.snap;
  const el = $('seqStatus');
  if (!snap) { el.textContent = '未连接'; el.className = 'seq-status'; return; }
  const gap = snap.buffered > 0;
  el.className = 'seq-status' + (gap ? ' gap' : '');
  el.textContent = `下一期望序号 #${snap.next_seq}　缓冲 ${snap.buffered}　累计上报 ${snap.total_events || 0}　站场 ${snap.yard_id || '—'}`;
  const hint = $('eventHint');
  hint.textContent = gap ? `缺号中：禁止新的放行与解锁` : '序号连续，可正常放行';
  if (!$('seqInput').dataset.touched) $('seqInput').value = snap.next_seq;
}
$('seqInput')?.addEventListener('input', () => { $('seqInput').dataset.touched = '1'; });
$('nextSeqBtn')?.addEventListener('click', () => {
  if (state.snap) { $('seqInput').value = state.snap.next_seq; delete $('seqInput').dataset.touched; }
});

/* ---------------- 裁决记录 ---------------- */
async function loadDecisions(reset = true) {
  const kind = $('kindFilter').value;
  let url = `/api/decisions?limit=80${kind ? `&kind=${encodeURIComponent(kind)}` : ''}`;
  if (!reset && state.logCursor) url += `&before_id=${state.logCursor}`;
  const r = await api(url);
  if (reset) state.log = r.items; else state.log = state.log.concat(r.items);
  state.logCursor = r.next_before_id;
  renderLog();
}

function fmtTime(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString('zh-CN', { hour12: false }) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

function renderLog() {
  const body = $('logBody');
  body.innerHTML = '';
  state.log.forEach((d) => {
    const tr = document.createElement('tr');
    if (d.kind === 'fault' || d.detail?.decision === 'denied') tr.className = 'bad';
    const tagText = { import: '导入', approval: '放行', event: '事件', fault: '故障', complete: '完成', reset: '复位' }[d.kind] || d.kind;
    tr.innerHTML = `<td>${d.id}</td><td><span class="tag ${d.kind}">${tagText}</span></td>
      <td>${d.route_id || ''}${d.instance_id ? ` #${d.instance_id}` : ''}</td>
      <td>${d.summary}</td><td>${fmtTime(d.created_at)}</td>`;
    tr.title = JSON.stringify(d.detail, null, 2);
    tr.style.cursor = 'pointer';
    tr.onclick = () => { state.highlightSections = new Set(findSections(d.detail)); renderYard(); };
    body.appendChild(tr);
  });
  $('olderBtn').style.display = state.logCursor ? '' : 'none';
}

function findSections(detail) {
  if (!detail) return [];
  if (Array.isArray(detail.sections)) return detail.sections;
  if (detail.section) return [detail.section];
  if (detail.last_section) return [detail.last_section];
  if (Array.isArray(detail.reasons)) return detail.reasons.map((r) => r.resource).filter(Boolean);
  return [];
}

$('kindFilter')?.addEventListener('change', () => loadDecisions(true));
$('olderBtn')?.addEventListener('click', () => loadDecisions(false));

/* ---------------- 导入 ---------------- */
$('fileInput')?.addEventListener('change', async (ev) => {
  const file = ev.target.files[0];
  if (!file) return;
  try {
    const json = JSON.parse(await file.text());
    const r = await api('/api/yard', { method: 'POST', body: JSON.stringify(json) });
    state.selectedRoute = null;
    $('approvalResult').innerHTML = `<div class="ok">✓ 已导入站场 ${r.yard_id}（${r.routes} 条进路）</div>`;
    delete $('seqInput').dataset.touched;
    await refresh();
    await loadDecisions(true);
  } catch (e) {
    const errs = e.body?.error?.errors;
    $('approvalResult').innerHTML = `<div class="bad">⛔ 导入被拒：${e.message}</div>
      <pre>${errs ? errs.map((x) => '• ' + x).join('\n') : JSON.stringify(e.body?.error || {}, null, 2)}</pre>`;
  } finally {
    ev.target.value = '';
  }
});

/* ---------------- 回放 ---------------- */
let replayTimer = null;
$('replayBtn')?.addEventListener('click', async () => {
  $('replayModal').classList.remove('hidden');
  const list = $('replayList');
  list.innerHTML = '<li>加载中…</li>';
  const all = [];
  let after = 0;
  for (;;) {
    const r = await api(`/api/replay?after_id=${after}&limit=500`);
    all.push(...r.items);
    after = r.next_after_id;
    if (!r.has_more) break;
  }
  list.innerHTML = '';
  all.forEach((d, idx) => {
    const li = document.createElement('li');
    li.textContent = `${fmtTime(d.created_at)}  #${d.id} [${d.kind}] ${d.summary}`;
    li.dataset.idx = idx;
    li.onclick = () => highlightDecision(d);
    list.appendChild(li);
  });
  $('replayStart').onclick = () => {
    clearInterval(replayTimer);
    let idx = 0;
    const step = () => {
      [...list.children].forEach((li) => li.classList.remove('active'));
      if (idx >= all.length) { clearInterval(replayTimer); return; }
      const li = list.children[idx];
      li.classList.add('active');
      li.scrollIntoView({ block: 'nearest' });
      highlightDecision(all[idx]);
      idx++;
    };
    step();
    replayTimer = setInterval(step, Number($('replaySpeed').value));
  };
});
$('closeReplay')?.addEventListener('click', () => {
  clearInterval(replayTimer);
  $('replayModal').classList.add('hidden');
});
function highlightDecision(d) {
  state.highlightSections = new Set(findSections(d.detail));
  if (d.route_id) state.selectedRoute = d.route_id;
  renderYard(); renderRoutes();
}

/* ---------------- 总刷新 ---------------- */
function renderAll() {
  renderYard();
  renderRoutes();
  renderDetectors();
  renderInstances();
  renderSeqStatus();
}
async function refresh() {
  try {
    state.snap = await api('/api/state');
    renderAll();
  } catch (e) {
    $('seqStatus').textContent = '连接失败: ' + e.message;
  }
}

(async function init() {
  await refresh();
  await loadDecisions(true);
  setInterval(() => { if ($('autoPoll').checked) { refresh(); loadDecisions(true); } }, 2000);
})();
