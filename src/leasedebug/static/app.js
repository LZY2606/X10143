"use strict";

const state = {
  experiments: [],
  expId: null,
  doc: null,
  replay: null,        // 当前游标位置的重放结果
  full: null,          // 完整重放（用于时间轴标尺）
  times: [0],          // 可单步的时间点
  timeIndex: 0,
  playing: false,
  selectedSeq: null,
  viewClient: null,
};

const $ = (id) => document.getElementById(id);

function toast(msg) {
  const t = $("toast");
  t.textContent = msg;
  t.hidden = false;
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => { t.hidden = true; }, 2200);
}

async function api(method, path, body) {
  const opt = { method, headers: {} };
  if (body !== undefined) {
    opt.headers["Content-Type"] = "application/json";
    opt.body = JSON.stringify(body);
  }
  const resp = await fetch(path, opt);
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(data.error || ("HTTP " + resp.status));
  return data;
}

const esc = (s) => String(s == null ? "" : s)
  .replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// ---- 实验 ----
async function loadExperiments(selectId) {
  const data = await api("GET", "/api/experiments");
  state.experiments = data.experiments;
  const sel = $(selectId);
  sel.innerHTML = "";
  for (const e of state.experiments) {
    const opt = document.createElement("option");
    opt.value = e.id;
    opt.textContent = e.name + (e.parent_id ? " ⎇" : "");
    sel.appendChild(opt);
  }
  if (state.expId && state.experiments.some((e) => e.id === state.expId)) {
    sel.value = state.expId;
  } else if (state.experiments.length) {
    state.expId = state.experiments[0].id;
    sel.value = state.expId;
  } else {
    state.expId = null;
  }
}

async function openExperiment(id) {
  const exp = await api("GET", "/api/experiments/" + id);
  state.expId = id;
  state.doc = exp.doc;
  state.full = await api("POST", "/api/experiments/" + id + "/replay");
  state.times = buildTimeSteps(state.full);
  state.timeIndex = state.times.length - 1;
  state.selectedSeq = null;
  await seek(state.times[state.timeIndex]);
  renderForms();
}

function buildTimeSteps(full) {
  const set = new Set([0]);
  for (const e of full.timeline) set.add(e.time);
  return [...set].sort((a, b) => a - b);
}

async function seek(t) {
  if (t === undefined) t = state.times[state.timeIndex];
  state.replay = await api("POST",
    "/api/experiments/" + state.expId + "/replay-until", { t });
  // 完整时间轴仍用 full 的标尺，事件可见性按时间过滤
  render();
}

function step(delta) {
  const n = Math.max(0, Math.min(state.times.length - 1,
    state.timeIndex + delta));
  state.timeIndex = n;
  seek();
}

function play() {
  if (state.playing) {
    state.playing = false;
    $("btnPlay").textContent = "⏩ 快进";
    return;
  }
  state.playing = true;
  $("btnPlay").textContent = "⏸ 暂停";
  const tick = () => {
    if (!state.playing) return;
    if (state.timeIndex >= state.times.length - 1) {
      state.playing = false;
      $("btnPlay").textContent = "⏩ 快进";
      return;
    }
    step(1);
    setTimeout(tick, 450);
  };
  tick();
}

// ---- 渲染 ----
function clientName(cid) {
  const c = (state.doc.clients || []).find((x) => x.id === cid);
  return c ? c.name : cid;
}

function render() {
  if (!state.replay) return;
  const r = state.replay;
  $("clockNow").textContent = r.clock;
  $("ttlVal").textContent = r.ttl;
  $("timelineRange").max = state.times.length - 1;
  $("timelineRange").value = state.timeIndex;
  renderTimeline();
  renderServer();
  renderClientView();
  renderInvariants();
  renderSummary();
  renderRequests();
}

function renderTimeline() {
  const full = state.full, r = state.replay;
  const box = $("timeline");
  box.innerHTML = "";
  const times = state.times;
  const tMin = times[0], tMax = times[times.length - 1] || 1;
  const span = Math.max(1, tMax - tMin);
  const x = (t) => ((t - tMin) / span) * (box.clientWidth - 70) + 35;

  // 分区带（横跨所有泳道）
  for (const p of full.partitions) {
    if (p.start > tMax) continue;
    const band = document.createElement("div");
    band.className = "tl-partition-band";
    band.style.left = x(p.start) + "px";
    band.style.width = Math.max(4, x(Math.min(p.end, tMax)) - x(p.start)) + "px";
    band.title = "分区 " + p.id + (p.client ? " (" + clientName(p.client) + ")"
      : " (全部)") + " [" + p.start + "," + p.end + ")";
    box.appendChild(band);
  }

  // 每个客户端一条泳道 + 服务端泳道（持有段）
  const lanes = [...state.doc.clients.map((c) => c.id), "__server__"];
  const laneEls = {};
  lanes.forEach((cid, i) => {
    const lane = document.createElement("div");
    lane.className = "tl-lane";
    lane.style.top = i * 27 + "px";
    const label = document.createElement("span");
    label.className = "tl-lane-label";
    label.textContent = cid === "__server__" ? "服务端持有段"
      : clientName(cid);
    lane.appendChild(label);
    box.appendChild(lane);
    laneEls[cid] = lane;
  });
  box.style.height = lanes.length * 27 + 34 + "px";

  // 服务端持有段
  const srv = laneEls["__server__"];
  for (const resource of Object.keys(full.server.segments)) {
    for (const seg of full.server.segments[resource]) {
      if (seg.start > tMax) continue;
      const bar = document.createElement("div");
      bar.className = "tl-seg";
      const end = seg.end_reason === "open" ? tMax : seg.end;
      bar.style.left = x(seg.start) + "px";
      bar.style.width = Math.max(2, x(end) - x(seg.start)) + "px";
      bar.style.top = "15px";
      bar.title = resource + " f" + seg.fence + " 持有者="
        + clientName(seg.holder) + " [" + seg.start + "," + seg.end
        + ") " + seg.end_reason;
      srv.appendChild(bar);
    }
  }

  // 事件点（时间戳 <= 当前游标的可见）
  const nowT = r.clock;
  for (const ev of full.timeline) {
    if (ev.time > nowT) continue;
    const cid = ev.kind === "expire" ? "__server__"
      : ev.kind.startsWith("partition") ? "__server__" : ev.client;
    const lane = laneEls[cid];
    if (!lane) continue;
    const dot = document.createElement("div");
    const cls = ev.kind.startsWith("partition") ? "partition"
      : ev.kind === "expire" ? "expire" : ev.kind;
    dot.className = "tl-event " + cls
      + (ev.kind === "expire" || ev.kind.startsWith("partition")
        ? " diamond" : "")
      + (ev.prefork ? " prefork" : "");
    if (ev.time === nowT) dot.classList.add("current");
    dot.style.left = x(ev.time) + "px";
    const meta = {
      send: "发送 #" + ev.seq + " " + ev.op,
      arrival: "到达 #" + ev.seq + (ev.delayed ? "（分区排队）" : ""),
      response: "响应 #" + ev.seq + " " + (ev.status || ""),
      expire: (ev.resource || "") + " 过期 f" + (ev.fence || ""),
      partition_start: "分区开始",
      partition_end: "分区恢复",
    }[ev.kind] || ev.kind;
    dot.title = "t=" + ev.time + " " + meta;
    if (ev.seq) dot.onclick = () => selectRequest(ev.seq);
    lane.appendChild(dot);
  }

  // 当前时刻游标
  const cursor = document.createElement("div");
  cursor.className = "tl-cursor";
  cursor.style.left = x(nowT) + "px";
  box.appendChild(cursor);

  // 时间轴刻度
  const axis = document.createElement("div");
  axis.className = "tl-axis";
  axis.style.top = lanes.length * 27 + 8 + "px";
  box.appendChild(axis);
  const ticks = 5;
  for (let i = 0; i <= ticks; i++) {
    const t = Math.round(tMin + span * i / ticks);
    const tk = document.createElement("span");
    tk.className = "tl-tick";
    tk.style.left = x(t) + "px";
    tk.textContent = t;
    axis.appendChild(tk);
  }
}

function renderServer() {
  const r = state.replay;
  const box = $("serverState");
  const resources = new Set([
    ...Object.keys(r.server.active),
    ...Object.keys(r.server.segments),
  ]);
  if (!resources.size) {
    box.innerHTML = '<div class="muted">尚无资源活动</div>';
    return;
  }
  const segsAll = state.full.server.segments;
  const allEnds = [];
  for (const resource of Object.keys(segsAll))
    for (const s of segsAll[resource]) allEnds.push(s.end);
  const tMin = 0;
  const tMax = Math.max(r.clock, ...allEnds, 1);
  box.innerHTML = "";
  for (const resource of [...resources].sort()) {
    const active = r.server.active[resource];
    const div = document.createElement("div");
    div.className = "res";
    const head = active
      ? '<span class="tag holding">有效</span> ' + esc(resource)
      : '<span class="tag none">空闲</span> ' + esc(resource);
    div.innerHTML = '<div class="res-head"><span>' + head
      + '</span><span>下一个 fence=<span class="fence">'
      + ((r.server.fence[resource] || 0) + 1) + '</span></span></div>';
    if (active) {
      const sub = document.createElement("div");
      sub.className = "muted";
      sub.textContent = "持有者 " + clientName(active.holder)
        + " · fence=" + active.fence + " · 获取于 " + active.acquired
        + " · 过期于 " + active.expiry;
      div.appendChild(sub);
    }
    const bar = document.createElement("div");
    bar.className = "seg-bar";
    for (const seg of (segsAll[resource] || [])) {
      if (seg.start > r.clock && seg.end_reason === "open") continue;
      const s = document.createElement("div");
      s.className = "seg " + (seg.end_reason === "expired" ? "expired"
        : seg.end_reason === "released" ? "released" : "");
      s.style.left = (seg.start / tMax * 100) + "%";
      const end = seg.end_reason === "open" ? r.clock : seg.end;
      s.style.width = Math.max(0.5, (end - seg.start) / tMax * 100) + "%";
      s.title = clientName(seg.holder) + " f" + seg.fence + " ["
        + seg.start + "," + seg.end + ") " + seg.end_reason;
      bar.appendChild(s);
    }
    div.appendChild(bar);
    const meta = document.createElement("div");
    meta.className = "seg-meta";
    meta.textContent = "0ms ─ " + tMax + "ms（半开有效期，端点不重叠）";
    div.appendChild(meta);
    box.appendChild(div);
  }
}

function renderClientView() {
  const r = state.replay;
  const sel = $("viewClient");
  if (!sel.options.length || !state.doc.clients.some(
      (c) => c.id === sel.value)) {
    sel.innerHTML = "";
    for (const c of state.doc.clients) {
      const o = document.createElement("option");
      o.value = c.id; o.textContent = c.name;
      sel.appendChild(o);
    }
    state.viewClient = state.viewClient &&
      state.doc.clients.some((c) => c.id === state.viewClient)
      ? state.viewClient : (state.doc.clients[0] || {}).id;
    sel.value = state.viewClient || "";
  }
  const cv = r.clients.find((c) => c.id === state.viewClient);
  const box = $("clientView");
  if (!cv) { box.innerHTML = '<div class="muted">无客户端</div>'; return; }
  const resources = Object.keys(cv.beliefs);
  let html = '<div class="row"><b>' + esc(cv.name) + '</b>'
    + '<span class="muted">时钟偏移 ' + cv.clock_offset
    + 'ms · 墙上时间 ' + cv.wall_now + 'ms · 请求延迟 '
    + cv.latency_req + '/' + cv.latency_resp + 'ms</span></div>';
  if (!resources.length) {
    html += '<div class="muted">尚无认知（等待响应）</div>';
  }
  for (const resource of resources.sort()) {
    const b = cv.beliefs[resource];
    const expiredHint = b.expired_locally
      ? '<span class="tag none">按墙上时间已过期</span>' : "";
    let detail = "";
    if (b.status === "holding") {
      detail = "fence=" + b.fence + "，认为过期于 " + b.expiry
        + "（更新于 #" + b.updated_seq + "）";
    } else if (b.status === "busy") {
      detail = "持有者 " + clientName(b.holder) + " fence=" + b.fence
        + "（更新于 #" + b.updated_seq + "）";
    } else if (b.status === "released") {
      detail = "release 结果：" + b.reason + "（更新于 #"
        + b.updated_seq + "）";
    }
    html += '<div class="row"><span class="tag ' + b.status + '">'
      + esc(b.status) + '</span><b>' + esc(resource) + '</b>'
      + '<span class="muted">' + esc(detail) + '</span>'
      + expiredHint + '</div>';
  }
  box.innerHTML = html;
}

function renderInvariants() {
  const box = $("invariants");
  box.innerHTML = "";
  for (const inv of state.replay.invariants) {
    const div = document.createElement("div");
    div.className = "inv";
    div.innerHTML = '<span class="mark ' + (inv.ok ? "ok" : "bad")
      + '"></span><div><div class="inv-name">' + esc(inv.name)
      + '</div><div class="inv-detail">' + esc(inv.detail) + '</div>'
      + (inv.violations && inv.violations.length
        ? '<div class="inv-violations">'
          + esc(JSON.stringify(inv.violations.slice(0, 3))) + '</div>'
        : "") + '</div>';
    box.appendChild(div);
  }
}

function renderSummary() {
  const s = state.replay.summary;
  const entries = [
    ["granted 授予", s.granted], ["renewed 续租", s.renewed],
    ["released 释放", s.released], ["busy 忙等", s.busy],
    ["幂等回放", s.replayed], ["旧响应丢弃", s.ignored],
    ["错误", s.errors], ["当前有效租约", s.active_leases],
  ];
  $("summary").innerHTML = entries.map(([k, v]) =>
    '<div>' + k + ': <b>' + v + '</b></div>').join("");
}

function renderRequests() {
  const box = $("requestList");
  const reqs = state.replay.requests;
  box.innerHTML = "";
  for (const q of reqs) {
    const processed = !!q.response;
    const row = document.createElement("div");
    row.className = "req" + (state.selectedSeq === q.seq ? " selected" : "");
    const st = processed ? q.response.status : "pending";
    row.innerHTML = '<span class="seq">#' + q.seq + '</span>'
      + '<span>' + esc(clientName(q.client)) + '</span>'
      + '<span>' + esc(q.op) + ' ' + esc(q.resource)
      + (q.delayed ? ' <span class="tag none">分区</span>' : '')
      + (q.response && q.response.replayed
        ? ' <span class="tag none">回放</span>' : '')
      + (q.prefork ? ' <span class="tag none">分叉前</span>' : '')
      + '<div class="muted">发 ' + q.send + ' → 达 ' + q.arrival
      + ' → 收 ' + (processed ? q.recv : "…") + '</div></span>'
      + '<span class="status ' + st + '">' + st + '</span>';
    row.onclick = () => selectRequest(q.seq);
    box.appendChild(row);
  }
}

function selectRequest(seq) {
  state.selectedSeq = seq;
  const q = state.replay.requests.find((x) => x.seq === seq);
  if (q) {
    $("requestDetail").textContent = JSON.stringify(q, null, 2);
  }
  renderRequests();
}

function renderForms() {
  const clients = state.doc.clients || [];
  for (const id of ["fClient", "pClient"]) {
    const sel = $(id);
    const first = id === "fClient" ? "" :
      '<option value="">全部客户端</option>';
    sel.innerHTML = first + clients.map((c) =>
      '<option value="' + c.id + '">' + esc(c.name) + '</option>').join("");
  }
  $("clientList").innerHTML = clients.map((c) =>
    '<div class="row"><b>' + esc(c.name) + '</b>'
    + '<span class="tag">时钟偏移 ' + c.clock_offset + 'ms</span>'
    + '<span class="tag">请求延迟 ' + c.latency_req + 'ms</span>'
    + '<span class="tag">响应延迟 ' + c.latency_resp + 'ms</span></div>'
  ).join("") || '<div class="muted">尚无客户端</div>';
}

function numOrNull(v) {
  if (v === "" || v === null || v === undefined) return null;
  return Number(v);
}

async function mutate(fn) {
  try {
    await fn();
    await loadExperiments("expSelect");
    await openExperiment(state.expId);
  } catch (err) {
    toast(err.message);
  }
}

function wire() {
  $("expSelect").onchange = (e) => openExperiment(e.target.value)
    .catch((err) => toast(err.message));
  $("btnNew").onclick = () => mutate(async () => {
    const name = prompt("新实验名称", "未命名实验");
    if (name === null) return;
    const created = await api("POST", "/api/experiments", { name });
    state.expId = created.id;
  });
  $("btnFork").onclick = () => doFork();
  $("btnExport").onclick = doExport;
  $("importFile").onchange = doImport;

  $("btnReset").onclick = () => { state.timeIndex = 0; seek(); };
  $("btnStep").onclick = () => step(1);
  $("btnPlay").onclick = play;
  $("btnEnd").onclick = () => {
    state.timeIndex = state.times.length - 1; seek();
  };
  $("timelineRange").oninput = (e) => {
    state.timeIndex = Number(e.target.value);
    seek();
  };
  $("viewClient").onchange = (e) => {
    state.viewClient = e.target.value;
    renderClientView();
  };

  $("btnAddClient").onclick = () => mutate(async () => {
    const name = prompt("客户端名称", "客户端"
      + (state.doc.clients.length + 1));
    if (!name) return;
    const off = Number(prompt("时钟偏移（ms，可为负数）", "0"));
    const dl = Number(prompt("默认请求延迟（ms）", "5"));
    const dr = Number(prompt("默认响应延迟（ms）", "5"));
    await api("POST",
      "/api/experiments/" + state.expId + "/clients",
      { name, clock_offset: off, latency_req: dl, latency_resp: dr });
  });

  $("btnAddRequest").onclick = () => mutate(async () => {
    const body = {
      client: $("fClient").value,
      send: Number($("fSend").value),
      op: $("fOp").value,
      resource: $("fResource").value.trim(),
      req_delay: numOrNull($("fReqDelay").value),
      resp_delay: numOrNull($("fRespDelay").value),
      idem_key: $("fIdem").value.trim() || null,
      fence: numOrNull($("fFence").value),
    };
    await api("POST",
      "/api/experiments/" + state.expId + "/requests", body);
    toast("已添加请求");
  });

  $("btnAddPartition").onclick = () => mutate(async () => {
    await api("POST",
      "/api/experiments/" + state.expId + "/partitions", {
        start: Number($("pStart").value),
        end: Number($("pEnd").value),
        client: $("pClient").value || null,
      });
    toast("已制造分区");
  });
}

async function doFork() {
  // 在当前单步位置建立快照并分叉；可输入对某后缀请求的延迟修改
  const kNow = processedCountAt(state.replay);
  const afterK = Number(prompt(
    "在处理完第几个请求后分叉？（共享此前事件）",
    String(kNow)));
  if (Number.isNaN(afterK)) return;
  let deltas = [];
  const change = prompt(
    "可选：修改分叉点之后某个请求的延迟，格式 \"序号:请求延迟:响应延迟\""
    + "（留空表示只保存分支）", "");
  if (change && change.trim()) {
    const parts = change.split(":").map((s) => s.trim());
    const d = { seq: Number(parts[0]) };
    if (parts[1] !== "") d.req_delay = Number(parts[1]);
    if (parts[2] !== "") d.resp_delay = Number(parts[2]);
    deltas = [d];
  }
  try {
    const fork = await api("POST",
      "/api/experiments/" + state.expId + "/forks",
      { after_k: afterK, deltas });
    // 同时保存一张快照（持久化分叉点）
    await api("POST",
      "/api/experiments/" + state.expId + "/snapshots",
      { after_k: afterK, label: "分叉 @#" + afterK });
    toast("已创建分支: " + fork.name);
    await loadExperiments("expSelect");
    state.expId = fork.id;
    $("expSelect").value = fork.id;
    await openExperiment(fork.id);
  } catch (err) {
    toast(err.message);
  }
}

function processedCountAt(replay) {
  return replay.requests.filter((q) => q.response).length;
}

async function doExport() {
  const data = await api("GET", "/api/export");
  const blob = new Blob([JSON.stringify(data, null, 2)],
    { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "leasedebug-export.json";
  a.click();
  URL.revokeObjectURL(a.href);
}

async function doImport(e) {
  const file = e.target.files[0];
  if (!file) return;
  try {
    const text = await file.text();
    const data = JSON.parse(text);
    const res = await api("POST", "/api/import", data);
    toast("已导入 " + res.imported.experiments + " 个实验，"
      + res.imported.snapshots + " 张快照");
    await loadExperiments("expSelect");
    await openExperiment(state.expId);
  } catch (err) {
    toast("导入失败: " + err.message);
  }
  e.target.value = "";
}

window.addEventListener("resize", () => renderTimeline());

(async function init() {
  wire();
  await loadExperiments("expSelect");
  if (state.expId) await openExperiment(state.expId);
})().catch((err) => toast("初始化失败: " + err.message));
