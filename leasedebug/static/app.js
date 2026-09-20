let state = null;
let experiments = [];
let selectedExperimentId = null;

const $ = (id) => document.getElementById(id);
const fmt = (value) => value === null || value === undefined ? '—' : String(value);
async function api(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || 'GET',
    headers: options.body ? {'Content-Type': 'application/json'} : {},
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || 'request failed');
  return data;
}
function esc(value) {
  return String(value ?? '').replace(/[&<>"]/g, (ch) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[ch]));
}

async function boot() {
  experiments = await api('/api/experiments');
  if (experiments.length === 0) {
    await api('/api/experiments', {method:'POST', body:{
      name:'默认实验', ttl_ms:10000,
      clients:[
        {client_id:'c1', clock_offset_ms:0, latency_ms:10},
        {client_id:'c2', clock_offset_ms:250, latency_ms:30},
      ],
    }});
    experiments = await api('/api/experiments');
  }
  selectedExperimentId = experiments[0].id;
  wireEvents();
  await refresh();
}

function wireEvents() {
  $('newExperimentBtn').onclick = createExperiment;
  $('addClientBtn').onclick = addClient;
  $('addOpBtn').onclick = addOperation;
  $('addPartBtn').onclick = addPartition;
  $('stepBtn').onclick = () => act('step');
  $('runBtn').onclick = () => act('run');
  $('snapshotBtn').onclick = () => act('snapshots', {label:'手动快照'});
  $('forkBtn').onclick = fork;
  $('exportBtn').onclick = exportExperiment;
  $('importFile').onchange = importExperiment;
  $('viewClient').onchange = renderState;
  $('branchSelect').onchange = selectBranch;
  $('experimentSelect').onchange = async () => { selectedExperimentId = $('experimentSelect').value; await refresh(); };
  $('timeline').addEventListener('input', async (event) => {
    if (!state) return;
    state = await api(`/api/experiments/${selectedExperimentId}/seek`, {method:'POST', body:{target_ms:Number(event.target.value)}});
    renderState();
  });
}

async function createExperiment() {
  const exp = await api('/api/experiments', {method:'POST', body:{
    name:$('experimentName').value || '未命名实验',
    ttl_ms:Number($('ttlMs').value || 10000),
    clients:[{client_id:'c1', clock_offset_ms:0, latency_ms:0}],
  }});
  selectedExperimentId = exp.id;
  await refresh();
}
async function addClient() {
  await api(`/api/experiments/${selectedExperimentId}/clients`, {method:'POST', body:{
    client_id:$('clientId').value.trim(),
    clock_offset_ms:Number($('clockOffset').value || 0),
    latency_ms:Number($('clientLatency').value || 0),
  }});
  await refresh();
}
async function addOperation() {
  await api(`/api/experiments/${selectedExperimentId}/operations`, {method:'POST', body:{
    client_id:$('opClient').value,
    op:$('opType').value,
    resource:$('resource').value || 'r1',
    send_ms:Number($('sendMs').value),
    latency_ms:$('latencyMs').value === '' ? null : Number($('latencyMs').value),
    ttl_ms:$('ttlOpMs').value === '' ? null : Number($('ttlOpMs').value),
    fence_token:$('fenceToken').value === '' ? null : Number($('fenceToken').value),
    idem_key:$('idemKey').value || null,
  }});
  await refresh();
}
async function addPartition() {
  await api(`/api/experiments/${selectedExperimentId}/partitions`, {method:'POST', body:{
    client_id:$('partClient').value,
    start_ms:Number($('partStart').value),
    end_ms:Number($('partEnd').value),
  }});
  await refresh();
}
async function act(action, body={}) {
  state = await api(`/api/experiments/${selectedExperimentId}/${action}`, {method:'POST', body});
  renderState();
}
async function fork() {
  state = await api(`/api/experiments/${selectedExperimentId}/fork`, {method:'POST', body:{
    label:$('forkLabel').value || '延迟分叉',
    client_id:$('latencyClient').value,
    latency_ms:Number($('forkLatency').value || 0),
  }});
  await refresh();
}
async function selectBranch() {
  await api(`/api/experiments/${selectedExperimentId}/branch`, {method:'POST', body:{branch_id:$('branchSelect').value}});
  await refresh();
}
function exportExperiment() {
  const blob = new Blob([JSON.stringify(state.experiment, null, 2)], {type:'application/json'});
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.download = `${state.experiment.name || 'lease-experiment'}.json`;
  link.click();
  URL.revokeObjectURL(link.href);
}
async function importExperiment(event) {
  const file = event.target.files[0];
  if (!file) return;
  const payload = JSON.parse(await file.text());
  const imported = await api(`/api/experiments/${payload.id || 'unknown'}/import`, {method:'POST', body:payload});
  selectedExperimentId = imported.id;
  await refresh();
  event.target.value = '';
}
async function refresh() {
  experiments = await api('/api/experiments');
  if (!experiments.some(item => item.id === selectedExperimentId)) selectedExperimentId = experiments[0]?.id;
  if (selectedExperimentId) state = await api(`/api/experiments/${selectedExperimentId}?client=${encodeURIComponent($('viewClient').value || '')}`);
  renderSetup();
  renderState();
}

function renderSetup() {
  $('experimentSelect').innerHTML = experiments.map(exp => `<option value="${esc(exp.id)}" ${exp.id===selectedExperimentId?'selected':''}>${esc(exp.name)} (${esc(exp.id.slice(-6))})</option>`).join('');
  const exp = state?.experiment;
  if (!exp) return;
  $('clientList').innerHTML = exp.clients.map(c => `<span class="chip">${esc(c.client_id)} · offset ${fmt(c.clock_offset_ms)} · delay ${fmt(c.latency_ms)}</span>`).join('');
  const clientOptions = exp.clients.map(c => `<option>${esc(c.client_id)}</option>`).join('');
  $('opClient').innerHTML = clientOptions;
  $('partClient').innerHTML = clientOptions;
  $('latencyClient').innerHTML = clientOptions;
  if (![...$('viewClient').options].map(o=>o.value).includes(exp.clients[0]?.client_id)) $('viewClient').value = exp.clients[0]?.client_id;
  $('viewClient').innerHTML = clientOptions;
  if (state.selected_client_id) $('viewClient').value = state.selected_client_id;
  $('branchSelect').innerHTML = exp.branches.map(b => `<option value="${esc(b.id)}" ${b.id===exp.selected_branch_id?'selected':''}>${esc(b.label)} · t=${fmt(b.cursor_ms ?? b.base_snapshot_ms)}</option>`).join('');
}

function renderState() {
  if (!state) return;
  const selected = $('viewClient').value || state.selected_client_id;
  const client = state.clients.find(item => item.client_id === selected) || state.selected_client;
  $('nowLabel').textContent = `t=${state.server.now_ms}`;
  $('maxLabel').textContent = ` / ${state.max_time_ms}`;
  $('timeline').max = Math.max(1, state.max_time_ms);
  $('timeline').value = state.server.now_ms;
  $('finishBadge').textContent = state.finished ? '已完成' : '';
  renderServer();
  $('clientState').textContent = JSON.stringify(client, null, 2);
  renderTimeline();
  renderRequests();
  renderInvariants();
  $('summary').textContent = JSON.stringify(state.summary, null, 2);
}
function renderServer() {
  $('serverState').innerHTML = state.server.resources.map(item => {
    const lease = item.lease;
    return `<div class="lease-card"><strong>${esc(item.resource)} <span class="badge ${lease?'active':'free'}">${lease?'ACTIVE':'FREE / EXPIRED'}</span> max fence=${item.highest_fence_token}</strong>
      <div>${lease ? `holder=${esc(lease.holder_client_id)} fence=${lease.fence_token} expires=${lease.expires_at}` : '当前无有效持有者'}</div>
      ${item.history.length ? `<div class="kv">历史: ${item.history.map(h => `${h.fence_token}:${h.state}`).join(' → ')}</div>`:''}</div>`;
  }).join('') || '<div class="kv">暂无资源</div>';
}
function renderTimeline() {
  const types = ['request_sent','request_queued','request_arrived','request_processed','response_held','response_scheduled','response_delivered','response_ignored_stale','lease_expired','partition_started','partition_recovered'];
  $('timelineEvents').innerHTML = state.timeline.filter(e => types.includes(e.type)).slice(-180).reverse().map(event => {
    const time = event.delivery_ms ?? event.process_ms ?? event.arrival_ms ?? event.scheduled_arrival_ms ?? event.send_ms ?? event.start_ms ?? event.recovery_ms ?? '';
    const who = event.client_id ? ` ${esc(event.client_id)}` : '';
    const what = event.resource ? ` ${esc(event.resource)}` : event.request_id ? ` ${esc(event.request_id)}` : '';
    return `<div class="event-row"><span>t=${fmt(time)}</span><span class="event-type">${esc(event.type)}</span><span>${who}${what}${event.fence_token?` fence=${event.fence_token}`:''}${event.stale?' stale':''}</span></div>`;
  }).join('');
}
function statusBadge(status) {
  const cls = status === 'delivered' ? 'active' : status === 'stale_ignored' ? 'stale' : status === 'response_held' || status === 'queued_by_partition' ? 'held' : 'free';
  return `<span class="badge ${cls}">${esc(status || '')}</span>`;
}
function renderRequests() {
  const requests = Object.values(state.server.requests);
  $('requestsTable').innerHTML = `<thead><tr><th>请求</th><th>客户端</th><th>操作</th><th>资源</th><th>发送</th><th>原定到达</th><th>处理</th><th>响应</th><th>状态</th></tr></thead><tbody>${requests.map(req => `<tr class="request-row" data-id="${esc(req.id)}">
    <td>${esc(req.id)}</td><td>${esc(req.client_id)}</td><td>${esc(req.op)}</td><td>${esc(req.resource)}</td><td>${req.send_ms}</td><td>${req.scheduled_arrival_ms}</td><td>${fmt(req.process_ms)}</td><td>${fmt(req.response_delivery_ms)}</td><td>${statusBadge(req.status)}</td></tr>`).join('')}</tbody>`;
  document.querySelectorAll('.request-row').forEach(row => row.onclick = () => {
    const req = state.server.requests[row.dataset.id];
    $('requestDetail').textContent = JSON.stringify(req, null, 2);
  });
}
function renderInvariants() {
  $('invariants').innerHTML = state.invariants.checks.map(check => `<div class="invariant"><span class="badge ${check.ok?'ok':'bad'}">${check.ok?'PASS':'FAIL'}</span><strong>${esc(check.name)}</strong><div class="kv">${esc(check.detail)}</div></div>`).join('');
}
boot().catch((error) => alert(error.stack || error));
