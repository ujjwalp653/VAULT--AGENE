(() => {
  'use strict';

  const cfg = window.VAULT_CONFIG || {};
  const viteLocal = location.port === '5173';
  const localBackend = `${location.protocol}//${location.hostname || '127.0.0.1'}:8000`;
  const API = String(cfg.API || '').replace(/\/$/, '') || (viteLocal ? localBackend : window.location.origin);
  const WS = cfg.WS || (viteLocal
    ? `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.hostname || '127.0.0.1'}:8000/ws`
    : `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`);

  const $ = (s, root = document) => root.querySelector(s);
  const $$ = (s, root = document) => [...root.querySelectorAll(s)];
  const esc = (v) => String(v ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const fmt = (n) => {
    if (n === null || n === undefined || Number.isNaN(Number(n))) return '—';
    let x = Number(n), i = 0;
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    while (Math.abs(x) >= 1024 && i < units.length - 1) { x /= 1024; i++; }
    return `${x.toFixed(x >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
  };
  const pct = (n) => `${Math.max(0, Math.min(100, Number(n) || 0)).toFixed(0)}%`;
  const ago = (ts) => ts ? `${Math.max(0, Math.round(Date.now() / 1000 - Number(ts)))}s` : '—';
  const statusClass = (s) => String(s || 'unknown').toLowerCase().replaceAll('_', '-');

  const state = {
    cluster: null,
    metrics: null,
    policies: null,
    events: [],
    selectedNode: 1,
    chaos: true,
    filter: '',
    loading: false,
    socket: null,
    socketRetry: 0,
    actionBusy: false,
  };

  async function fetchJson(path, opts = {}) {
    const r = await fetch(`${API}${path}`, opts);
    const text = await r.text();
    let body = {};
    try { body = text ? JSON.parse(text) : {}; } catch { body = text; }
    if (!r.ok) throw new Error(typeof body === 'string' ? body : (body.detail || JSON.stringify(body)));
    return body;
  }

  function post(path, body) {
    return fetchJson(path, {
      method: 'POST',
      ...(body ? { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) } : {})
    });
  }

  function nav(icon, label, active = false) {
    const targets = {
      Dashboard: 'dashboardSection', Objects: 'objectsSection', Nodes: 'nodesSection',
      Replication: 'replicationSection', Repairs: 'repairsSection', Analytics: 'analyticsSection', Chaos: 'chaosSection'
    };
    return `<button class="${active ? 'active' : ''}" data-nav="${label}" data-target="${targets[label]}" title="Open ${label}"><span class="ico">${icon}</span><span>${label}</span></button>`;
  }

  function injectShell() {
    document.title = 'VAULT — Distributed Object Storage';
    $('#app').innerHTML = `
      <div class="shell">
        <aside class="sidebar">
          <div class="brand"><div class="brand-mark">V</div><div><div class="brand-name">VAULT</div><div class="brand-sub">Distributed Object Storage</div></div></div>
          <div class="nav-label">Control plane</div>
          <nav class="nav">
            ${nav('▣', 'Dashboard', true)}
            ${nav('◫', 'Objects')}
            ${nav('◉', 'Nodes')}
            ${nav('◌', 'Replication')}
            ${nav('↺', 'Repairs')}
            ${nav('⌁', 'Analytics')}
            ${nav('⚡', 'Chaos')}
          </nav>
          <div class="sidebar-foot"><div class="system-mini"><div class="mini-head"><span>System</span><span class="pulse"></span></div><div class="mini-stat" id="miniOnline">—<small>nodes online</small></div><div class="mini-stat" id="miniAvailability">—<small>availability</small></div></div></div>
        </aside>

        <main class="main">
          <header class="topbar">
            <div class="search"><span>⌕</span><input id="globalSearch" placeholder="Search objects, nodes, or events…" /></div>
            <div class="top-stats">
              <div class="top-chip"><b id="topOnline">—</b><span>Nodes online</span></div>
              <div class="top-chip"><b id="topAvailability">—</b><span>Availability</span></div>
              <div class="top-chip"><b id="topClock">--:--:--</b><span id="topDate">VAULT</span></div>
            </div>
          </header>

          <div class="content">
            <section class="hero reveal" id="dashboardSection" data-motion>
              <div class="hero-inner">
                <div class="hero-copy">
                  <div class="eyebrow">Fault-tolerant distributed object storage</div>
                  <h1>Data that <span class="accent">survives</span> anything.</h1>
                  <p>Replicate, detect, repair and rebalance data across independently failing storage nodes. Watch the cluster react in real time.</p>
                  <div class="hero-tags"><span class="tag">store</span><span class="tag">replicate</span><span class="tag">heal</span><span class="tag">scale</span></div>
                  <div class="hero-side-metrics"><div class="glass-stat"><span>Replication</span><b id="heroRF">3×</b></div><div class="glass-stat"><span>Write quorum</span><b id="heroW">2</b></div><div class="glass-stat"><span>Read quorum</span><b id="heroR">1</b></div></div>
                </div>
                <div class="hero-network"><div class="network" id="network"></div></div>
              </div>
            </section>

            <div class="metrics reveal" id="metrics" data-motion></div>

            <section class="panel reveal policy-panel" id="replicationSection" data-motion>
              <div class="panel-head">
                <div class="panel-title"><div><div class="eyebrow">Durability policy</div><h2>Replication & quorum policy</h2></div></div>
                <div class="panel-actions"><button class="btn" id="rebalanceBtn">Rebalance now</button><button class="btn primary" id="repairBtn">Repair now</button></div>
              </div>
              <div class="policy-grid" id="policyGrid"></div>
            </section>

            <section class="grid-main reveal" id="chaosSection" data-motion>
              <section class="panel">
                <div class="panel-head"><div class="panel-title"><div><div class="eyebrow">Live topology</div><h2>Cluster control map</h2></div></div><div class="panel-actions"><button class="btn" id="refreshBtn">Refresh</button></div></div>
                <div class="node-grid" id="nodeCards"></div>
              </section>
              <section class="panel">
                <div class="panel-head"><div class="panel-title"><div><div class="eyebrow">Failure injection</div><h2>Chaos console</h2></div></div><button class="switch on" id="chaosToggle"><i></i></button></div>
                <div class="failure-console" id="failureConsole"></div>
              </section>
            </section>

            <section class="analytics reveal" id="analyticsSection" data-motion>
              <section class="panel"><div class="panel-head"><div class="panel-title"><div><div class="eyebrow">Capacity</div><h2>Storage utilization</h2></div></div></div><div class="chart-body" id="storagePanel"></div></section>
              <section class="panel"><div class="panel-head"><div class="panel-title"><div><div class="eyebrow">Operations</div><h2>Cluster activity</h2></div></div></div><div class="chart-body" id="throughputPanel"></div></section>
              <section class="panel"><div class="panel-head"><div class="panel-title"><div><div class="eyebrow">Integrity</div><h2>Replica health</h2></div></div></div><div class="chart-body" id="integrityPanel"></div></section>
            </section>

            <section class="ops-grid reveal" data-motion>
              <section class="panel" id="objectsSection"><div class="panel-head"><div class="panel-title"><div><div class="eyebrow">Object fabric</div><h2>Recent objects</h2></div></div><button class="btn" id="uploadBtn">Upload</button></div><div class="object-list" id="objectList"></div></section>
              <section class="panel" id="repairsSection"><div class="panel-head"><div class="panel-title"><div><div class="eyebrow">Recovery</div><h2>Repair queue</h2></div></div></div><div class="repair-list" id="repairList"></div></section>
              <section class="panel event-panel"><div class="panel-head"><div class="panel-title"><div><div class="eyebrow">Observability</div><h2>Live events</h2></div></div><span class="status healthy">Live</span></div><div class="events" id="events"></div></section>
            </section>

            <section class="panel reveal" id="nodesSection" data-motion><div class="panel-head"><div class="panel-title"><div><div class="eyebrow">Node telemetry</div><h2>Storage nodes</h2></div></div></div><div class="node-grid" id="nodesBottom"></div></section>
            <div class="footer">VAULT v2.4 · control plane · independent node agents · SHA-256 · replica consistency · automatic repair · rebalancing · quorum policies</div>
          </div>
        </main>
      </div>
      <div class="modal-backdrop" id="modalBackdrop"></div>
      <div class="toast-stack" id="toastStack"></div>
    `;
    setInterval(updateClock, 1000);
    updateClock();
    bindStaticEvents();
    observeMotion();
  }

  function bindStaticEvents() {
    $('#refreshBtn').onclick = () => refresh(true);
    $('#uploadBtn').onclick = openUpload;
    $('#rebalanceBtn').onclick = () => runAction(() => post('/api/rebalance'), 'Rebalance finished', r => r?.moved ? `Moved ${r.chunk_id} from N${r.source} to N${r.target}.` : (r?.reason || 'No replica movement was required.'));
    $('#repairBtn').onclick = () => runAction(() => post('/api/repair'), 'Repair complete', 'Cluster reconciliation finished.');
    $('#chaosToggle').onclick = () => {
      state.chaos = !state.chaos;
      $('#chaosToggle').classList.toggle('on', state.chaos);
      toast('Chaos mode', state.chaos ? 'Failure injection controls enabled.' : 'Failure injection controls paused.');
    };
    $('#globalSearch').addEventListener('input', e => { state.filter = e.target.value.trim().toLowerCase(); renderAll(); });

    $$('.nav button').forEach(btn => btn.addEventListener('click', () => goToSection(btn.dataset.target, btn.dataset.nav)));

    const sections = ['dashboardSection','replicationSection','chaosSection','analyticsSection','objectsSection','repairsSection','nodesSection'];
    const observer = new IntersectionObserver(entries => entries.forEach(entry => {
      if (!entry.isIntersecting) return;
      $$('.nav button').forEach(x => x.classList.toggle('active', x.dataset.target === entry.target.id));
    }), { rootMargin: '-26% 0px -58% 0px', threshold: 0 });
    sections.forEach(id => { const el = document.getElementById(id); if (el) observer.observe(el); });
  }

  function goToSection(id, label) {
    const el = document.getElementById(id);
    if (!el) return;
    const top = el.getBoundingClientRect().top + window.scrollY - 86;
    window.scrollTo({ top, behavior: 'smooth' });
    $$('.nav button').forEach(x => x.classList.toggle('active', x.dataset.target === id));
  }

  async function runAction(fn, title, success) {
    try {
      const result = await fn();
      if (result && result.moved === 0) toast(title, result.reason || 'No safe movement was necessary.');
      else toast(title, typeof success === 'function' ? success(result) : success);
      await refresh();
    } catch (e) {
      toast(`${title} failed`, e.message, true);
    }
  }

  function updateClock() {
    const d = new Date();
    $('#topClock').textContent = d.toLocaleTimeString([], { hour12: false });
    $('#topDate').textContent = d.toLocaleDateString([], { month: 'short', day: '2-digit', year: 'numeric' });
  }

  async function refresh(showToast = false) {
    if (state.loading) return;
    state.loading = true;
    try {
      const [cluster, metrics, policies] = await Promise.all([
        fetchJson('/api/cluster'), fetchJson('/api/metrics'), fetchJson('/api/policies')
      ]);
      state.cluster = cluster;
      state.metrics = metrics;
      state.policies = policies;
      state.events = cluster.events || state.events;
      renderAll();
      if (showToast) toast('Cluster refreshed', 'Live state synchronized with the control plane.');
    } catch (err) {
      toast('Connection issue', err.message, true);
    } finally {
      state.loading = false;
    }
  }

  function renderAll() {
    if (!state.cluster) return;
    const c = state.cluster, m = state.metrics || {}, s = c.summary || {};
    const online = Number(s.healthy_nodes || 0), total = Number(s.nodes || 0);
    const availability = total ? Math.round((online / total) * 1000) / 10 : 0;
    $('#topOnline').textContent = `${online}/${total}`;
    $('#topAvailability').textContent = `${availability}%`;
    $('#miniOnline').innerHTML = `${online}/${total}<small>nodes online</small>`;
    $('#miniAvailability').innerHTML = `${availability}%<small>current availability</small>`;
    $('#heroRF').textContent = `${c.objects?.[0]?.replication_factor || state.policies?.default_replication_factor || 3}×`;
    $('#heroW').textContent = `W=${c.objects?.[0]?.write_quorum || state.policies?.default_write_quorum || 2}`;
    $('#heroR').textContent = `R=${c.objects?.[0]?.read_quorum || state.policies?.default_read_quorum || 1}`;

    renderMetrics(s, m);
    renderPolicy(m, state.policies || {});
    renderNetwork(c.nodes || []);
    renderNodeCards(c.nodes || [], $('#nodeCards'));
    renderNodeCards(c.nodes || [], $('#nodesBottom'));
    renderFailureConsole(c.nodes || []);
    renderStorage({ ...s, ...m });
    renderThroughput(m.activity || []);
    renderIntegrity(m.integrity || {});
    renderObjects(c.objects || []);
    renderRepairs(c.repairs || []);
    renderEvents(c.events || state.events || []);
  }

  function renderMetrics(s, m) {
    const concurrent = m.concurrency || {};
    const metrics = [
      ['Objects', Number(s.objects || 0).toLocaleString(), `${fmt(s.logical_bytes || 0)} logical data`],
      ['Storage', fmt(s.physical_bytes || 0), `${Number(s.replication_overhead || 0).toFixed(2)}× physical overhead`],
      ['Replicas', Number(m.healthy_replicas || 0).toLocaleString(), `${Number(m.avg_replicas || 0).toFixed(1)} average / chunk`],
      ['Healthy nodes', `${s.healthy_nodes || 0}/${s.nodes || 0}`, `${s.failed_nodes || 0} not serving`],
      ['Repairs', Number(s.repairs || 0).toLocaleString(), `avg ${((m.avg_repair_ms || 0) / 1000).toFixed(2)}s recovery`],
      ['Concurrency', `${concurrent.active_reads || 0}R / ${concurrent.active_writes || 0}W`, `peak ${concurrent.max_concurrent_reads || 0}R / ${concurrent.max_concurrent_writes || 0}W`]
    ];
    $('#metrics').innerHTML = metrics.map(([k, v, d]) => `<article class="metric"><div class="kicker">${k}</div><div class="value">${esc(v)}</div><div class="detail">${esc(d)}</div><div class="accent-line"></div></article>`).join('');
  }

  function renderPolicy(m, p) {
    const rf = p.default_replication_factor ?? 3;
    const w = p.default_write_quorum ?? 2;
    const r = p.default_read_quorum ?? 1;
    const concurrency = m.concurrency || {};
    $('#policyGrid').innerHTML = `
      <div class="policy-card"><span>Replication factor</span><b>${rf}×</b><small>copies per chunk</small></div>
      <div class="policy-card"><span>Write quorum</span><b>W=${w}</b><small>successful writes required</small></div>
      <div class="policy-card"><span>Read quorum</span><b>R=${r}</b><small>verified replicas per read</small></div>
      <div class="policy-card"><span>Chunk size</span><b>${p.chunk_size ? fmt(p.chunk_size) : '—'}</b><small>large-file segmentation</small></div>
      <div class="policy-card"><span>Repair cycle</span><b>${p.repair_interval ?? '—'}s</b><small>background reconciliation</small></div>
      <div class="policy-card"><span>Rebalance gap</span><b>${p.rebalance_threshold != null ? Math.round(p.rebalance_threshold * 100) + '%' : '—'}</b><small>utilization difference</small></div>
      <div class="policy-card"><span>Concurrent writes</span><b>${concurrency.max_concurrent_writes || 0}</b><small>peak observed</small></div>
      <div class="policy-card"><span>Concurrent reads</span><b>${concurrency.max_concurrent_reads || 0}</b><small>peak observed</small></div>
    `;
  }

  function renderNetwork(nodes) {
    const ns = nodes.length || 1;
    const positions = nodes.map((n, i) => {
      const angle = (-Math.PI / 2) + (i / ns) * Math.PI * 2;
      const r = ns > 6 ? 36 : 34;
      return { node: n, x: 50 + Math.cos(angle) * r, y: 51 + Math.sin(angle) * r * .72 };
    });
    const lines = positions.map(p => `<line class="network-line ${['FAILED','PARTITIONED','CORRUPTED','INCONSISTENT','UNREACHABLE'].includes(p.node.status) ? 'alert' : ''}" x1="50%" y1="51%" x2="${p.x}%" y2="${p.y}%"></line>`).join('');
    const nodesHtml = positions.map(p => {
      const sc = statusClass(p.node.status);
      return `<button class="net-node ${sc}" data-node="${p.node.node_id}" style="left:${p.x}%;top:${p.y}%"><div class="net-core"></div><span class="net-label">N${p.node.node_id}</span></button><div class="net-tooltip" style="left:${p.x}%;top:${p.y}%">Node ${p.node.node_id} · ${esc(p.node.status)} · ${pct((p.node.load || 0) * 100)}</div>`;
    }).join('');
    $('#network').innerHTML = `<svg viewBox="0 0 100 100" preserveAspectRatio="none">${lines}</svg><div class="net-node healthy" style="left:50%;top:51%;cursor:default"><div class="net-core"></div><span class="net-label">VAULT</span></div>${nodesHtml}`;
    $$('.net-node[data-node]').forEach(btn => btn.onclick = () => selectNode(Number(btn.dataset.node)));
  }

  function renderNodeCards(nodes, root) {
    const items = filterNodes(nodes);
    root.innerHTML = items.map(n => {
      const storage = Number(n.storage_utilization ?? (n.capacity_bytes ? n.used_bytes / n.capacity_bytes : 0));
      const load = Number(n.load ?? storage);
      const activity = Number(n.activity_load ?? 0);
      const sc = statusClass(n.status);
      const util = (storage * 100).toFixed(storage * 100 < 1 && storage > 0 ? 2 : 1);
      const activityPct = Math.max(0, Math.min(100, activity * 100));
      const lastOp = n.last_operation_at ? `${Math.max(0, Math.round(Date.now() / 1000 - Number(n.last_operation_at)))}s ago` : 'no recent operation';
      return `<button class="node-card ${sc === 'failed' ? 'failed' : ''}" data-node-card="${n.node_id}">
        <div class="node-head"><b>NODE ${String(n.node_id).padStart(2, '0')}</b><span class="status ${sc}">${esc(n.status)}</span></div>
        <div class="node-sub">${fmt(n.used_bytes)} / ${fmt(n.capacity_bytes)} · heartbeat ${ago(n.last_heartbeat)}</div>
        <div class="node-bar"><span style="width:${Math.min(100, Math.max(0, load * 100))}%"></span></div>
        <div class="node-telemetry"><span>storage <b>${util}%</b></span><span>activity <b>${activityPct.toFixed(0)}%</b></span></div>
        <div class="node-sub">${Number(n.total_ops || 0).toLocaleString()} ops · ${esc(lastOp)} · ${esc(n.url || '')}</div>
        ${n.last_error ? `<div class="node-error">${esc(n.last_error)}</div>` : ''}
      </button>`;
    }).join('') || '<div class="empty">No nodes match the current search.</div>';
    $$('[data-node-card]', root).forEach(el => el.onclick = () => selectNode(Number(el.dataset.nodeCard)));
  }

  function filterNodes(nodes) {
    return state.filter ? nodes.filter(n => `node ${n.node_id} ${n.status} ${n.url}`.toLowerCase().includes(state.filter)) : nodes;
  }

  function selectNode(id) {
    state.selectedNode = id;
    renderFailureConsole(state.cluster?.nodes || []);
    toast(`Node ${id} selected`, 'Failure and consistency controls are scoped to this storage agent.');
  }

  function renderFailureConsole(nodes) {
    const target = nodes.find(n => n.node_id === state.selectedNode) || nodes[0];
    if (!target) return;
    state.selectedNode = target.node_id;
    const opts = nodes.map(n => `<option value="${n.node_id}" ${n.node_id === target.node_id ? 'selected' : ''}>Node ${n.node_id} — ${esc(n.status)}</option>`).join('');
    const object = (state.cluster?.objects || [])[0];
    const objectOptions = (state.cluster?.objects || []).map(o => `<option value="${esc(o.object_id)}">${esc(o.name)}</option>`).join('');
    const replicaHint = object?.replica_node_ids?.length ? `Integrity target nodes: ${object.replica_node_ids.map(n => `N${n}`).join(', ')}` : 'Upload an object to enable integrity tests.';
    $('#failureConsole').innerHTML = `
      <div class="console-head"><div><h3>Chaos, safely.</h3><p>Inject a node failure, network partition, byte corruption or stale-replica version and watch repair converge.</p></div></div>
      <div class="select-row"><label class="label">Target node<select class="select" id="nodeSelect">${opts}</select></label><label class="label">Target object<select class="select" id="objectSelect">${objectOptions || '<option value="">No objects yet</option>'}</select></label></div>
      <div class="console-hint">${esc(replicaHint)}</div>
      <div class="control-grid">
        <button class="btn danger" data-action="kill">Kill node</button>
        <button class="btn" data-action="partition">Partition</button>
        <button class="btn" data-action="revive">Revive node</button>
        <button class="btn" data-action="heal">Heal network</button>
        <button class="btn" data-action="corrupt" ${object ? '' : 'disabled'}>Corrupt replica</button>
        <button class="btn" data-action="inconsistent" ${object ? '' : 'disabled'}>Create stale replica</button>
        <button class="btn primary" data-action="repair">Reconcile cluster</button>
        <button class="btn wide" data-action="chaos">Chaos burst</button>
      </div>
    `;
    $('#nodeSelect').onchange = e => state.selectedNode = Number(e.target.value);
    $$('[data-action]', $('#failureConsole')).forEach(b => b.onclick = () => failureAction(b.dataset.action, Number($('#nodeSelect').value), $('#objectSelect').value));
  }

  async function failureAction(action, nodeId, objectId) {
    if (state.actionBusy) return;
    if (!state.chaos && ['kill','partition','corrupt','inconsistent','chaos'].includes(action)) {
      return toast('Chaos mode paused', 'Turn on the switch to enable failure injection.', true);
    }
    state.actionBusy = true;
    const buttons = $$('[data-action]', $('#failureConsole'));
    buttons.forEach(b => { b.disabled = true; });
    try {
      if (action === 'kill') await post(`/api/nodes/${nodeId}/kill`);
      if (action === 'partition') await post(`/api/nodes/${nodeId}/partition`);
      if (action === 'revive') await post(`/api/nodes/${nodeId}/revive`);
      if (action === 'heal') await post(`/api/nodes/${nodeId}/heal`);
      if (action === 'repair') await post('/api/repair');
      if (action === 'chaos') await post('/api/chaos');
      if (action === 'corrupt' || action === 'inconsistent') {
        const selected = objectId || state.cluster?.objects?.[0]?.object_id;
        if (!selected) throw new Error('Upload an object first.');
        const selectedObj = (state.cluster?.objects || []).find(o => o.object_id === selected);
        if (!selectedObj?.replica_node_ids?.length) throw new Error('This object has no known replica nodes yet.');
        if (['corrupt','inconsistent'].includes(action) && !selectedObj.replica_node_ids.includes(nodeId)) {
          throw new Error(`Node ${nodeId} does not store the selected object chunk. Choose one of: ${selectedObj.replica_node_ids.map(n => `N${n}`).join(', ')}`);
        }
        const detail = await fetchJson(`/api/objects/${encodeURIComponent(selected)}`);
        const chunkId = detail?.chunks?.[0]?.chunk_id;
        if (!chunkId) throw new Error('Selected object has no stored chunks.');
        const endpoint = action === 'corrupt' ? 'corrupt' : 'inconsistent';
        await post(`/api/nodes/${nodeId}/${endpoint}/${encodeURIComponent(chunkId)}`);
      }
      toast('Operation completed', `${action.toUpperCase()} completed for Node ${nodeId}.`);
      await refresh();
    } catch (err) {
      toast('Operation failed', err.message, true);
    } finally {
      state.actionBusy = false;
      const freshButtons = $$('[data-action]', $('#failureConsole'));
      freshButtons.forEach(b => {
        b.disabled = false;
        const actionName = b.dataset.action;
        if (['corrupt','inconsistent'].includes(actionName)) b.disabled = !(state.cluster?.objects?.length);
      });
    }
  }

  function renderStorage(s) {
    const physical = Number(s.physical_bytes || 0), logical = Number(s.logical_bytes || 0);
    const pctUsed = Math.min(100, Number(s.storage_utilization_pct || 0));
    $('#storagePanel').innerHTML = `<div class="donut" style="--pct:${pctUsed}%"><div class="donut-center"><b>${pct(pctUsed)}</b><span>utilized</span></div></div><div class="legend"><span><i style="background:var(--red)"></i>physical ${fmt(physical)}</span><span><i style="background:var(--teal)"></i>logical ${fmt(logical)}</span><span><i style="background:var(--brown)"></i>overhead ${Number(s.replication_overhead || 0).toFixed(2)}×</span><span><i style="background:var(--cream)"></i>nodes ${s.nodes || 0}</span></div>`;
  }

  function renderThroughput(activity) {
    const series = (activity || []).slice(-20);
    const W = 640, H = 185, pad = 18;
    if (!series.length) { $('#throughputPanel').innerHTML = '<div class="empty">No activity samples yet.</div>'; return; }
    const max = Math.max(1, ...series.map(x => Math.max(Number(x.read_bytes || 0), Number(x.write_bytes || 0))));
    const point = (v, i) => `${pad + (i / Math.max(1, series.length - 1)) * (W - pad * 2)},${H - pad - (v / max) * (H - pad * 2)}`;
    const readPts = series.map((x, i) => point(Number(x.read_bytes || 0), i)).join(' ');
    const writePts = series.map((x, i) => point(Number(x.write_bytes || 0), i)).join(' ');
    const grid = [0, .25, .5, .75, 1].map(r => { const y = H - pad - r * (H - pad * 2); return `<line class="gridline" x1="${pad}" x2="${W - pad}" y1="${y}" y2="${y}"/>`; }).join('');
    $('#throughputPanel').innerHTML = `<svg class="chart" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">${grid}<polyline class="read" points="${readPts}"/><polyline class="write" points="${writePts}"/></svg><div class="legend"><span><i style="background:var(--teal)"></i>read ${fmt(series.at(-1).read_bytes || 0)}</span><span><i style="background:var(--red)"></i>write ${fmt(series.at(-1).write_bytes || 0)}</span></div>`;
  }

  function renderIntegrity(i) {
    const pctGood = Number(i.checksum_ok_pct ?? 100), corrupt = Number(i.corrupted || 0), inconsistent = Number(i.inconsistent || 0), repaired = Number(i.repaired || 0), checked = Number(i.checks || 0);
    $('#integrityPanel').innerHTML = `<div class="integrity"><div class="integrity-ring" style="background:conic-gradient(var(--teal) ${pctGood}%, rgba(144,174,173,.12) 0)"><div><b>${pctGood.toFixed(1)}%</b><span>verified</span></div></div><div class="integrity-stats"><div><span>Checks performed</span><b>${checked.toLocaleString()}</b></div><div><span>Corrupted</span><b>${corrupt}</b></div><div><span>Inconsistent</span><b>${inconsistent}</b></div><div><span>Repaired</span><b>${repaired}</b></div></div></div>`;
  }

  function renderObjects(objects) {
    const items = state.filter ? objects.filter(o => `${o.name} ${o.status} ${o.object_id}`.toLowerCase().includes(state.filter)) : objects;
    $('#objectList').innerHTML = items.slice(0, 10).map(o => {
      const healthyRep = Number(o.healthy_replica_count ?? o.replica_count ?? 0), rf = Number(o.replication_factor || 1), p = rf ? Math.min(100, healthyRep / rf * 100) : 0;
      return `<button class="row" data-object="${esc(o.object_id)}"><div class="row-main"><b>${esc(o.name)}</b><small>${esc(o.status)} · v${o.version} · ${fmt(o.size_bytes)} · W=${o.write_quorum ?? 1}/R=${o.read_quorum ?? 1}</small><div class="progress ${o.status === 'UNDER_REPLICATED' ? 'red' : ''}"><span style="width:${p}%"></span></div></div><div class="row-meta"><b>${healthyRep}/${rf}</b><small>replicas</small></div></button>`;
    }).join('') || '<div class="empty">No objects yet. Upload a file to start the fabric.</div>';
    $$('[data-object]', $('#objectList')).forEach(el => el.onclick = () => openObject(el.dataset.object));
  }

  function renderRepairs(repairs) {
    const items = repairs.slice(0, 8);
    $('#repairList').innerHTML = items.map(r => {
      const running = r.status === 'RUNNING';
      const dur = r.ts_completed && r.ts_started ? `${((r.ts_completed - r.ts_started) * 1000).toFixed(0)} ms` : (running ? 'in progress' : '—');
      return `<div class="row"><div class="row-main"><b>${esc(r.chunk_id)}</b><small>${esc(r.reason)} · ${r.source_node ?? '—'} → ${r.target_node ?? '—'}</small><div class="progress ${r.status === 'FAILED' ? 'red' : ''}"><span style="width:${running ? 68 : r.status === 'COMPLETED' ? 100 : 25}%"></span></div></div><div class="row-meta"><b>${esc(r.status)}</b><small>${dur}</small></div></div>`;
    }).join('') || '<div class="empty">Repair queue is clear.</div>';
  }

  function renderEvents(events) {
    $('#events').innerHTML = events.slice(0, 22).map(e => `<div class="event ${String(e.level || '').toLowerCase()}"><span class="event-dot"></span><div><b>${esc(e.message)}</b><small>${esc(e.kind)} · ${e.ts ? ago(e.ts) + ' ago' : ''}</small></div><time>${e.ts ? new Date(e.ts * 1000).toLocaleTimeString([], { hour12: false }) : ''}</time></div>`).join('') || '<div class="empty">No events recorded yet.</div>';
  }

  function openUpload() {
    const healthy = Number(state.cluster?.summary?.healthy_nodes || 0);
    const maxRf = Math.max(1, Number(state.policies?.node_count || 8));
    showModal(`<div class="modal-head"><div><div class="eyebrow">Object intake</div><h3 style="margin:7px 0 0;font-size:20px">Upload to VAULT</h3></div><button class="close" data-close>×</button></div>
      <div class="modal-body">
        <label class="label">Object files
          <input class="input" id="upFiles" type="file" multiple />
        </label>
        <div class="upload-summary" id="uploadSummary">Choose one or more files. Each file is stored independently with the same policy.</div>
        <div class="detail-grid" style="margin-top:12px">
          <label class="label detail-box">Replication factor<select class="select" id="upRf">${Array.from({length:maxRf},(_,i)=>i+1).map(x=>`<option ${x === 3 ? 'selected' : ''}>${x}</option>`).join('')}</select></label>
          <label class="label detail-box">Write quorum<select class="select" id="upW">${Array.from({length:maxRf},(_,i)=>i+1).map(x=>`<option ${x === 2 ? 'selected' : ''}>${x}</option>`).join('')}</select></label>
          <label class="label detail-box">Read quorum<select class="select" id="upR">${Array.from({length:maxRf},(_,i)=>i+1).map(x=>`<option ${x === 1 ? 'selected' : ''}>${x}</option>`).join('')}</select></label>
          <div class="detail-box upload-health"><span>Healthy nodes now</span><b>${healthy}/${Number(state.policies?.node_count || 0)}</b><small id="uploadPolicyHint">RF cannot exceed the healthy-node count.</small></div>
        </div>
      </div>
      <div class="modal-actions"><button class="btn" data-close>Cancel</button><button class="btn primary" id="confirmUpload">Upload files</button></div>`);

    const syncPolicySelects = () => {
      const rf = Number($('#upRf').value);
      ['#upW', '#upR'].forEach(sel => {
        const el = $(sel);
        $$('option', el).forEach(o => { o.disabled = Number(o.value) > rf; });
        if (Number(el.value) > rf) el.value = String(rf);
      });
      const available = Number(state.cluster?.summary?.healthy_nodes || 0);
      const hint = $('#uploadPolicyHint');
      const submit = $('#confirmUpload');
      const blocked = rf > available || available === 0;
      hint.textContent = available === 0 ? 'No healthy nodes are available.' : (rf > available ? `RF ${rf} needs ${rf} healthy nodes; only ${available} are ready.` : 'Policy is currently available.');
      hint.style.color = blocked ? 'var(--danger)' : 'var(--muted)';
      submit.disabled = blocked;
      submit.style.opacity = blocked ? '.45' : '1';
    };

    $('#upRf').onchange = syncPolicySelects;
    $('#upFiles').onchange = () => {
      const files = [...($('#upFiles').files || [])];
      const total = files.reduce((sum, f) => sum + f.size, 0);
      $('#uploadSummary').innerHTML = files.length ? `<b>${files.length}</b> file${files.length > 1 ? 's' : ''} selected · <b>${fmt(total)}</b> total<br><span>${files.slice(0, 6).map(f => esc(f.name)).join(' · ')}${files.length > 6 ? ' · …' : ''}</span>` : 'Choose one or more files. Each file is stored independently with the same policy.';
      syncPolicySelects();
    };
    syncPolicySelects();

    $('#confirmUpload').onclick = async () => {
      const files = [...($('#upFiles').files || [])];
      if (!files.length) return toast('Upload blocked', 'Choose at least one file.', true);
      const rf = Number($('#upRf').value), w = Number($('#upW').value), r = Number($('#upR').value);
      if (rf > Number(state.cluster?.summary?.healthy_nodes || 0)) return toast('Upload blocked', 'The selected replication factor is not currently available.', true);
      const form = new FormData();
      files.forEach(f => form.append('files', f));
      form.append('replication_factor', rf);
      form.append('write_quorum', w);
      form.append('read_quorum', r);
      const btn = $('#confirmUpload');
      btn.disabled = true;
      btn.textContent = `Uploading ${files.length} file${files.length === 1 ? '' : 's'}…`;
      try {
        const res = await fetchJson('/api/objects/batch', { method: 'POST', body: form });
        closeModal();
        if (res.failed) {
          const failedNames = (res.errors || []).map(x => x.name).join(', ');
          toast('Batch upload finished', `${res.succeeded}/${res.total} stored. Failed: ${failedNames || res.failed}.`, res.succeeded === 0);
        } else {
          toast('Batch upload complete', `${res.succeeded} file${res.succeeded === 1 ? '' : 's'} stored with RF=${rf}, W=${w}, R=${r}.`);
        }
        await refresh();
      } catch (e) {
        btn.disabled = false;
        btn.textContent = 'Upload files';
        toast('Batch upload failed', e.message, true);
      }
    };
  }

  async function openObject(id) {
    try {
      const o = await fetchJson(`/api/objects/${encodeURIComponent(id)}`);
      const chunks = (o.chunks || []).map(c => `<div class="row"><div class="row-main"><b>${esc(c.chunk_id)}</b><small>${fmt(c.size_bytes)} · SHA ${esc(c.checksum.slice(0, 18))}…</small></div><div class="row-meta"><b>${c.healthy_replica_count || 0}/${o.replication_factor}</b><small>healthy replicas</small></div></div>`).join('');
      showModal(`<div class="modal-head"><div><div class="eyebrow">Object detail</div><h3 style="margin:7px 0 0;font-size:20px">${esc(o.name)}</h3></div><button class="close" data-close>×</button></div><div class="modal-body"><div class="detail-grid"><div class="detail-box"><span>Status</span><b class="readable-status">${esc(o.status).replaceAll("_", " ")}</b></div><div class="detail-box"><span>Size</span><b>${fmt(o.size_bytes)}</b></div><div class="detail-box"><span>Version</span><b>v${o.version}</b></div><div class="detail-box"><span>Replication</span><b>${o.replication_factor}×</b></div><div class="detail-box"><span>Durability</span><b>W=${o.write_quorum ?? 1} · R=${o.read_quorum ?? 1}</b></div><div class="detail-box"><span>Chunks</span><b>${o.chunks?.length || 0}</b></div><div class="detail-box" style="grid-column:1/-1"><span>SHA-256</span><b>${esc(o.checksum)}</b></div></div><div style="margin-top:14px"><div class="eyebrow">Chunk placement</div>${chunks || '<div class="empty">No chunks.</div>'}</div></div><div class="modal-actions"><button class="btn danger" id="deleteObjectBtn">Delete</button><a class="btn primary" href="${API}/api/objects/${encodeURIComponent(o.object_id)}/download" target="_blank" rel="noreferrer">Download</a></div>`);
      $('#deleteObjectBtn').onclick = async () => {
        if (!confirm(`Delete ${o.name}?`)) return;
        try {
          await fetchJson(`/api/objects/${encodeURIComponent(o.object_id)}`, { method: 'DELETE' });
          closeModal();
          toast('Object deleted', 'Metadata and best-effort node copies were removed.');
          await refresh();
        } catch (e) { toast('Delete failed', e.message, true); }
      };
    } catch (e) { toast('Object unavailable', e.message, true); }
  }

  function showModal(html) {
    const b = $('#modalBackdrop');
    b.innerHTML = `<div class="modal">${html}</div>`;
    b.classList.add('show');
    $$('[data-close]', b).forEach(el => el.onclick = closeModal);
    b.onclick = e => { if (e.target === b) closeModal(); };
  }
  function closeModal() { $('#modalBackdrop').classList.remove('show'); $('#modalBackdrop').innerHTML = ''; }

  function toast(title, detail, error = false) {
    const host = $('#toastStack');
    if (!host) return;
    const el = document.createElement('div');
    el.className = 'toast';
    el.innerHTML = `<b>${esc(title)}</b><span>${esc(detail)}</span>`;
    if (error) el.style.borderColor = 'rgba(230,75,51,.55)';
    host.appendChild(el);
    setTimeout(() => el.remove(), 4600);
  }

  function connectSocket() {
    try {
      state.socket = new WebSocket(WS);
      state.socket.onopen = () => { state.socketRetry = 0; toast('Live channel connected', 'Real-time cluster events are flowing.'); };
      state.socket.onmessage = ev => {
        try {
          const item = JSON.parse(ev.data);
          state.events.unshift(item);
          state.events = state.events.slice(0, 100);
          renderEvents(state.events);
          setTimeout(refresh, 180);
        } catch {}
      };
      state.socket.onclose = () => { const delay = Math.min(15000, 1200 * (++state.socketRetry)); setTimeout(connectSocket, delay); };
      state.socket.onerror = () => state.socket.close();
    } catch { setTimeout(connectSocket, 4000); }
  }

  function observeMotion() {
    const io = new IntersectionObserver(entries => entries.forEach(e => { if (e.isIntersecting) e.target.classList.add('visible'); }), { threshold: .08 });
    $$('[data-motion]').forEach(el => io.observe(el));
  }

  injectShell();
  refresh();
  setInterval(() => refresh(false), 2000);
  connectSocket();
})();
