// Browser check of the built page in docs/, served as static files under a strict content policy, with no backend.
// Run:  node browser/test/e2e_live.js   (needs Playwright; set PLAYWRIGHT_PATH if it is not on the module path)
const { chromium } = require(process.env.PLAYWRIGHT_PATH || 'playwright');
const http = require('http'), fs = require('fs'), path = require('path');
const OUT = path.join(__dirname, '..', '..', 'docs'), PORT = 8793, B = `http://127.0.0.1:${PORT}`;
const CSP = "default-src 'none'; script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; style-src 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data: blob:; connect-src 'self'";
const srv = http.createServer((req, res) => { const f = req.url.split('?')[0] === '/' ? 'index.html' : req.url.split('?')[0].slice(1);
  const p = path.join(OUT, f); if (!fs.existsSync(p)) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'content-type': f.endsWith('.js') ? 'text/javascript' : 'text/html; charset=utf-8', 'content-security-policy': CSP }); res.end(fs.readFileSync(p)); }).listen(PORT);
const checks = [], errs = [];
const check = (name, ok, extra) => { checks.push([name, !!ok]); console.log((ok ? '  PASS ' : '  FAIL ') + name + (!ok && extra ? '  ' + extra : '')); };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const SHOTS = path.join(__dirname, 'shots'); fs.mkdirSync(SHOTS, { recursive: true });
(async () => {
  const b = await chromium.launch();
  const open = async (ctx, tag) => { const p = await ctx.newPage();
    p.on('pageerror', e => errs.push(tag + ' pageerror: ' + e.message));
    p.on('console', m => { if (m.type() === 'error' && !/fonts\.g|ERR_|Failed to load resource/.test(m.text())) errs.push(tag + ' console: ' + m.text()); });
    await p.goto(B + '/'); await p.waitForFunction(() => S.ready, null, { timeout: 30000 }); return p; };
  const sweep = async (p, tag) => { for (const v of await p.evaluate(() => Object.keys(VIEWS))) { await p.evaluate(v => go(v), v); await sleep(120);
    const o = await p.evaluate(() => ({ a: document.getElementById('content').scrollWidth, b: document.getElementById('content').clientWidth, d: document.documentElement.scrollWidth, e: document.documentElement.clientWidth }));
    if (o.a > o.b + 1 || o.d > o.e + 1) errs.push(`${tag} overflow on ${v}`); } };
  const waitRun = async (p, pid) => { for (let i = 0; i < 200; i++) { const s = await p.evaluate(pid => { const r = S.runs.find(r => r.pid === pid); return r ? r.status : 'none'; }, pid); if (s !== 'running' && s !== 'none') return s; await sleep(100); } return 'timeout'; };
  const toastText = p => p.evaluate(() => [...document.querySelectorAll('.toast')].map(t => t.textContent).join(' | '));
  const ctx = await b.newContext({ viewport: { width: 1360, height: 900 }, colorScheme: 'light' });
  const p = await open(ctx, 'desk');
  check('the page starts without a server and the SQL engine loads under a strict content policy', await p.evaluate(() => S.ready && typeof initSqlJs === 'function'));
  check('it opens with a local engine, a landing area and no pipelines', await p.evaluate(() => S.pipelines.length === 0 && S.connections.length === 2 && S.connections.every(c => c.status === 'healthy')));
  check('the overview says what this copy is', (await p.textContent('#view')).includes('This copy runs in your browser') && (await p.textContent('#view')).includes('No pipelines yet'));
  await p.screenshot({ path: path.join(SHOTS, 'start.png') });
  await sweep(p, 'empty');
  await p.click('.rail-foot [data-act="liveAbout"]'); check('the explanation of what works here opens', (await p.textContent('#drawer-body')).includes('Needs the deployed service'));
  await p.screenshot({ path: path.join(SHOTS, 'about.png') }); await p.evaluate(() => ACT.closeAll());

  // connections: a second landing area through the real form, then uploads
  await p.evaluate(() => go('connections')); await p.click('[data-act="connNew"]');
  check('a new connection defaults to a landing area and needs no address', await p.evaluate(() => document.getElementById('cn-type').value === 'folder' && !document.getElementById('cn-endpoint')));
  await p.fill('#cn-id', 'inbox'); await p.click('#drawer-body form button'); await p.waitForFunction(() => S.connections.length === 3);
  await p.click('[data-act="connNew"]'); await p.selectOption('#cn-type', 'bq'); await p.fill('#cn-id', 'bq_prod'); await p.fill('#cn-endpoint', 'my-project'); await p.click('#drawer-body form button');
  await p.waitForFunction(() => S.connections.length === 4);
  check('a BigQuery connection is saved but reported unreachable, with the reason', await p.evaluate(() => { const c = S.connections.find(c => c.id === 'bq_prod'); return c.status === 'down' && /cannot reach BigQuery/.test(c.note); }), await toastText(p));
  await p.click('[data-act="filesOpen"][data-id="inbox"]'); await p.fill('#up-folder', 'orders');
  await p.setInputFiles('#up-files', [{ name: 'day1.csv', mimeType: 'text/csv', buffer: Buffer.from('order_id,customer_email,amount,order_date\n1,a@example.com,10.50,2026-10-01\n2,b@example.com,20,2026-10-01\n3,c@example.com,oops,2026-10-02\n') },
    { name: 'notes.txt', mimeType: 'text/plain', buffer: Buffer.from('hello') }]);
  await p.click('#drawer-body form button.primary'); await p.waitForFunction(() => /orders\/day1\.csv/.test(document.getElementById('files-list').textContent), null, { timeout: 8000 });
  check('a file chosen from the device is uploaded, and a wrong file type is refused with the reason', /notes\.txt is not a CSV or JSON file/.test(await toastText(p)), await toastText(p));
  await p.click('#drawer-body details summary'); await p.fill('#up-name', 'day2'); await p.fill('#up-text', 'order_id,customer_email,amount,order_date\n4,d@example.com,5.25,2026-10-02\n');
  await p.click('#drawer-body form button.primary'); await p.waitForFunction(() => /orders\/day2\.csv/.test(document.getElementById('files-list').textContent), null, { timeout: 8000 });
  check('pasted data is uploaded as a file', true);
  await p.screenshot({ path: path.join(SHOTS, 'files.png') }); await p.evaluate(() => ACT.closeAll());

  // builder through the real controls, on the sample file
  await p.evaluate(() => go('builder'));
  check('the builder offers the local engine and its landing area', await p.evaluate(() => S.builder.target === 'lake' && S.builder.conn === 'landing'));
  await p.waitForSelector('[data-act="bFolder"][data-folder="vendor_returns"]'); await p.click('[data-act="bFolder"][data-folder="vendor_returns"]');
  check('the builder suggests the folders that hold files, and one click fills the source', await p.evaluate(() => S.builder.path === 'vendor_returns' && S.builder.name === 'vendor_returns'));
  await p.click('[data-act="bBrowse"]'); await p.waitForFunction(() => /2026-10-01\.csv/.test(document.getElementById('drawer-body').textContent));
  check('the builder lists the files in the folder', true);
  await p.evaluate(() => ACT.closeAll());
  await p.click('[data-act="bDetect"]'); await p.waitForFunction(() => /refund_amount:DECIMAL/.test(S.builder.columns));
  check('columns and types are read from the file', await p.evaluate(() => /return_id:STRING:key/.test(S.builder.columns) && /return_date:DATE/.test(S.builder.columns) && /customer_email:STRING:pii/.test(S.builder.columns)), await p.evaluate(() => S.builder.columns));
  await p.waitForFunction(() => S.ui.plan && S.ui.plan.data, null, { timeout: 8000 });
  check('the code pane shows the SQL that will run', (await p.textContent('#codepane')).includes('vendor_returns_raw'));
  await p.screenshot({ path: path.join(SHOTS, 'builder.png') });
  for (const i of [1, 2, 3]) await p.click(`.steps button[data-i="${i}"]`);
  await p.fill('#b-gold-name', 'agg_returns_daily'); await p.fill('#b-gold-dateCol', 'return_date'); await p.fill('#b-gold-dims', 'reason, channel');
  await p.fill('#b-gold-measures', 'COUNT(*) AS returns\nSUM(refund_amount) AS refund_total');
  await p.click('.steps button[data-i="5"]'); await p.click('[data-act="bDeploy"][data-run="1"]');
  await p.waitForFunction(() => S.pipelines.length === 1 && S.view === 'orchestration', null, { timeout: 15000 });
  let status = await waitRun(p, 'vendor_returns_medallion');
  check('first run stops on the missing key, as the default check demands', status === 'failed', status);
  check('the failure reason is shown on the page', (await p.textContent('#view')).includes('not null'));
  await p.evaluate(() => go('controls'));
  const nn = await p.evaluate(() => S.rules.find(r => r.type === 'not_null').id);
  await p.selectOption(`#rule-sev-${nn}`, 'quarantine'); await p.fill('#ctl-breaker', '50'); await p.dispatchEvent('#ctl-breaker', 'change'); await sleep(600);
  check('the forecast updates with the controls', await p.evaluate(() => outcome(predict(S.pipelines[0], 'null_keys'))[1]) === 'Succeeds with warnings');
  await p.evaluate(() => { go('orchestration'); ACT.retryRun({ id: S.runs[0].id }); }); await sleep(300);
  status = await waitRun(p, 'vendor_returns_medallion');
  const run = await p.evaluate(() => S.runs[0]);
  check('retry from the failed task succeeds with the new controls', status === 'success' && run.rows.silver === 8 && run.rows.gold === 8 && run.quarantined === 3, JSON.stringify(run.rows));
  await p.click(`tr[data-act="openRun"]`); check('the run drawer shows the real log', (await p.textContent('#drawer-body')).includes('Cast and validated 12 rows'));
  await p.screenshot({ path: path.join(SHOTS, 'run.png') });
  await p.evaluate(() => ACT.closeAll());
  await p.screenshot({ path: path.join(SHOTS, 'orchestration.png') });

  await p.evaluate(() => { go('silver'); ACT.layerTab({ tab: 'preview' }); });
  await p.waitForFunction(() => /R1001/.test(document.getElementById('view').textContent), null, { timeout: 8000 });
  check('Silver preview shows real rows with hashed emails', !(await p.textContent('#view')).includes('@example.com'));
  await p.screenshot({ path: path.join(SHOTS, 'silver.png') });
  await p.evaluate(() => { S.ui.layerTab = 'schema'; go('quality'); });
  await p.click('[data-act="qView"]'); await p.waitForFunction(() => /not null|castable/.test(document.getElementById('drawer-body').textContent));
  check('quarantined rows can be inspected with their reason', (await p.textContent('#drawer-body')).includes('12,90'));
  await p.evaluate(() => ACT.closeAll()); await p.click('[data-act="dqRun"]'); await sleep(900);
  check('checks can be run on demand', /checks evaluated/.test(await toastText(p)), await toastText(p));
  await p.evaluate(() => go('mappings'));
  await p.click('[data-act="mapAdd"]'); const n = await p.evaluate(() => S.ui.mapDraft.rows.length - 1);
  await p.fill(`#map-${n}-src`, 'channel'); await p.dispatchEvent(`#map-${n}-src`, 'change'); await sleep(80);
  await p.fill(`#map-${n}-tgt`, 'channel_upper'); await p.dispatchEvent(`#map-${n}-tgt`, 'change'); await sleep(80);
  await p.fill(`#map-${n}-tx`, 'upper'); await p.dispatchEvent(`#map-${n}-tx`, 'change'); await sleep(80);
  await p.click('[data-act="mapSave"]'); await p.waitForFunction(() => !S.ui.mapDraft && S.mappings['silver.vendor_returns'].rows.some(r => r.tgt === 'channel_upper'), null, { timeout: 8000 });
  check('a mapping edit is saved', true);
  await p.evaluate(() => { go('orchestration'); ACT.trigger({ id: 'vendor_returns_medallion', full: '1' }); }); await sleep(300);
  status = await waitRun(p, 'vendor_returns_medallion');
  await p.evaluate(() => { S.cache = {}; go('silver'); ACT.layerTab({ tab: 'preview' }); });
  await p.waitForFunction(() => /channel_upper/.test(document.getElementById('view').textContent), null, { timeout: 8000 });
  check('the new column appears in Silver after a full reload', status === 'success' && /WEB|APP|STORE/.test(await p.textContent('#view')));
  await p.evaluate(() => { S.ui.layerTab = 'schema'; });

  // a second pipeline on the uploaded files, deployed through the API of the page
  const cfg2 = { name: 'orders', target: 'lake', conn: 'inbox', kind: 'files', format: 'csv', path: 'orders', pattern: 'incremental', columns: 'order_id:BIGINT:key\ncustomer_email:STRING:pii\namount:DECIMAL(18,2)\norder_date:DATE', scd: '2', onBad: 'quarantine', cron: '0 6 * * *', retries: 0,
    gold: { name: 'orders_daily', type: 'aggregate', dateCol: 'order_date', dims: '', measures: 'COUNT(*) AS orders\nSUM(amount) AS revenue' } };
  await p.evaluate(async cfg => { const pl = await api('POST', '/api/pipelines', { cfg }); await api('PUT', `/api/pipelines/${pl.id}/controls`, { key: 'breaker', value: 50 }); await api('POST', `/api/pipelines/${pl.id}/run`, {}); }, cfg2);
  status = await waitRun(p, 'orders_medallion');
  const r2 = await p.evaluate(() => S.runs.find(r => r.pid === 'orders_medallion'));
  check('a pipeline over the uploaded files runs: 4 rows land, the bad amount is quarantined', status === 'success' && r2.rows.bronze === 4 && r2.rows.silver === 3 && r2.quarantined === 1, JSON.stringify(r2).slice(0, 300));

  // platform and governance
  await p.evaluate(() => go('mcp')); await p.waitForFunction(() => S.cache.tools && S.cache.tools.data);
  await p.click('[data-act="toolTry"][data-tool="run_sql"]'); await p.fill('#tool-args', '{"sql": "SELECT COUNT(*) AS n FROM silver.vendor_returns"}'); await p.click('#drawer-body form button');
  await p.waitForFunction(() => /"n": 8/.test(document.getElementById('tool-result').textContent));
  check('a tool call from the MCP page returns real data', true);
  await p.evaluate(() => ACT.closeAll());
  const r = await p.evaluate(() => BE.mcp({ jsonrpc: '2.0', id: 9, method: 'tools/call', params: { name: 'pause_pipeline', arguments: { pipeline: 'vendor_returns_medallion' } } }));
  await p.evaluate(() => { LAST = ''; return sync(true); }); await p.waitForSelector('[data-act="approval"][data-ok="1"]');
  check('a change requested through the MCP handler waits for approval', JSON.parse(r.result.content[0].text).status === 'waiting_for_approval');
  await p.click('[data-act="approval"][data-ok="1"]'); await p.waitForFunction(() => S.pipelines[0].status === 'paused');
  check('approving it applies the change', true);
  await p.screenshot({ path: path.join(SHOTS, 'mcp.png') });
  await p.evaluate(() => ACT.togglePipe({ id: 'vendor_returns_medallion' })); await sleep(400);
  await p.evaluate(() => go('compute')); await p.waitForFunction(() => /In-browser SQLite/.test(document.getElementById('view').textContent));
  check('compute is read from the engine', true);
  await p.evaluate(() => { go('governance'); ACT.govTab({ tab: 'pii' }); });
  await p.fill('#erase-col', 'return_id'); await p.fill('#erase-val', 'R1002'); await p.click('form[data-form="erase"] button');
  await p.waitForSelector('[data-act="eraseConfirm"]'); await p.click('[data-act="eraseConfirm"]'); await p.waitForFunction(() => S.ui.erase.done);
  check('an erasure request finds and deletes the rows', true);
  for (const t of ['access', 'secrets', 'audit']) { await p.evaluate(t => ACT.govTab({ tab: t }), t); await sleep(150); }
  await p.evaluate(() => go('agent')); check('the agent page says it needs the deployed service', (await p.textContent('#view')).includes('The agent needs the deployed service'));
  await p.evaluate(() => go('cicd')); await p.click('[data-act="exportZip"]'); await p.waitForFunction(() => /sql\/vendor_returns_medallion\/silver\.sql/.test(document.getElementById('drawer-body').textContent), null, { timeout: 8000 });
  check('the code export shows every file to copy', (await p.textContent('#drawer-body')).includes('controls/vendor_returns_medallion.yml'));
  await p.evaluate(() => ACT.closeAll());
  await p.evaluate(() => { go('lineage'); ACT.lineageSel({ id: 'silver.vendor_returns' }); }); await p.screenshot({ path: path.join(SHOTS, 'lineage.png') });
  await p.evaluate(() => { go('overview'); }); await sleep(300); await p.screenshot({ path: path.join(SHOTS, 'overview.png') });
  await p.evaluate(() => go('controls')); await p.screenshot({ path: path.join(SHOTS, 'controls.png') });
  await p.evaluate(() => go('monitoring')); await p.screenshot({ path: path.join(SHOTS, 'monitoring.png') });
  await sweep(p, 'filled');

  // the work survives a reload of the page
  await sleep(1200);
  const before = await p.evaluate(async () => ({ pipes: S.pipelines.length, rows: (await api('POST', '/api/tools/run_sql', { args: { sql: 'SELECT COUNT(*) AS n FROM silver.vendor_returns' } })).result.rows[0].n, hist: (await api('POST', '/api/tools/run_sql', { args: { sql: 'SELECT COUNT(*) AS n FROM silver.orders WHERE is_current' } })).result.rows[0].n, keeps: BE.info.keeps }));
  await p.reload(); await p.waitForFunction(() => S.ready, null, { timeout: 30000 });
  const after = await p.evaluate(async () => ({ pipes: S.pipelines.length, rows: (await api('POST', '/api/tools/run_sql', { args: { sql: 'SELECT COUNT(*) AS n FROM silver.vendor_returns' } })).result.rows[0].n, hist: (await api('POST', '/api/tools/run_sql', { args: { sql: 'SELECT COUNT(*) AS n FROM silver.orders WHERE is_current' } })).result.rows[0].n, files: (await api('GET', '/api/local/files/inbox')).files.length, restored: BE.info.restored }));
  check('after a reload the pipelines, files and table contents are still there', after.restored && after.pipes === 2 && after.rows === before.rows && after.hist === before.hist && after.files === 2 && before.keeps === 'everything', JSON.stringify({ before, after }));
  await p.evaluate(() => { go('orchestration'); ACT.trigger({ id: 'vendor_returns_medallion' }); }); await sleep(300); status = await waitRun(p, 'vendor_returns_medallion');
  check('a run after the reload sees that the files were already loaded', status === 'success' && await p.evaluate(() => Object.values(S.runs[0].tasks).filter(t => t.status === 'skipped').length === 5), status);

  // phone, dark
  const ctx2 = await b.newContext({ viewport: { width: 400, height: 820 }, colorScheme: 'dark', storageState: await ctx.storageState() });
  const p2 = await open(ctx2, 'phone');
  await sweep(p2, 'phone'); await p2.evaluate(() => go('orchestration')); await p2.screenshot({ path: path.join(SHOTS, 'phone.png') });
  await p2.evaluate(() => { go('connections'); ACT.filesOpen({ id: 'landing' }); }); await sleep(400); await p2.screenshot({ path: path.join(SHOTS, 'phone-files.png') });
  await p2.evaluate(() => { ACT.closeAll(); go('builder'); }); await sleep(300); await p2.screenshot({ path: path.join(SHOTS, 'phone-builder.png') });

  // delete, then start over
  await p.evaluate(() => go('pipelines')); await p.click('[data-act="delPipe"]'); await p.check('#del-drop'); await p.click('#drawer-body form button');
  await p.waitForFunction(() => S.pipelines.length === 1, null, { timeout: 10000 });
  check('a pipeline can be deleted together with its tables', await p.evaluate(() => S.tables.length === 3));
  await p.click('.rail-foot [data-act="liveAbout"]'); await p.click('[data-act="liveReset"]'); await p.click('[data-act="liveReset"]');
  await p.waitForFunction(() => S.pipelines.length === 0 && S.connections.length === 2, null, { timeout: 10000 });
  check('Start over puts the copy back to its starting state', await p.evaluate(() => S.tables.length === 0 && S.runs.length === 0));
  await b.close();
  if (errs.length) console.log('BROWSER ERRORS:\n' + errs.join('\n'));
  check('no errors in the browser console, no content-policy violations, no sideways scrolling', errs.length === 0);
})().catch(e => { console.error('SCRIPT FAIL', e); checks.push(['script', false]); }).finally(() => {
  srv.close(); const failed = checks.filter(c => !c[1]); console.log(`\n${checks.length - failed.length} of ${checks.length} checks passed`); process.exit(failed.length ? 1 : 0);
});
