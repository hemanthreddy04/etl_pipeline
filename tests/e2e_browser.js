// Browser check of the whole site against a live service on the local engine.
// Run:  node tests/e2e_browser.js   (needs Playwright; set PLAYWRIGHT_PATH if it is not on the module path)
const { chromium } = require(process.env.PLAYWRIGHT_PATH || 'playwright');
const { spawn } = require('child_process');
const fs = require('fs'), os = require('os'), path = require('path');
const ROOT = path.dirname(__dirname), work = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-e2e-'));
fs.cpSync(path.join(ROOT, 'samples', 'landing'), path.join(work, 'landing'), { recursive: true });
const PORT = 8771, B = `http://127.0.0.1:${PORT}`;
const srv = spawn('python3', ['-m', 'uvicorn', 'app.main:app', '--port', String(PORT), '--log-level', 'warning'],
  { cwd: ROOT, env: { ...process.env, STATE_URI: path.join(work, 'state.json'), RETRY_BASE_SECONDS: '0', APP_TOKEN: 'e2e-token' }, stdio: 'inherit' });
const checks = [], errs = [];
const check = (name, ok, extra) => { checks.push([name, !!ok]); console.log((ok ? '  PASS ' : '  FAIL ') + name + (!ok && extra ? '  ' + extra : '')); };
const sleep = ms => new Promise(r => setTimeout(r, ms));
(async () => {
  for (let i = 0; i < 80; i++) { try { await fetch(B + '/healthz'); break; } catch (e) { await sleep(150); } }
  const b = await chromium.launch();
  const open = async (w, h, scheme, tag) => {
    const ctx = await b.newContext({ viewport: { width: w, height: h }, colorScheme: scheme, acceptDownloads: true });
    const p = await ctx.newPage();
    p.on('pageerror', e => errs.push(tag + ' pageerror: ' + e.message));
    p.on('console', m => { if (m.type() === 'error' && !/fonts\.g|ERR_|Failed to load resource/.test(m.text())) errs.push(tag + ' console: ' + m.text()); });
    await p.goto(B + '/');
    return p;
  };
  const sweep = async (p, tag) => { for (const v of await p.evaluate(() => Object.keys(VIEWS))) { await p.evaluate(v => go(v), v); await sleep(120);
    const o = await p.evaluate(() => ({ a: document.getElementById('content').scrollWidth, b: document.getElementById('content').clientWidth, d: document.documentElement.scrollWidth, e: document.documentElement.clientWidth }));
    if (o.a > o.b + 1 || o.d > o.e + 1) errs.push(`${tag} overflow on ${v}`); } };
  const waitRun = async (p, pid) => { for (let i = 0; i < 200; i++) { const s = await p.evaluate(pid => { const r = S.runs.find(r => r.pid === pid); return r ? r.status : 'none'; }, pid); if (s !== 'running' && s !== 'none') return s; await sleep(150); } return 'timeout'; };
  const toastText = p => p.evaluate(() => [...document.querySelectorAll('.toast')].map(t => t.textContent).join(' | '));

  const p = await open(1360, 900, 'light', 'desk');
  await p.waitForSelector('#token-form');
  check('a protected service asks for the access token', true);
  await p.fill('#tk', 'e2e-token'); await p.click('#token-form button');
  await p.waitForFunction(() => S.ready);
  check('after the token the empty workspace loads', await p.evaluate(() => S.pipelines.length === 0 && S.connections.length === 0));
  check('overview tells a new user to connect a warehouse', (await p.textContent('#view')).includes('Start by connecting a warehouse'));
  await sweep(p, 'empty');

  // connections through the real form
  await p.evaluate(() => go('connections')); await p.click('[data-act="connNew"]');
  await p.selectOption('#cn-type', 'local'); await p.fill('#cn-id', 'local_lake'); await p.fill('#cn-endpoint', path.join(work, 'lake'));
  await p.click('#drawer-body form button'); await p.waitForFunction(() => S.connections.length === 1);
  await p.click('[data-act="connNew"]'); await p.selectOption('#cn-type', 'folder'); await p.fill('#cn-id', 'landing'); await p.fill('#cn-endpoint', path.join(work, 'landing'));
  await p.click('#drawer-body form button'); await p.waitForFunction(() => S.connections.length === 2);
  check('two connections added through the form and tested healthy', await p.evaluate(() => S.connections.every(c => c.status === 'healthy')), await toastText(p));

  // builder through the real controls
  await p.evaluate(() => go('builder'));
  await p.fill('#b-name', 'vendor_returns'); await p.fill('#b-path', 'vendor_returns');
  await p.click('[data-act="bBrowse"]'); await p.waitForFunction(() => /2026-10-01\.csv/.test(document.getElementById('drawer-body').textContent));
  check('the builder lists the real files in the folder', true);
  await p.evaluate(() => ACT.closeAll());
  await p.click('[data-act="bDetect"]'); await p.waitForFunction(() => /refund_amount:DECIMAL/.test(S.builder.columns));
  check('columns and types are read from the real file', await p.evaluate(() => /return_id:STRING:key/.test(S.builder.columns) && /return_date:DATE/.test(S.builder.columns)), await p.evaluate(() => S.builder.columns));
  await p.waitForFunction(() => S.ui.plan && S.ui.plan.data, null, { timeout: 8000 });
  check('the code pane shows the SQL that will run', (await p.textContent('#codepane')).includes('vendor_returns_raw'));
  for (const i of [1, 2, 3]) await p.click(`.steps button[data-i="${i}"]`);
  await p.fill('#b-gold-name', 'agg_returns_daily'); await p.fill('#b-gold-dateCol', 'return_date'); await p.fill('#b-gold-dims', 'reason, channel');
  await p.fill('#b-gold-measures', 'COUNT(*) AS returns\nSUM(refund_amount) AS refund_total');
  await p.click('.steps button[data-i="5"]'); await p.click('[data-act="bDeploy"][data-run="1"]');
  await p.waitForFunction(() => S.pipelines.length === 1 && S.view === 'orchestration', null, { timeout: 15000 });
  let status = await waitRun(p, 'vendor_returns_medallion');
  check('first run stops on the missing key, as the default check demands', status === 'failed', status);
  check('the failure reason is shown on the page', (await p.textContent('#view')).includes('not null'));
  // controls: change behaviour, then retry
  await p.evaluate(() => go('controls'));
  const nn = await p.evaluate(() => S.rules.find(r => r.type === 'not_null').id);
  await p.selectOption(`#rule-sev-${nn}`, 'quarantine'); await p.fill('#ctl-breaker', '50'); await p.dispatchEvent('#ctl-breaker', 'change'); await sleep(600);
  check('the forecast updates with the controls', await p.evaluate(() => outcome(predict(S.pipelines[0], 'null_keys'))[1]) === 'Succeeds with warnings');
  await p.evaluate(() => { go('orchestration'); ACT.retryRun({ id: S.runs[0].id }); }); await sleep(400);
  status = await waitRun(p, 'vendor_returns_medallion');
  const run = await p.evaluate(() => S.runs[0]);
  check('retry from the failed task succeeds with the new controls', status === 'success' && run.rows.silver === 8 && run.quarantined === 3, JSON.stringify(run.rows));
  await p.click(`tr[data-act="openRun"]`); check('the run drawer shows the real log', (await p.textContent('#drawer-body')).includes('Cast and validated 12 rows'));
  await p.evaluate(() => ACT.closeAll());
  await p.screenshot({ path: path.join(ROOT, 'tests', 'shot-orchestration.png') });

  // layers, quality, mappings
  await p.evaluate(() => { go('silver'); ACT.layerTab({ tab: 'preview' }); });
  await p.waitForFunction(() => /R1001/.test(document.getElementById('view').textContent), null, { timeout: 8000 });
  check('Silver preview shows real rows with hashed emails', !(await p.textContent('#view')).includes('@example.com'));
  await p.screenshot({ path: path.join(ROOT, 'tests', 'shot-silver.png') });
  await p.evaluate(() => { S.ui.layerTab = 'schema'; go('quality'); });
  await p.click('[data-act="qView"]'); await p.waitForFunction(() => /not null|castable/.test(document.getElementById('drawer-body').textContent));
  check('quarantined rows can be inspected with their reason', (await p.textContent('#drawer-body')).includes('12,90'));
  await p.evaluate(() => ACT.closeAll()); await p.click('[data-act="dqRun"]'); await sleep(1200);
  check('checks can be run on demand', /checks evaluated/.test(await toastText(p)), await toastText(p));
  await p.evaluate(() => go('mappings'));
  await p.click('[data-act="mapAdd"]'); const n = await p.evaluate(() => S.ui.mapDraft.rows.length - 1);
  await p.fill(`#map-${n}-src`, 'channel'); await p.dispatchEvent(`#map-${n}-src`, 'change'); await sleep(80);
  await p.fill(`#map-${n}-tgt`, 'channel_upper'); await p.dispatchEvent(`#map-${n}-tgt`, 'change'); await sleep(80);
  await p.fill(`#map-${n}-tx`, 'upper'); await p.dispatchEvent(`#map-${n}-tx`, 'change'); await sleep(80);
  await p.click('[data-act="mapSave"]'); await p.waitForFunction(() => !S.ui.mapDraft && S.mappings['silver.vendor_returns'].rows.some(r => r.tgt === 'channel_upper'), null, { timeout: 8000 });
  check('a mapping edit is saved to the service', true);
  await p.evaluate(() => { go('orchestration'); ACT.trigger({ id: 'vendor_returns_medallion', full: '1' }); }); await sleep(400);
  status = await waitRun(p, 'vendor_returns_medallion');
  await p.evaluate(() => { S.cache = {}; go('silver'); ACT.layerTab({ tab: 'preview' }); });
  await p.waitForFunction(() => /channel_upper/.test(document.getElementById('view').textContent), null, { timeout: 8000 });
  check('the new column appears in Silver after a full reload', status === 'success' && /WEB|APP|STORE/.test(await p.textContent('#view')));
  await p.evaluate(() => { S.ui.layerTab = 'schema'; });

  // platform and governance
  await p.evaluate(() => go('mcp')); await p.waitForFunction(() => S.cache.tools && S.cache.tools.data);
  await p.click('[data-act="toolTry"][data-tool="run_sql"]'); await p.fill('#tool-args', '{"sql": "SELECT COUNT(*) AS n FROM silver.vendor_returns"}'); await p.click('#drawer-body form button');
  await p.waitForFunction(() => /"n": 8/.test(document.getElementById('tool-result').textContent));
  check('a tool call from the MCP page returns real data', true);
  await p.evaluate(() => ACT.closeAll());
  const r = await (await fetch(B + '/mcp', { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer e2e-token' }, body: JSON.stringify({ jsonrpc: '2.0', id: 9, method: 'tools/call', params: { name: 'pause_pipeline', arguments: { pipeline: 'vendor_returns_medallion' } } }) })).json();
  await p.evaluate(() => { LAST = ''; return sync(true); }); await p.waitForSelector('[data-act="approval"][data-ok="1"]');
  check('a change requested over MCP shows up for approval', JSON.parse(r.result.content[0].text).status === 'waiting_for_approval');
  await p.click('[data-act="approval"][data-ok="1"]'); await p.waitForFunction(() => S.pipelines[0].status === 'paused');
  check('approving it applies the change', true);
  await p.evaluate(() => ACT.togglePipe({ id: 'vendor_returns_medallion' })); await sleep(500);
  await p.evaluate(() => go('compute')); await p.waitForFunction(() => /In-process SQLite/.test(document.getElementById('view').textContent));
  check('compute is read from the engine', true);
  await p.evaluate(() => { go('governance'); ACT.govTab({ tab: 'pii' }); });
  await p.fill('#erase-col', 'return_id'); await p.fill('#erase-val', 'R1002'); await p.click('form[data-form="erase"] button');
  await p.waitForSelector('[data-act="eraseConfirm"]'); await p.click('[data-act="eraseConfirm"]'); await p.waitForFunction(() => S.ui.erase.done);
  check('an erasure request finds and deletes the rows', true);
  for (const t of ['access', 'secrets', 'audit']) { await p.evaluate(t => ACT.govTab({ tab: t }), t); await sleep(150); }
  await p.click('[data-act="govTab"][data-tab="access"]');
  await p.evaluate(() => go('agent')); check('the agent page explains how to switch it on', (await p.textContent('#view')).includes('ANTHROPIC_API_KEY'));
  await p.evaluate(() => go('cicd'));
  const [dl] = await Promise.all([p.waitForEvent('download', { timeout: 15000 }), p.click('[data-act="exportZip"]')]);
  check('code export downloads a zip', (await dl.suggestedFilename()).endsWith('.zip'));
  await p.evaluate(() => { go('lineage'); ACT.lineageSel({ id: 'silver.vendor_returns' }); });
  await p.evaluate(() => { go('agent'); go('overview'); }); await sleep(300);
  await p.screenshot({ path: path.join(ROOT, 'tests', 'shot-overview.png') });
  await p.evaluate(() => go('controls')); await p.screenshot({ path: path.join(ROOT, 'tests', 'shot-controls.png') });
  await sweep(p, 'filled');
  const p2 = await open(400, 820, 'dark', 'phone');
  await p2.waitForSelector('#token-form'); await p2.fill('#tk', 'e2e-token'); await p2.click('#token-form button'); await p2.waitForFunction(() => S.ready);
  await sweep(p2, 'phone'); await p2.evaluate(() => go('orchestration')); await p2.screenshot({ path: path.join(ROOT, 'tests', 'shot-phone.png') });
  // delete
  await p.evaluate(() => go('pipelines')); await p.click('[data-act="delPipe"]'); await p.check('#del-drop'); await p.click('#drawer-body form button');
  await p.waitForFunction(() => S.pipelines.length === 0, null, { timeout: 10000 });
  check('a pipeline can be deleted together with its tables', await p.evaluate(() => S.tables.length === 0));
  await b.close();
  if (errs.length) console.log('BROWSER ERRORS:\n' + errs.join('\n'));
  check('no errors in the browser console, no sideways scrolling', errs.length === 0);
})().catch(e => { console.error('SCRIPT FAIL', e); checks.push(['script', false]); }).finally(() => {
  srv.kill(); fs.rmSync(work, { recursive: true, force: true });
  const failed = checks.filter(c => !c[1]); console.log(`\n${checks.length - failed.length} of ${checks.length} checks passed`); process.exit(failed.length ? 1 : 0);
});
