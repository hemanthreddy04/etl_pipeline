/* ---------- Operate: overview, pipelines, orchestration, monitoring ---------- */
const scopedPipes=()=>S.pipelines.filter(p=>inScope(p.platform));
const scopedTables=()=>S.tables.filter(t=>inScope(t.platform));
function dqScore(tabs){const ids=new Set(tabs.map(t=>t.id)),rs=S.rules.filter(r=>ids.has(r.table)&&r.on!==false&&r.checked);return rs.length?rs.reduce((a,r)=>a+r.pass,0)/rs.length:100}
function nextRunText(p){if(p.status==='paused')return 'Paused';const n=nextRuns(p.cron,1)[0];return n?ago(n):'Not scheduled'}
function runRow(r){return `<tr class="click" tabindex="0" data-act="openRun" data-id="${r.id}"><td class="mono">${r.id}</td><td>${st(r.status)}</td><td>${esc(r.trigger)}${r.note?`<div class="muted small">${esc(r.note)}</div>`:''}</td><td class="num">${stamp(r.start)}</td><td class="r num">${r.status==='running'?'':dur(r.dur)}</td><td class="r num">${num(r.rows.bronze)}</td><td class="r num">${num(r.rows.silver)}</td><td class="r num">${num(r.quarantined)}</td><td class="r">${r.status==='failed'?`<button class="btn sm" data-act="retryRun" data-id="${r.id}">Retry from failed task</button>`:''}</td></tr>`}

VIEWS.overview={nav:'Overview',render(){
  const pipes=scopedPipes(),tabs=scopedTables(),pid=new Set(pipes.map(p=>p.id)),since=Date.now()-DAY;
  const day=S.runs.filter(r=>r.start>=since&&pid.has(r.pid)&&r.status!=='running'),ok=day.filter(r=>r.status==='success').length;
  const running=S.runs.filter(r=>r.status==='running'&&pid.has(r.pid)).length,alerts=S.alerts.filter(a=>!a.ack),waiting=S.approvals.filter(a=>a.status==='pending');
  const L=l=>{const t=tabs.filter(x=>x.layer===l&&x.built);return {n:tabs.filter(x=>x.layer===l).length,rows:t.reduce((a,x)=>a+x.rows,0),fresh:t.length?Math.max(...t.map(x=>x.fresh)):0}};
  const b=L('bronze'),s=L('silver'),g=L('gold'),checked=S.rules.some(r=>r.checked&&tabs.some(t=>t.id===r.table));
  const step=(view,title,layer,n,sub)=>`<button class="flow-step" data-act="go" data-view="${view}"><span class="t">${layer?disc(layer):''}${title}</span><span class="n">${n}</span><span class="s">${sub}</span></button>`;
  const lay=(x,word)=>x.n?`${x.n} table${x.n>1?'s':''}, ${word}${x.fresh?'. Written '+ago(x.fresh):'. Not built yet'}`:'No tables yet';
  const noWarehouse=!engines().length;
  return head('Overview',`Your lakehouse in <b>${esc(S.env)}</b>. Raw data lands in Bronze, is cleaned in Silver and is shaped for the business in Gold.`,
    `<button class="btn" data-act="go" data-view="agent">Ask the agent</button><button class="btn primary" data-act="go" data-view="builder">New pipeline</button>`)+
  `<div class="banner info"><div><b>This copy runs in your browser.</b> It is the same site and the same generated SQL as the service in your repository, on a built-in SQLite engine. BigQuery and Databricks need the deployed service.</div><button class="btn sm" data-act="liveAbout">What works here</button></div>`+
  (noWarehouse?`<div class="banner info"><div><b>Start by connecting a warehouse.</b> Add a local SQLite lake, then a landing area for your files, then create a pipeline.</div><button class="btn primary sm" data-act="go" data-view="connections">Add a connection</button></div>`
    :S.pipelines.length?'':`<div class="banner info"><div><b>No pipelines yet.</b> A local engine and a landing area with one sample file are ready. Describe a dataset in the builder and it becomes a Bronze, Silver and Gold pipeline.</div><button class="btn primary sm" data-act="go" data-view="builder">Open the pipeline builder</button></div>`)+
  (waiting.length?`<div class="banner"><div><b>${waiting.length} change${waiting.length>1?'s are':' is'} waiting for your approval.</b> An agent or MCP client asked to ${esc(waiting[0].tool.replace(/_/g,' '))}.</div><button class="btn sm primary" data-act="go" data-view="mcp">Review</button></div>`:'')+
  `<div class="kpis">
    <div class="kpi"><div class="l">Pipelines</div><div class="v">${pipes.filter(p=>p.status==='active').length}<span class="muted small"> of ${pipes.length} active</span></div><div class="d">${running?running+' running now':'None running now'}</div></div>
    <div class="kpi"><div class="l">Runs, last 24 h</div><div class="v">${day.length}</div><div class="d">${day.length?(ok/day.length*100).toFixed(1)+'% succeeded':'No runs yet'}</div></div>
    <div class="kpi"><div class="l">Rows landed, last 24 h</div><div class="v">${fmt(day.reduce((a,r)=>a+r.rows.bronze,0))}</div><div class="d">${fmt(day.reduce((a,r)=>a+(r.quarantined||0),0))} quarantined</div></div>
    <div class="kpi"><div class="l">Quality score</div><div class="v">${checked?dqScore(tabs).toFixed(2)+'%':'None'}</div><div class="d">${S.rules.filter(r=>tabs.some(t=>t.id===r.table)).length} checks</div></div>
    <div class="kpi"><div class="l">Open alerts</div><div class="v">${alerts.length}</div><div class="d">${alerts.filter(a=>a.sev==='bad').length} critical</div></div>
    <div class="kpi"><div class="l">Connections</div><div class="v">${S.connections.filter(c=>c.status==='healthy').length}<span class="muted small"> of ${S.connections.length} healthy</span></div><div class="d">${engines().length} warehouse${engines().length===1?'':'s'}</div></div>
  </div>
  <div class="panel"><header><h2>How data moves</h2><span class="muted small">Select a stage to open it</span></header><div class="body flow">
    ${step('connections','Sources','',new Set(pipes.map(p=>p.source+'/'+p.path)).size,'files and tables')}<span class="flow-arrow" aria-hidden="true">&rarr;</span>
    ${step('bronze','Bronze','bronze',fmt(b.rows),lay(b,'raw'))}<span class="flow-arrow" aria-hidden="true">&rarr;</span>
    ${step('silver','Silver','silver',fmt(s.rows),lay(s,'clean'))}<span class="flow-arrow" aria-hidden="true">&rarr;</span>
    ${step('gold','Gold','gold',fmt(g.rows),lay(g,'business ready'))}
  </div></div>
  <div class="cols wide-left">
    <div class="panel"><header><h2>Pipelines</h2><button class="btn sm" data-act="go" data-view="pipelines">All pipelines</button></header><div class="scroll-x"><table class="tbl"><thead><tr><th>Pipeline</th><th>Engine</th><th>Last run</th><th>When</th><th>Next run</th><th></th></tr></thead><tbody>
      ${pipes.map(p=>{const r=lastRun(p.id);return `<tr class="click" tabindex="0" data-act="openPipe" data-id="${p.id}"><td><b>${esc(p.name)}</b><div class="muted small">${esc(p.mode)}</div></td><td>${plat(p.platform)}</td><td>${r?st(r.status):st('idle')}</td><td class="num">${r?ago(r.start):''}</td><td class="num">${nextRunText(p)}</td><td class="r"><button class="btn sm" data-act="trigger" data-id="${p.id}">Run now</button></td></tr>`}).join('')||'<tr><td colspan="6" class="empty">No pipelines yet.</td></tr>'}
    </tbody></table></div></div>
    <div class="panel"><header><h2>Alerts</h2><span class="muted small">${alerts.length} open</span></header><div class="list">
      ${S.alerts.slice(0,6).map(a=>`<div><span>${st(a.ack?'ack':a.sev==='bad'?'fail':'warn',a.ack?'Acknowledged':a.sev==='bad'?'Critical':'Warning')}</span><div class="grow"><b>${esc(a.title)}</b><span class="muted small">${esc(a.detail)} ${ago(a.ts)}.</span><div style="display:flex;gap:6px;margin-top:6px"><button class="btn sm" data-act="openAlert" data-id="${a.id}">Open</button>${a.ack?'':`<button class="btn sm" data-act="ackAlert" data-id="${a.id}">Acknowledge</button>`}</div></div></div>`).join('')||'<div class="empty">Nothing needs attention.</div>'}
    </div></div>
  </div>
  <div class="panel"><header><h2>Recent activity</h2><button class="btn sm" data-act="openAudit">Full audit log</button></header><div class="scroll-x"><table class="tbl"><tbody>
    ${S.audit.slice(0,6).map(a=>`<tr><td class="num muted">${ago(a.ts)}</td><td>${esc(a.who)}</td><td><b>${esc(a.action)}</b></td><td>${esc(a.target)}</td><td class="muted">${esc(a.detail)}</td></tr>`).join('')||'<tr><td class="empty">Nothing has happened yet. Every change is recorded here.</td></tr>'}
  </tbody></table></div></div>`;
}};
ACT.trigger=d=>act(()=>api('POST',`/api/pipelines/${d.id}/run`,{full:d.full==='1'}),r=>`${d.id} started (${r.id})`);
ACT.retryRun=d=>act(()=>api('POST',`/api/runs/${d.id}/retry`,{}),()=>`Retrying ${d.id} from the task that failed`).then(r=>{if(r&&!$('#drawer').hidden&&$('#drawer-title').textContent===d.id){S.ui.drawerRun=d.id;ACT.openRun({id:d.id,keep:'1'})}});
ACT.openPipe=d=>{S.ui.orchPid=d.id;go('orchestration')};
ACT.ackAlert=d=>act(()=>api('POST',`/api/alerts/${d.id}/ack`,{}));
ACT.openAlert=d=>{const a=S.alerts.find(x=>x.id===d.id);if(!a)return;if(a.pid)S.ui.orchPid=a.pid;go(a.view)};
ACT.openAudit=()=>{S.ui.govTab='audit';go('governance')};

VIEWS.pipelines={nav:'Pipelines',render(){
  const pipes=scopedPipes();
  return head('Pipelines','Each pipeline carries one dataset through Bronze, Silver and Gold. Open one to see its task graph, schedule and runs.',`<button class="btn primary" data-act="go" data-view="builder">New pipeline</button>`)+
  `<div class="panel"><div class="scroll-x"><table class="tbl"><thead><tr><th>Pipeline</th><th>Engine</th><th>Source</th><th>Schedule</th><th>Last run</th><th class="r">Typical duration</th><th>State</th><th></th></tr></thead><tbody>
  ${pipes.map(p=>{const r=lastRun(p.id),ok=S.runs.filter(x=>x.pid===p.id&&x.status==='success'),avg=ok.length?ok.reduce((a,x)=>a+x.dur,0)/ok.length:0;return `<tr class="click" tabindex="0" data-act="openPipe" data-id="${p.id}"><td><b>${esc(p.name)}</b><div class="muted small">${esc(p.mode)}</div></td><td>${plat(p.platform)}<div class="muted small">${esc(p.engine)}</div></td><td class="mono small">${esc(p.source)}<br>${esc(p.path)}</td><td><span class="mono">${esc(p.cron)}</span><div class="muted small">${cronText(p.cron)}</div></td><td>${r?st(r.status)+`<div class="muted small num">${ago(r.start)}</div>`:st('idle')}</td><td class="r num">${avg?dur(avg):''}</td><td>${st(p.status)}</td>
    <td class="r" style="white-space:nowrap"><button class="btn sm" data-act="trigger" data-id="${p.id}">Run now</button> <button class="btn sm" data-act="togglePipe" data-id="${p.id}">${p.status==='paused'?'Resume':'Pause'}</button> <button class="btn sm" data-act="delPipe" data-id="${p.id}">Delete</button></td></tr>`}).join('')||'<tr><td colspan="8" class="empty">No pipelines yet. Select New pipeline to create your first one.</td></tr>'}
  </tbody></table></div></div>`;
}};
ACT.togglePipe=d=>act(()=>api('POST',`/api/pipelines/${d.id}/toggle`,{}),p=>`${p.name} ${p.status==='paused'?'paused. Scheduled runs are skipped until you resume it.':'resumed'}`);
ACT.delPipe=d=>{const p=pipe(d.id);openDrawer('Delete '+p.name,`<p>This removes the pipeline, its mapping, its checks and its run history from the control plane.</p>
  <form data-form="delPipe"><input type="hidden" name="id" value="${p.id}"><label class="check"><input type="checkbox" id="del-drop" name="drop"> Also drop its tables in the warehouse: <span class="mono small">${p.writes.map(esc).join(', ')}</span></label>
  <p class="muted small">Leave this unticked to keep the data. Dropping tables cannot be undone.</p><div><button class="btn danger">Delete pipeline</button></div></form>`)};
FORMS.delPipe=f=>{closeDrawer();act(()=>api('DELETE',`/api/pipelines/${f.id}`,{dropTables:!!f.drop}),()=>`${f.id} deleted${f.drop?' and its tables dropped':'. Its tables were kept'}`)};

VIEWS.orchestration={nav:'Orchestration',render(){
  const pipes=scopedPipes();let p=pipe(S.ui.orchPid);if(!p||!inScope(p.platform)){p=pipes[0];S.ui.orchPid=p?p.id:null}
  if(!p)return head('Orchestration','No pipelines yet.',`<button class="btn primary" data-act="go" data-view="builder">New pipeline</button>`);
  const runs=S.runs.filter(r=>r.pid===p.id),cur=runs[0],nx=nextRuns(p.cron,3),c=p.ctl,ch=channels(c);
  return head('Orchestration','The task graph, schedule and run history of one pipeline. A task runs when everything before it has succeeded.',
    `<button class="btn" data-act="openSql" data-id="${p.id}">View SQL</button><button class="btn" data-act="openSchedule" data-id="${p.id}">Edit schedule</button><button class="btn" data-act="go" data-view="controls">Controls</button><button class="btn" data-act="togglePipe" data-id="${p.id}">${p.status==='paused'?'Resume':'Pause'}</button><button class="btn" data-act="trigger" data-id="${p.id}" data-full="1" title="Reload every file or row, not only what is new">Full reload</button><button class="btn primary" data-act="trigger" data-id="${p.id}">Run now</button>`)+
  `<div class="panel"><header><label class="field" style="flex-direction:row;align-items:center;gap:8px"><span>Pipeline</span><select id="orch-pipe" data-change="orchPipe">${pipes.map(x=>`<option value="${x.id}" ${x.id===p.id?'selected':''}>${esc(x.name)}</option>`).join('')}</select></label>${plat(p.platform)}${st(p.status)}<span class="muted small" style="margin-left:auto">${cur?'Showing '+cur.id+', '+ago(cur.start):'No runs yet'}</span></header>
    <div class="body">${dagSvg(p,cur)}<div class="legend" style="margin-top:10px"><span>${disc('bronze')} Bronze task</span><span>${disc('silver')} Silver task</span><span>${disc('gold')} Gold task</span><span>${disc('source')} Checks</span><span>Select a task for its details</span></div>
    ${cur&&cur.status==='failed'?`<div class="banner" style="margin-top:12px"><div><b>${esc(cur.id)} failed at ${esc(cur.failedTask||'')}.</b> ${esc(cap(cur.failMsg||''))}</div><button class="btn sm primary" data-act="retryRun" data-id="${cur.id}">Retry from failed task</button></div>`:''}</div></div>
  <div class="cols wide-left">
    <div class="panel"><header><h2>Runs</h2><span class="muted small">${runs.length} recorded</span></header><div class="scroll-x"><table class="tbl"><thead><tr><th>Run</th><th>State</th><th>Trigger</th><th>Started</th><th class="r">Duration</th><th class="r">Landed</th><th class="r">Merged</th><th class="r">Quarantined</th><th></th></tr></thead><tbody>${runs.slice(0,15).map(r=>runRow(r)).join('')||'<tr><td colspan="9" class="empty">This pipeline has not run yet. Select Run now.</td></tr>'}</tbody></table></div></div>
    <div class="panel"><header><h2>Schedule and policy</h2></header><div class="body"><dl class="kv">
      <dt>Engine</dt><dd>${esc(p.engine)} through <span class="mono">${esc(p.target)}</span></dd><dt>Source</dt><dd class="mono">${esc(p.source)} / ${esc(p.path)}</dd>
      <dt>Load</dt><dd>${esc(p.mode)}${p.kind==='files'?', '+esc(p.format.toUpperCase())+' files':''}</dd><dt>Schedule</dt><dd><span class="mono">${esc(p.cron)}</span> · ${cronText(p.cron)}</dd>
      <dt>Next runs</dt><dd class="num">${p.status==='paused'?'Paused':nx.length?nx.map(stamp).join('<br>'):'None'}</dd>
      <dt>Retries</dt><dd>${p.retries} per task for warehouse and network errors. A failed check is not retried.</dd>
      <dt>On failure</dt><dd>Later tasks are held${c.alertFail&&ch.length?', alert by '+ch.join(', '):', no alert (switched off)'}</dd>
      <dt>Checks</dt><dd>${pipeRules(p).filter(r=>r.on!==false).length} of ${pipeRules(p).length} switched on. <button class="linkish" data-act="go" data-view="controls">Change controls</button></dd>
      <dt>Files loaded</dt><dd class="num">${p.kind==='files'?Object.keys(p.loaded||{}).length:'Not a file source'}</dd><dt>Owner</dt><dd>${esc(p.owner)}</dd>
    </dl></div></div>
  </div>`;
}};
CHG.orchPipe=v=>{S.ui.orchPid=v;render()};
ACT.openSql=d=>{openDrawer('SQL that runs for '+d.id,'<div class="empty">Loading from the service</div>');api('POST','/api/plan',{pipeline:d.id}).then(pl=>{$('#drawer-body').innerHTML=`<p class="muted">These are the statements a run executes on the warehouse, built from the mapping, the checks and the controls as they are now.</p>${codeBlock('Bronze',pl.bronze)}${codeBlock('Silver',pl.silver)}${codeBlock('Gold',pl.gold)}`},e=>{$('#drawer-body').innerHTML=`<div class="banner">${esc(e.message)}</div>`})};
ACT.openSchedule=d=>{const p=pipe(d.id);openDrawer('Edit schedule',`<form data-form="saveSchedule"><input type="hidden" name="id" value="${p.id}">
  <label class="field"><span>Cron expression (UTC)</span><input type="text" id="sch-cron" name="cron" value="${esc(p.cron)}"><small>Five fields: minute, hour, day of month, month, day of week. Examples: <code>0 * * * *</code> hourly, <code>*/15 * * * *</code> every 15 minutes, <code>0 2 * * *</code> daily at 02:00.</small></label>
  <label class="field"><span>Retries per task</span><input type="number" id="sch-retries" name="retries" min="0" max="5" value="${p.retries}"></label>
  <label class="field"><span>Freshness target for Gold, in minutes</span><input type="number" id="sch-sla" name="sla" min="1" value="${p.sla}"></label>
  <div><button class="btn primary">Save schedule</button></div></form>`)};
FORMS.saveSchedule=f=>act(()=>api('PUT',`/api/pipelines/${f.id}/schedule`,{cron:f.cron,retries:+f.retries,sla:+f.sla}),'Schedule saved').then(r=>{if(r)closeDrawer()});
ACT.openRun=d=>{const r=S.runs.find(x=>x.id===d.id);if(!r)return;const p=pipe(r.pid),ts=runTasks(r);S.ui.drawerRun=r.status==='running'?r.id:null;
  const lines=S.logs.filter(l=>l.runId===r.id).slice().reverse();
  const html=`<dl class="kv"><dt>Pipeline</dt><dd>${esc(r.pid)}</dd><dt>State</dt><dd>${st(r.status)}</dd><dt>Trigger</dt><dd>${esc(r.trigger)}${r.note?', '+esc(r.note):''}</dd><dt>Started</dt><dd class="num">${stamp(r.start)}</dd><dt>Duration</dt><dd class="num">${r.status==='running'?'In progress':dur(r.dur)}</dd>
    <dt>Rows</dt><dd class="num">${num(r.rows.bronze)} landed in Bronze, ${num(r.rows.silver)} merged into Silver, ${num(r.rows.gold)} in Gold</dd><dt>Quarantined</dt><dd class="num">${num(r.quarantined)}</dd></dl>
  ${r.status==='failed'?`<div class="banner"><div>${esc(cap(r.failMsg||'The run failed'))}. Fix the data, the mapping or the check, then retry. Tasks that already succeeded are not repeated.</div></div><div><button class="btn primary" data-act="retryRun" data-id="${r.id}">Retry from failed task</button></div>`:''}
  <h3>Task timeline</h3><div class="gantt">${ts.map(x=>`<span>${esc(x.t.label)}</span><div class="lane"><i class="${x.status}" style="left:${x.left.toFixed(1)}%;width:${Math.min(100-x.left,x.width).toFixed(1)}%"></i></div><span class="small">${st(x.status)}</span>`).join('')}</div>
  <h3>Log</h3><div class="logs" tabindex="0">${lines.map(l=>`<div><span class="t">${clock(l.ts)}</span> <span class="${l.level}">${l.level}</span> [${esc(l.task)}] ${esc(l.msg)}</div>`).join('')||'<div>No log lines are kept for this run any more.</div>'}</div>`;
  if(d.keep){$('#drawer-body').innerHTML=html}else openDrawer(r.id,html)};
ACT.openTask=d=>{const p=pipe(d.pid),t=p.tasks.find(x=>x.id===d.task),r=lastRun(p.id),s=r&&r.tasks?r.tasks[t.id]:null;
  const what={bronze:'Lands the source data unchanged as text and adds lineage columns. Loads only what is new, so it is safe to rerun.',silver:'Casts types, runs the row checks, sends bad rows to quarantine, removes duplicates and merges into the clean table.',gold:'Builds the business-ready table that dashboards and models read.',ops:'Runs the checks for the layer that was just written. A failing check set to stop ends the run here.'}[t.layer];
  const lines=S.logs.filter(l=>l.pid===p.id&&l.task===t.id).slice(0,14).reverse();
  openDrawer(t.label,`<p>${what}</p><dl class="kv"><dt>Task</dt><dd class="mono">${esc(t.id)}</dd><dt>Layer</dt><dd>${t.layer==='ops'?'Checks':layerChip(t.layer)}</dd><dt>Runs after</dt><dd>${t.deps.length?t.deps.map(esc).join(', '):'Nothing. It starts the run.'}</dd><dt>Last state</dt><dd>${st(s?s.status:'idle')}${s&&s.tries>1?` after ${s.tries} attempts`:''}</dd></dl>
  <div><button class="btn sm" data-act="openSql" data-id="${p.id}">View the SQL</button></div>
  <h3>Recent log lines</h3><div class="logs">${lines.map(l=>`<div><span class="t">${clock(l.ts)}</span> <span class="${l.level}">${l.level}</span> ${esc(l.msg)}</div>`).join('')||'<div>No log lines for this task yet.</div>'}</div>`)};

function logList(){const u=S.ui,q=u.logText.toLowerCase();const ls=S.logs.filter(l=>(u.logLevel==='all'||l.level===u.logLevel)&&(u.logPid==='all'||l.pid===u.logPid)&&(!q||(l.msg+' '+l.task+' '+l.pid).toLowerCase().includes(q))).slice(0,150);
  return ls.map(l=>`<div><span class="t">${iso(l.ts).slice(5,19).replace('T',' ')}</span> <span class="${l.level}">${l.level.padEnd(5)}</span> ${esc(l.pid)}.${esc(l.task)}  ${esc(l.msg)}</div>`).join('')||'<div>No log lines match these filters.</div>'}
VIEWS.monitoring={nav:'Monitoring',render(){
  const pipes=scopedPipes(),days=S.ui.monRange,d=dailySeries(days),pid=new Set(pipes.map(p=>p.id));
  const since=Date.now()-days*DAY,rs=S.runs.filter(r=>r.start>=since&&pid.has(r.pid)&&r.status!=='running'),failed=rs.filter(r=>r.status==='failed');
  const sla=pipes.map(p=>{const g=tbl(p.writes[2]);const age=g&&g.fresh?(Date.now()-g.fresh)/MIN:null;return {p,g,age,ok:age!=null&&age<=p.sla}});
  const avgs=pipes.map(p=>{const ok=rs.filter(r=>r.pid===p.id&&r.status==='success');return [p.name,ok.length?ok.reduce((a,r)=>a+r.dur,0)/ok.length:0]}).filter(x=>x[1]);
  const quar=pipes.map(p=>[p.name,rs.filter(r=>r.pid===p.id).reduce((a,r)=>a+(r.quarantined||0),0)]).filter(x=>x[1]);
  return head('Monitoring','Volume, reliability and freshness across every pipeline in scope, from the runs this service recorded.',`<div class="seg" role="group" aria-label="Date range">${[7,14].map(n=>`<button aria-pressed="${days===n}" data-act="monRange" data-n="${n}">Last ${n} days</button>`).join('')}</div>`)+
  `<div class="kpis">
    <div class="kpi"><div class="l">Runs</div><div class="v">${num(rs.length)}</div><div class="d">${failed.length} failed</div></div>
    <div class="kpi"><div class="l">Success rate</div><div class="v">${rs.length?((1-failed.length/rs.length)*100).toFixed(1)+'%':'None'}</div><div class="d">${rs.length?'Of finished runs':'No runs yet'}</div></div>
    <div class="kpi"><div class="l">Rows landed</div><div class="v">${fmt(d.b.reduce((a,x)=>a+x,0))}</div><div class="d">Into Bronze</div></div>
    <div class="kpi"><div class="l">Gold on time</div><div class="v">${sla.filter(x=>x.ok).length}<span class="muted small"> of ${sla.length}</span></div><div class="d">Against each freshness target</div></div>
  </div>
  <div class="panel"><header><h2>Rows written per day, by layer</h2></header><div class="body">${rs.length?lineChart('ch-rows','Rows written per day into Bronze, Silver and Gold',d.labels,[{name:'Bronze',color:'bronze',values:d.b},{name:'Silver',color:'silver',values:d.s},{name:'Gold',color:'gold',values:d.g}]):'<div class="empty">No runs in this range yet. The chart fills in as pipelines run.</div>'}</div></div>
  <div class="cols split">
    <div class="panel"><header><h2>Average run duration</h2><span class="muted small">Seconds</span></header><div class="body">${avgs.length?hbars(avgs,'',v=>v.toFixed(1)):'<div class="empty">No successful runs yet.</div>'}</div></div>
    <div class="panel"><header><h2>Rows quarantined</h2><span class="muted small">In this range</span></header><div class="body">${quar.length?hbars(quar,'',num):'<div class="empty">Nothing was quarantined.</div>'}</div></div>
  </div>
  <div class="cols split">
    <div class="panel"><header><h2>Freshness</h2></header><div class="scroll-x"><table class="tbl"><thead><tr><th>Pipeline</th><th>Gold table</th><th class="r">Age</th><th class="r">Target</th><th>State</th></tr></thead><tbody>${sla.map(x=>`<tr><td>${esc(x.p.name)}</td><td class="mono">${x.g?esc(x.g.id):''}</td><td class="r num">${x.age==null?'':dur(x.age*60)}</td><td class="r num">${dur(x.p.sla*60)}</td><td>${x.age==null?st('idle','Not built yet'):st(x.ok?'pass':'warn',x.ok?'On time':'Late')}</td></tr>`).join('')||'<tr><td colspan="5" class="empty">No Gold tables yet.</td></tr>'}</tbody></table></div></div>
    <div class="panel"><header><h2>Failed runs</h2></header><div class="scroll-x"><table class="tbl"><thead><tr><th>Run</th><th>Pipeline</th><th>When</th><th></th></tr></thead><tbody>${failed.slice(0,6).map(r=>`<tr class="click" tabindex="0" data-act="openRun" data-id="${r.id}"><td class="mono">${r.id}</td><td>${esc(r.pid)}<div class="muted small">${esc((r.failMsg||'').slice(0,90))}</div></td><td class="num">${ago(r.start)}</td><td class="r"><button class="btn sm" data-act="retryRun" data-id="${r.id}">Retry</button></td></tr>`).join('')||'<tr><td colspan="4" class="empty">No failed runs in this range.</td></tr>'}</tbody></table></div></div>
  </div>
  <div class="panel"><header><h2>Logs</h2>
    <select id="log-level" data-change="logLevel" aria-label="Level"><option value="all">All levels</option>${['INFO','WARN','ERROR'].map(l=>`<option ${S.ui.logLevel===l?'selected':''}>${l}</option>`).join('')}</select>
    <select id="log-pid" data-change="logPid" aria-label="Pipeline"><option value="all">All pipelines</option>${pipes.map(p=>`<option ${S.ui.logPid===p.id?'selected':''}>${esc(p.id)}</option>`).join('')}</select>
    <input type="search" id="log-text" data-input="logText" placeholder="Filter text" value="${esc(S.ui.logText)}" aria-label="Filter log text"></header>
    <div class="body"><div class="logs" id="loglist" data-keep="logs" tabindex="0">${logList()}</div></div></div>`;
},after(){attachCharts()}};
ACT.monRange=d=>{S.ui.monRange=+d.n;render()};
CHG.logLevel=v=>{S.ui.logLevel=v;$('#loglist').innerHTML=logList()};
CHG.logPid=v=>{S.ui.logPid=v;$('#loglist').innerHTML=logList()};
INP.logText=v=>{S.ui.logText=v;$('#loglist').innerHTML=logList()};
