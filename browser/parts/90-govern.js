/* ---------- Catalog and lineage ---------- */
function downstreamOf(id){const out=new Set();const walk=x=>{S.tables.filter(t=>t.up.includes(x)&&!out.has(t.id)).forEach(t=>{out.add(t.id);walk(t.id)})};walk(id);return [...out]}
VIEWS.lineage={nav:'Catalog and lineage',render(){
  const tabs=scopedTables(),sel=S.ui.lineageSel,t=tbl(sel),src=S.sources.find(s=>s.id===sel);
  let detail='<div class="empty">Select any box in the graph to trace what feeds it and what depends on it.</div>';
  if(t){const down=downstreamOf(t.id),m=S.mappings[t.id],w=pipe(t.pid);
    detail=`<div class="body" style="display:flex;flex-direction:column;gap:12px"><dl class="kv"><dt>Table</dt><dd class="mono">${esc(fullName(t))}</dd><dt>Layer</dt><dd>${layerChip(t.layer)} ${plat(t.platform)}</dd><dt>Reads from</dt><dd class="mono">${t.up.map(u=>esc(u.startsWith('src:')&&w?w.source+' / '+w.path:u)).join('<br>')}</dd><dt>Written by</dt><dd>${w?`<button class="linkish" data-act="openPipe" data-id="${w.id}">${esc(w.name)}</button>`:'No pipeline'}</dd>
      <dt>Impact of a change</dt><dd>${down.length?`${down.length} table${down.length>1?'s':''} downstream: <span class="mono">${down.map(esc).join(', ')}</span>`:'Nothing downstream'}</dd><dt>Rows</dt><dd class="num">${t.built?num(t.rows):'Not built yet'}</dd></dl>
      <div><button class="btn sm" data-act="layerSel" data-layer="${t.layer}" data-id="${t.id}" data-go="1">Open in ${cap(t.layer)}</button></div></div>
      ${m?`<div class="scroll-x"><table class="tbl"><thead><tr><th>Source column</th><th>Transformation</th><th>Target column</th></tr></thead><tbody>${m.rows.map(r=>`<tr><td class="mono">${esc(r.src)}</td><td class="mono muted">${esc(r.tx)}${r.pii?', hashed':''}</td><td class="mono">${esc(r.tgt)} <span class="muted">${esc(r.ttype)}</span></td></tr>`).join('')}</tbody></table></div>`:''}`}
  else if(src){const c=conn(src.conn);detail=`<div class="body"><dl class="kv"><dt>Source</dt><dd class="mono">${esc(src.name)}</dd><dt>Connection</dt><dd class="mono">${esc(src.conn)}</dd><dt>Address</dt><dd class="mono">${c?esc(c.endpoint):'Unknown'}</dd><dt>Feeds</dt><dd class="mono">${S.tables.filter(x=>x.up.includes(src.id)).map(x=>esc(x.id)).join('<br>')}</dd><dt>Impact of a change</dt><dd>${downstreamOf(src.id).length} tables downstream</dd></dl></div>`}
  return head('Catalog and lineage','Where every table comes from and what depends on it. Use it before changing a column, and when a number on a dashboard looks wrong.',sel?`<button class="btn" data-act="lineageSel" data-id="">Clear selection</button>`:'')+
  `<div class="panel"><header><h2>Lineage</h2><div class="legend">${['bronze','silver','gold'].map(l=>`<span>${disc(l)} ${cap(l)}</span>`).join('')}<span>${disc('source')} Source</span></div></header><div class="body">${tabs.length?lineageGraph():'<div class="empty">No tables yet. Lineage is drawn as soon as a pipeline is created.</div>'}</div></div>
  <div class="cols wide-left">
    <div class="panel"><header><h2>Catalog</h2><span class="muted small">${tabs.length} tables</span></header><div class="scroll-x"><table class="tbl"><thead><tr><th>Table</th><th>Engine</th><th class="r">Columns</th><th class="r">Rows</th><th>Tags</th><th>Last written</th></tr></thead><tbody>
      ${tabs.map(x=>`<tr class="click${x.id===sel?' sel':''}" tabindex="0" data-act="lineageSel" data-id="${x.id}"><td>${disc(x.layer)} <span class="mono">${esc(x.id)}</span></td><td>${plat(x.platform)}</td><td class="r num">${x.cols.length||''}</td><td class="r num">${x.built?num(x.rows):''}</td><td><div class="tags">${x.tags.map(g=>`<span class="chip${g==='pii'?' pii':''}">${esc(g)}</span>`).join('')}</div></td><td class="num">${x.fresh?ago(x.fresh):''}</td></tr>`).join('')||'<tr><td colspan="6" class="empty">The catalog fills in as pipelines are created.</td></tr>'}
    </tbody></table></div></div>
    <div class="panel"><header><h2>${t||src?'Selected':'Details'}</h2></header>${detail}</div>
  </div>`;
}};
ACT.lineageSel=d=>{S.ui.lineageSel=d.id||null;render()};

/* ---------- Governance ---------- */
VIEWS.governance={nav:'Governance',render(){
  const tab=S.ui.govTab,tabs=scopedTables();let body='';
  if(tab==='access'){const g=S.ui.grants;
    body=`<div class="scroll-x"><table class="tbl"><thead><tr><th>Group</th><th>Principal in the warehouse</th>${['bronze','silver','gold'].map(l=>`<th>${cap(l)}</th>`).join('')}</tr></thead><tbody>${S.roles.map((r,i)=>`<tr><td><b>${esc(r.name)}</b></td>
      <td><input type="text" id="acc-who-${i}" class="mono" value="${esc(r.principal||'')}" placeholder="group:data-eng@example.com" data-change="govRole" data-i="${i}" data-f="principal" aria-label="Principal for ${esc(r.name)}"></td>
      ${['bronze','silver','gold'].map(l=>`<td><select id="acc-${i}-${l}" data-change="govRole" data-i="${i}" data-f="${l}" aria-label="${esc(r.name)} on ${l}">${['none','read','write'].map(a=>`<option value="${a}" ${r.acc[l]===a?'selected':''}>${a==='none'?'No access':a==='read'?'Read':'Read and write'}</option>`).join('')}</select></td>`).join('')}</tr>`).join('')}</tbody></table></div>
    <div class="body" style="border-top:1px solid var(--line);display:flex;flex-direction:column;gap:10px"><p class="muted" style="max-width:75ch">Fill in the group or user each row stands for (BigQuery form: <code>group:name@domain</code> or <code>user:name@domain</code>; Databricks: the group name). Rows without a principal are skipped. The matrix becomes GRANT statements on the Bronze, Silver and Gold schemas of each cloud warehouse.</p>
      <div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn" data-act="govGrants">Show the statements</button><button class="btn primary" data-act="govGrants" data-apply="1">Apply to the warehouses</button></div>
      ${g?(g.statements.length?g.statements.map(x=>`<div class="mono small" style="overflow-wrap:anywhere">${x.result?st(x.result==='applied'?'pass':'fail',x.result==='applied'?'Applied':'Failed')+' ':''}${esc(x.sql)}${x.result&&x.result!=='applied'?`<div class="muted">${esc(x.result)}</div>`:''}</div>`).join(''):'<div class="muted">No statements: add a principal to a row, and a BigQuery or Databricks connection.</div>'):''}</div>`}
  if(tab==='pii'){const rows=Object.entries(S.mappings).filter(([id])=>tabs.some(t=>t.id===id)).flatMap(([id,m])=>m.rows.map((r,i)=>[id,r,i]));const e=S.ui.erase;
    body=`<div class="scroll-x"><table class="tbl"><thead><tr><th>Silver table</th><th>Column</th><th>Type</th><th>Hash as personal data</th></tr></thead><tbody>${rows.map(([id,r,i])=>`<tr><td>${disc('silver')} <span class="mono">${esc(id)}</span></td><td class="mono">${esc(r.tgt)}</td><td class="mono muted">${esc(r.ttype)}</td><td>${r.ttype==='STRING'?`<label class="switch"><input type="checkbox" id="pii-${id.replace('.','-')}-${i}" ${r.pii?'checked':''} data-change="govPii" data-table="${id}" data-i="${i}" aria-label="Hash ${esc(r.tgt)}"><i></i></label>`:'<span class="muted small">Text columns only</span>'}</td></tr>`).join('')||'<tr><td colspan="4" class="empty">No Silver tables yet.</td></tr>'}</tbody></table></div>
    <p class="muted small" style="padding:8px 14px">A hashed column is stored as a SHA-256 value in Silver and everything built from it. Bronze keeps the original, so limit who can read Bronze. A change applies from the next run; use Full reload to rewrite rows already in Silver. Hashing is skipped while the pipeline's "Mask personal data" control is off.</p>
    <div class="body" style="border-top:1px solid var(--line)"><h3>Erasure request</h3><p class="muted" style="margin:4px 0 10px;max-width:70ch">Deletes every row for one person from the Silver and Gold tables that carry the column you name. Bronze is not touched, so remove the person at the source too, or a full reload brings them back.</p>
      <form data-form="erase" style="display:flex;gap:8px;flex-wrap:wrap"><input type="text" id="erase-col" name="column" placeholder="Column, for example customer_id" value="${esc(e?e.column:'')}" required aria-label="Column"><input type="text" id="erase-val" name="value" placeholder="Value to erase" value="${esc(e?e.value:'')}" required aria-label="Value"><button class="btn">Find the rows</button></form>
      ${e&&e.tables?`<div style="margin-top:10px">${e.tables.length?`<table class="tbl"><tbody>${e.tables.map(x=>`<tr><td class="mono">${esc(x.table)}</td><td class="r num">${num(x.rows)} row${x.rows===1?'':'s'}</td></tr>`).join('')}</tbody></table>${e.done?'<p style="margin-top:8px">'+st('pass','Deleted')+'</p>':e.tables.some(x=>x.rows)?`<div style="margin-top:8px"><button class="btn danger" data-act="eraseConfirm">Delete these rows</button></div>`:'<p class="muted" style="margin-top:8px">No rows match.</p>'}`:'<p class="muted">No built Silver or Gold table has a column with that name.</p>'}</div>`:''}</div>`}
  if(tab==='secrets'){const c=remote('secrets:'+S.connections.map(x=>x.id+x.secret).join(),()=>api('GET','/api/governance/secrets'));
    body=remoteHtml(c,d=>`<div class="scroll-x"><table class="tbl"><thead><tr><th>Secret name</th><th>Used by</th><th>Found in</th></tr></thead><tbody>${d.secrets.map(s=>`<tr><td class="mono">${esc(s.name)}</td><td class="mono">${esc(s.used)}</td><td>${st(s.found==='missing'?'fail':'pass',s.found==='missing'?'Not found':cap(s.found))}</td></tr>`).join('')||'<tr><td colspan="3" class="empty">No connection uses a secret. BigQuery and Cloud Storage sign in as the service account of this service.</td></tr>'}</tbody></table></div>`)+
      `<p class="muted small" style="padding:10px 14px">Only names appear here. A value is read from an environment variable of that name or from Google Secret Manager at the moment it is needed, and is never stored or shown.</p>`}
  if(tab==='audit')body=`<div class="scroll-x"><table class="tbl"><thead><tr><th>When</th><th>Who</th><th>Action</th><th>Target</th><th>Detail</th></tr></thead><tbody>${S.audit.map(a=>`<tr><td class="num" style="white-space:nowrap">${stamp(a.ts)}</td><td>${esc(a.who)}</td><td><b>${esc(a.action)}</b></td><td class="mono">${esc(a.target)}</td><td class="muted">${esc(a.detail)}</td></tr>`).join('')||'<tr><td colspan="5" class="empty">Nothing has happened yet.</td></tr>'}</tbody></table></div>`;
  return head('Governance','Who can read each layer, how personal data is protected, where secrets live, and a record of every change.')+
  `<div class="panel"><div class="tabs" role="tablist">${[['access','Access'],['pii','Personal data'],['secrets','Secrets'],['audit','Audit log']].map(([k,l])=>`<button role="tab" aria-selected="${tab===k}" data-act="govTab" data-tab="${k}">${l}</button>`).join('')}</div>${body}</div>`;
}};
ACT.govTab=d=>{S.ui.govTab=d.tab;render()};
CHG.govRole=(v,d)=>{const r=S.roles[+d.i];if(d.f==='principal')r.principal=String(v).trim();else r.acc[d.f]=v;S.ui.grants=null;
  api('POST','/api/governance/grants',{roles:S.roles}).then(()=>{LAST=''},e=>toast(e.message,'bad'));if(d.f==='bronze'&&v!=='none'&&r.id!=='data_engineer')toast(`${r.name} would be able to read unmasked source data in Bronze`,'warn')};
ACT.govGrants=d=>{api('POST','/api/governance/grants',{apply:d.apply==='1',roles:S.roles}).then(g=>{S.ui.grants=g;render();if(d.apply==='1')toast(`${g.statements.filter(x=>x.result==='applied').length} of ${g.statements.length} grants applied`,g.statements.some(x=>x.result!=='applied')?'warn':'ok')},e=>toast(e.message,'bad'))};
CHG.govPii=(v,d)=>{const rows=JSON.parse(JSON.stringify(S.mappings[d.table].rows));rows[+d.i].pii=v;act(()=>api('PUT',`/api/mappings/${d.table}`,{rows}),v?'This column is hashed from the next run on':'This column is stored in clear text from the next run on')};
FORMS.erase=f=>{const column=f.column.trim(),value=f.value.trim();api('POST','/api/governance/erase',{column,value}).then(r=>{S.ui.erase={column,value,tables:r.tables,done:false};render()},e=>toast(e.message,'bad'))};
ACT.eraseConfirm=()=>{const e=S.ui.erase;api('POST','/api/governance/erase',{column:e.column,value:e.value,confirm:true}).then(r=>{S.ui.erase={...e,tables:r.tables,done:true};LAST='';sync(true);toast('The rows were deleted and the request is in the audit log')},x=>toast(x.message,'bad'))};

/* ---------- Code export ---------- */
function pipelineFiles(p){const c=exportCfg({...p.cfg,target:p.target,conn:p.source}),tt=(conn(p.target)||{}).type,files=[[`controls/${p.id}.yml`,genControls(p)]];
  if(tt==='adb'||tt==='bq'){const n=names(c);files.push([`pipelines/bronze/bronze_${n.t}.py`,genBronze(c)],[`pipelines/silver/silver_${n.t}.py`,genSilver(c)],genOrch(c),[`quality/${n.t}.yml`,genQuality(c)])}
  return files}
VIEWS.cicd={nav:'Code export',render(){
  const pipes=scopedPipes();
  return head('Code export','Take every pipeline out as files: the SQL each run executes, the controls, and Spark and orchestration code for teams that prefer to run it themselves.',pipes.length?`<button class="btn primary" data-act="exportZip">Show every file</button>`:'')+
  `<div class="banner info"><div>The SQL files are exactly what this service runs. The Spark, Airflow and Databricks Workflows files are generated from the same definitions as a starting point, and this service does not run or test them.</div></div>
  <div class="panel"><div class="scroll-x"><table class="tbl"><thead><tr><th>Pipeline</th><th>Engine</th><th>Files</th><th></th></tr></thead><tbody>${pipes.map(p=>{let fs=[];try{fs=pipelineFiles(p)}catch(e){}return `<tr><td><b>${esc(p.name)}</b></td><td>${plat(p.platform)}</td><td class="mono small">sql/${esc(p.id)}/bronze.sql, silver.sql, gold.sql<br>${fs.map(f=>esc(f[0])).join('<br>')}</td><td class="r"><button class="btn sm" data-act="openSql" data-id="${p.id}">View SQL</button></td></tr>`}).join('')||'<tr><td colspan="4" class="empty">Nothing to export until a pipeline exists.</td></tr>'}</tbody></table></div></div>
  <div class="cols split">
    <div class="panel"><header><h2>Suggested repository layout</h2></header><div class="body scroll-x"><pre class="tree">${esc(REPO_TREE)}</pre></div></div>
    <div class="panel"><header><h2>Keeping it under review</h2></header><div class="body"><p>Commit the export to a repository so that a change to a mapping, a check or a control shows up as a diff someone can review. The <span class="mono">controls/</span> files record what each pipeline does when a check fails.</p><p style="margin-top:8px" class="muted">Every change made on this site is also in the audit log, with who made it and when.</p></div></div>
  </div>`;
}};
const REPO_TREE=`medallion-pipelines/
├── sql/
│   └── <pipeline>/        bronze.sql, silver.sql, gold.sql as the service runs them
├── controls/              what each pipeline does when a check fails
├── quality/               checks per table
├── pipelines/
│   ├── bronze/            Spark version of each Bronze load
│   └── silver/            Spark version of each Silver transform
├── dags/ or workflows/    orchestration definitions
└── README.md`;
ACT.exportZip=async(d,el)=>{el.disabled=true;try{const files=[];
  for(const p of scopedPipes()){const pl=await api('POST','/api/plan',{pipeline:p.id});['bronze','silver','gold'].forEach(k=>files.push({path:`sql/${p.id}/${k}.sql`,content:pl[k]}));try{pipelineFiles(p).forEach(f=>files.push({path:f[0],content:f[1]}))}catch(e){}}
  openDrawer('Exported files',`<p class="muted" style="margin-bottom:10px">${files.length} files. A page inside Claude cannot save files to your device, so copy the ones you need. The deployed service downloads the same files as one zip.</p><div style="display:flex;flex-direction:column;gap:10px">${files.map(f=>codeBlock(f.path,f.content)).join('')}</div>`)}catch(e){toast(e.message,'bad')}el.disabled=false};

/* ---------- Concepts ---------- */
const CONCEPTS=[
 ['Medallion architecture','Architecture','A lakehouse pattern with three layers of rising quality. Bronze keeps raw data, Silver keeps clean and conformed data, Gold keeps business-ready tables.','overview'],
 ['Lakehouse','Architecture','Cheap object storage with a table format on top, giving warehouse features such as transactions, schemas and SQL directly on files.','overview'],
 ['Data lake and data warehouse','Architecture','A lake stores any file cheaply and applies structure when read. A warehouse stores modelled tables and enforces structure when written.','overview'],
 ['ETL and ELT','Architecture','ETL transforms data before loading it. ELT loads raw data first and transforms it inside the platform, which is what the medallion layers do.','builder'],
 ['Batch and streaming','Architecture','Batch processes data in scheduled chunks. Streaming processes records continuously as they arrive, usually in small micro-batches.','pipelines'],
 ['Lambda and Kappa','Architecture','Lambda runs separate batch and streaming paths and merges them. Kappa treats everything as a stream and replays it when logic changes.','pipelines'],
 ['Data mesh','Architecture','Domain teams own and publish their data as products, on a shared platform with shared governance.','lineage'],
 ['Full and incremental load','Ingestion','A full load rereads everything each run. An incremental load reads only what changed, which is cheaper but needs a reliable way to detect change.','builder'],
 ['Change data capture','Ingestion','Reading inserts, updates and deletes from a database log or change table, so the lake can follow the source without rescanning it.','builder'],
 ['Watermark','Ingestion','In batch, the highest value already loaded, such as the latest updated_at. In streaming, how long to wait for late events before closing a window.','builder'],
 ['Auto Loader','Ingestion','A Databricks file source that tracks which files it has already read and loads only new ones, with schema inference and evolution.','builder'],
 ['Checkpoint','Ingestion','Saved progress of a stream, such as offsets and state. After a restart the job resumes exactly where it stopped.','builder'],
 ['Idempotency','Ingestion','Running the same load twice gives the same result as running it once. It is what makes retries and backfills safe.','orchestration'],
 ['Exactly-once processing','Ingestion','Each record affects the result once, even after failures. In practice it comes from replayable sources, checkpoints and idempotent writes.','builder'],
 ['Backfill','Ingestion','Rerunning a pipeline for past dates, to load history or repair a period after a fix.','orchestration'],
 ['Schema evolution','Ingestion','Handling new, removed or retyped source columns without breaking the load. Bronze accepts changes and Silver decides what to do with them.','bronze'],
 ['Schema on read and schema on write','Ingestion','Schema on read stores data as it comes and interprets it when queried, as in Bronze. Schema on write validates before storing, as in Silver.','bronze'],
 ['Late-arriving data','Ingestion','Records that show up after their time period was already processed. Handled with watermarks, merges and by rebuilding recent partitions.','silver'],
 ['Dead-letter queue','Ingestion','A side destination for messages that could not be processed, so one bad record does not block the stream.','quality'],
 ['Parquet','Storage','A columnar file format. It compresses well and lets engines read only the columns and row groups a query needs.','bronze'],
 ['Delta Lake','Storage','A table format that adds a transaction log to Parquet files, giving ACID writes, MERGE, schema enforcement and time travel.','silver'],
 ['Apache Iceberg','Storage','An open table format with snapshots, hidden partitioning and schema evolution, readable by many engines.','silver'],
 ['ACID transactions','Storage','Writes are all-or-nothing and isolated, so readers never see a half-written table.','silver'],
 ['Partitioning','Storage','Splitting a table into folders by a column such as date, so queries that filter on it skip most files.','bronze'],
 ['Clustering and Z-ordering','Storage','Sorting data within files by frequently filtered columns, so the engine can skip files using their min and max statistics.','silver'],
 ['Small file problem','Storage','Many tiny files make reads slow because of listing and open overhead. Compaction merges them into fewer, larger files.','silver'],
 ['Compaction and OPTIMIZE','Storage','Rewriting many small files into fewer large ones. In Delta this is the OPTIMIZE command.','silver'],
 ['Vacuum','Storage','Deleting data files that no table version references any more. It frees storage and shortens how far back time travel can go.','silver'],
 ['Time travel','Storage','Querying or restoring a table as it was at an earlier version or time.','gold'],
 ['Type casting','Transformation','Converting text to proper types. A safe cast returns NULL instead of failing, so bad rows can be isolated instead of crashing the job.','mappings'],
 ['Deduplication','Transformation','Keeping one row per business key, usually the newest, using a window function such as ROW_NUMBER.','silver'],
 ['MERGE and upsert','Transformation','One statement that updates rows that match a key and inserts those that do not.','silver'],
 ['SCD Type 1','Transformation','A slowly changing dimension where an update overwrites the old value. No history is kept.','silver'],
 ['SCD Type 2','Transformation','A slowly changing dimension that keeps history. Each change closes the current row and adds a new one with validity dates.','silver'],
 ['Surrogate key','Transformation','A generated key that stands in for the business key, stable across source systems and across versions of a row.','gold'],
 ['Source to target mapping','Transformation','A table that states, column by column, where each target value comes from and how it is transformed.','mappings'],
 ['Sessionization','Transformation','Grouping a user\'s events into sessions, usually by closing a session after a period of inactivity.','silver'],
 ['Window functions','Transformation','Calculations across related rows, such as ranking, running totals or the previous value, without collapsing them.','silver'],
 ['Shuffle','Transformation','Moving rows between Spark executors so matching keys end up together for joins and aggregations. It is usually the costly part of a job.','compute'],
 ['Data skew','Transformation','A few keys hold most of the rows, so a few tasks do most of the work. Fixed with salting, broadcast joins or adaptive execution.','compute'],
 ['Broadcast join','Transformation','Sending a small table to every executor so a large table can be joined without a shuffle.','compute'],
 ['Fact table','Modelling','Measurements of events, such as sales or page views, at a declared grain, with keys to dimensions.','gold'],
 ['Dimension table','Modelling','Descriptive context for facts, such as customer, product or date.','gold'],
 ['Grain','Modelling','What one row of a table represents. Stating it first prevents double counting.','gold'],
 ['Star schema','Modelling','A fact table in the centre joined directly to dimension tables. Simple for people and fast for engines.','gold'],
 ['Conformed dimension','Modelling','One shared dimension used by many facts, so numbers agree across reports.','gold'],
 ['Data Vault','Modelling','A modelling style of hubs, links and satellites, built for auditability and for adding sources without rework.','gold'],
 ['One big table','Modelling','A wide, denormalised table that joins everything in advance. Convenient for BI tools at the cost of storage and flexibility.','gold'],
 ['Aggregate table','Modelling','A precomputed summary, such as daily totals, so dashboards do not scan detail rows.','gold'],
 ['Data quality dimensions','Quality','Completeness, uniqueness, validity, consistency, timeliness and accuracy. Each check measures one of them.','quality'],
 ['Expectation','Quality','A rule the data must satisfy, such as not null, unique or within a range, checked on every run.','quality'],
 ['Quarantine','Quality','Setting bad rows aside with the reason, so good rows continue and the bad ones can be fixed and replayed.','quality'],
 ['Quality gate','Quality','A point in a pipeline where checks must pass before data moves on or is published.','controls'],
 ['Check severity','Quality','What a failing check does: stop the pipeline, move the bad rows to quarantine, or only warn.','controls'],
 ['Circuit breaker','Quality','A limit that stops a run when too many rows fail, so a mostly empty table is never loaded.','controls'],
 ['Data contract','Quality','An agreement between a producer and its consumers about schema, meaning and quality, enforced by tests.','cicd'],
 ['Reconciliation','Quality','Comparing counts or totals between layers, or with the source, to prove nothing was lost or doubled.','quality'],
 ['Freshness','Quality','How recently a table was updated, compared with how recent it is expected to be.','monitoring'],
 ['Anomaly detection','Quality','Flagging a volume or value that falls outside its usual pattern, such as half the normal rows.','quality'],
 ['DAG','Orchestration','A directed acyclic graph: tasks and their dependencies, with no cycles. It decides what runs after what.','orchestration'],
 ['Scheduling and cron','Orchestration','Starting pipelines on a timetable. A cron expression has five fields: minute, hour, day of month, month and day of week.','orchestration'],
 ['Sensor','Orchestration','A task that waits for something outside the pipeline, such as a file arriving or an upstream table being ready.','orchestration'],
 ['Retries and backoff','Orchestration','Rerunning a failed task automatically, waiting longer each time, to ride out temporary faults.','orchestration'],
 ['Catch-up','Orchestration','Whether the scheduler creates runs for intervals it missed while a pipeline was paused.','orchestration'],
 ['SLA','Orchestration','An agreed deadline or freshness target. Missing it raises an alert even if nothing failed.','monitoring'],
 ['Observability','Operations','Being able to tell what the pipelines are doing from the outside, through metrics, logs, lineage and data checks.','monitoring'],
 ['Lineage','Operations','The path data takes from source to consumer, at table or column level. It powers impact analysis and root-cause tracing.','lineage'],
 ['Autoscaling','Operations','Adding and removing workers as load changes, to finish on time without paying for idle machines.','compute'],
 ['Job cluster and all-purpose cluster','Operations','A job cluster starts for one run and stops afterwards. An all-purpose cluster stays up for interactive work and costs more per hour.','compute'],
 ['Serverless compute','Operations','Compute the platform starts and scales for you, billed only while it works.','compute'],
 ['FinOps','Operations','Managing cloud spend as an engineering concern: tagging, right-sizing, auto-stop and watching cost per pipeline.','monitoring'],
 ['Data catalog','Governance','A searchable inventory of tables, columns, owners and tags. Unity Catalog and Dataplex are examples.','lineage'],
 ['Role-based access control','Governance','Granting permissions to groups by role, not to individuals, with the least privilege each role needs.','governance'],
 ['Personal data masking','Governance','Hiding or transforming personal data, by hashing, redacting or partially showing it, depending on who is reading.','governance'],
 ['Secrets management','Governance','Keeping credentials in a vault and referring to them by name, so they never appear in code, configuration or logs.','governance'],
 ['Managed identity and workload identity','Governance','Letting a job authenticate as itself to cloud services, with no stored key to leak or rotate.','connections'],
 ['Right to erasure','Governance','Deleting a person\'s data on request across every layer, including old file versions.','governance'],
 ['Audit log','Governance','A record of who did what and when, for security reviews and compliance.','governance'],
 ['Retention policy','Governance','How long each table and its history are kept before being deleted.','governance'],
 ['CI/CD','Delivery','Testing every change automatically and deploying it through environments in a repeatable way.','cicd'],
 ['Infrastructure as code','Delivery','Defining storage, catalogs, clusters and permissions in versioned files, with tools such as Terraform or asset bundles.','cicd'],
 ['Environments','Delivery','Separate dev, test and prod copies of the platform, so changes are proven before they reach real consumers.','cicd'],
 ['Unit and data tests','Delivery','Unit tests check transformation logic on tiny inputs. Data tests check real tables after a run.','cicd'],
 ['Model Context Protocol','AI','An open protocol that lets an AI agent discover and call tools on external systems in a uniform, typed way.','mcp'],
 ['Agentic pipelines','AI','Using an AI agent to design, generate and operate pipelines through tools, with a person approving the steps that change things.','agent'],
 ['Human in the loop','AI','Requiring a person to approve an agent\'s actions that write, deploy or delete.','mcp'],
 ['Self-healing pipeline','AI','A pipeline that detects a known failure, applies a safe fix such as quarantine and retry, and escalates the rest.','agent'],
 ['Synthetic data','AI','Generated data that mimics the shape and statistics of real data, for testing without exposing real records.','quality']
];
function conceptList(){const q=S.ui.conceptText.toLowerCase(),cat=S.ui.conceptCat;const ls=CONCEPTS.filter(c=>(cat==='All'||c[1]===cat)&&(!q||(c[0]+' '+c[2]).toLowerCase().includes(q)));
  return ls.map(c=>`<div class="concept"><h3>${esc(c[0])}</h3><p>${esc(c[2])}</p><div class="meta"><span class="chip">${c[1]}</span><button class="linkish" data-act="go" data-view="${c[3]}">See it in ${esc(VIEWS[c[3]].nav)}</button></div></div>`).join('')||'<div class="empty">No concept matches. Try a shorter search.</div>'}
VIEWS.concepts={nav:'Concepts',render(){
  const cats=['All',...new Set(CONCEPTS.map(c=>c[1]))];
  return head('Concepts',`${CONCEPTS.length} data engineering ideas in plain words, each linked to the screen where this control plane puts it to work.`)+
  `<div class="panel"><header><input type="search" id="concept-q" data-input="conceptText" placeholder="Search concepts" value="${esc(S.ui.conceptText)}" aria-label="Search concepts" style="flex:1 1 200px"><div class="seg" role="group" aria-label="Category">${cats.map(c=>`<button aria-pressed="${S.ui.conceptCat===c}" data-act="conceptCat" data-cat="${c}">${c}</button>`).join('')}</div></header><div id="conceptlist">${conceptList()}</div></div>`;
}};
ACT.conceptCat=d=>{S.ui.conceptCat=d.cat;render()};
INP.conceptText=v=>{S.ui.conceptText=v;$('#conceptlist').innerHTML=conceptList()};
