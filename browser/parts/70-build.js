/* ---------- Pipeline builder ---------- */
function defaultBuilder(){const eng=engines().filter(c=>inScope(c.cloud))[0]||engines()[0];
  const b={step:0,tab:'bronze',name:'',target:eng?eng.id:'',conn:'',kind:'files',format:'csv',path:'',pattern:'incremental',watermark:'',columns:'',evolve:'addNewColumns',onBad:'quarantine',scd:'1',maskPii:true,
    gold:{name:'',type:'aggregate',dateCol:'',dims:'',measures:'COUNT(*) AS row_count',sql:'SELECT *\nFROM {silver}'},cron:'0 * * * *',retries:2};
  const fc=builderConns(b)[0];b.conn=fc?fc.id:'';return b}
const STEPS=['Source','Bronze','Silver','Gold','Schedule','Review and deploy'];
function targetType(b){const c=conn(b.target);return c?c.type:''}
function builderConns(b){const want=FILE_FOR[targetType(b)];return S.connections.filter(c=>c.type===want)}
function builderCfg(b){const {step,tab,...cfg}=b;return cfg}
function exportCfg(b){const tt=targetType(b);return {...b,platform:tt==='adb'?'azure':'gcp',bronzeStore:'bq_gcs',orch:tt==='adb'?'dbx':'airflow',gold:{...b.gold,name:b.gold.name||'agg_'+(b.name||'dataset')}}}
const BTABS=[['bronze','Bronze SQL'],['silver','Silver SQL'],['gold','Gold SQL'],['spark','Spark export'],['orch','Orchestration export']];
let planTimer=null;
function planFetch(){const b=S.builder;if(!b)return;const key=JSON.stringify(builderCfg(b));if(S.ui.plan&&S.ui.plan.key===key)return;
  S.ui.plan={key,loading:true,data:S.ui.plan&&S.ui.plan.data};clearTimeout(planTimer);
  planTimer=setTimeout(()=>{api('POST','/api/plan',{cfg:builderCfg(b)}).then(d=>{if(S.ui.plan.key===key)S.ui.plan={key,data:d}},e=>{if(S.ui.plan.key===key)S.ui.plan={key,error:e.message}}).finally(()=>{const el=$('#codepane');if(el&&S.view==='builder')el.innerHTML=builderCode()})},450)}
function builderCode(){const b=S.builder,pl=S.ui.plan||{};for(const k in CODE_STORE)delete CODE_STORE[k];
  const tabs=`<div class="tabs" role="tablist">${BTABS.map(([k,l])=>`<button role="tab" aria-selected="${b.tab===k}" data-act="bTab" data-tab="${k}">${l}</button>`).join('')}</div>`;
  if(b.tab==='spark'||b.tab==='orch'){const tt=targetType(b);
    if(tt!=='adb'&&tt!=='bq')return tabs+'<div class="panel"><div class="empty">The Spark and orchestration export is written for BigQuery and Azure Databricks targets.</div></div>';
    let files;try{const c=exportCfg(b);files=b.tab==='spark'?[[`pipelines/bronze/bronze_${names(c).t}.py`,genBronze(c)],[`pipelines/silver/silver_${names(c).t}.py`,genSilver(c)]]:[genOrch(c)]}catch(e){return tabs+`<div class="banner">${esc(e.message)}</div>`}
    return tabs+`<div class="banner info">An export for teams that run Spark with ${tt==='adb'?'Databricks Workflows':'Dataproc and Cloud Composer'}. It is generated from the same definition, but this service does not run or test it. Runs here use the SQL in the first three tabs.</div>`+files.map(f=>codeBlock(f[0],f[1])).join('')}
  if(pl.error)return tabs+`<div class="banner"><div><b>The SQL cannot be built yet.</b> ${esc(pl.error)}</div></div>`;
  if(!pl.data)return tabs+'<div class="panel"><div class="empty">The SQL appears here once the source and columns are filled in.</div></div>';
  return tabs+codeBlock({bronze:'Bronze: what a run executes',silver:'Silver: what a run executes',gold:'Gold: what a run executes'}[b.tab],pl.data[b.tab])+(pl.loading?'<p class="muted small">Updating</p>':'')}
function fld(label,k,html,hint){return `<label class="field"><span>${label}</span>${html}${hint?`<small>${hint}</small>`:''}</label>`}
const bText=(k,v,ph='')=>`<input type="text" id="b-${k.replace('.','-')}" data-input="bf" data-k="${k}" value="${esc(v)}" placeholder="${esc(ph)}">`;
const bSel=(k,v,opts)=>`<select id="b-${k.replace('.','-')}" data-change="bf" data-k="${k}">${opts.map(([val,l])=>`<option value="${esc(val)}" ${val===v?'selected':''}>${esc(l)}</option>`).join('')}</select>`;
const bArea=(k,v,rows,ph='')=>`<textarea id="b-${k.replace('.','-')}" rows="${rows}" data-input="bf" data-k="${k}" spellcheck="false" placeholder="${esc(ph)}">${esc(v)}</textarea>`;
function builderStep(b){
  const cols=parseCols(b.columns),keys=cols.filter(c=>c.key).map(c=>c.name),tt=targetType(b),eng=conn(b.target),t=(b.name||'dataset').replace(/[^a-z0-9_]/gi,'_').toLowerCase();
  switch(b.step){
  case 0:{const cs=builderConns(b),fmts=tt==='local'?[['csv','CSV'],['json','JSON lines']]:[['csv','CSV'],['json','JSON lines'],['parquet','Parquet'],['avro','Avro']];
    return `<div class="form-grid">
    ${fld('Dataset name','name',bText('name',b.name,'orders'),'Used for the table and task names.')}
    ${fld('Warehouse','target',bSel('target',b.target,engines().map(c=>[c.id,`${c.id} (${ctype(c.type).name})`])),'Where Bronze, Silver and Gold are built.')}
    ${fld('Source type','kind',bSel('kind',b.kind,[['files','Files in storage'],['table','A table already in the warehouse']]))}
    ${b.kind==='files'?fld('File connection','conn',cs.length?bSel('conn',b.conn,cs.map(c=>[c.id,`${c.id} (${c.endpoint})`])):`<span class="muted">No ${esc(ctype(FILE_FOR[tt]||'gcs').name)} connection yet.</span>`,cs.length?'':'Add one under Connections.')
      +fld('File format','format',bSel('format',b.format,fmts))+fld('Folder','path',bText('path',b.path,'orders'),'Inside the connection. Every file of this format under it is loaded.'+folderHints(b))
      :fld('Source table','path',bText('path',b.path,tt==='bq'?'dataset.table':'schema.table'),'A table this warehouse can read.')}
    ${fld('Load pattern','pattern',bSel('pattern',b.pattern,[['incremental',b.kind==='files'?'Incremental: only new or changed files':'Incremental: only rows past the watermark'],['full','Full: everything each run']]))}
    ${b.kind==='table'&&b.pattern!=='full'?fld('Watermark column','watermark',bText('watermark',b.watermark,'updated_at'),'A column that only grows. Empty means every run reads the whole table.'):''}
  </div>
  <div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn" data-act="bDetect">Read columns from the source</button>${b.kind==='files'?`<button class="btn" data-act="bBrowse">Show the files</button>${b.conn?`<button class="btn" data-act="filesOpen" data-id="${esc(b.conn)}" data-folder="${esc(b.path)}">Upload files</button>`:''}`:''}</div>
  ${fld('Columns','columns',bArea('columns',b.columns,9,'order_id:STRING:key\ncustomer_email:STRING:pii\namount:DECIMAL(18,2)\norder_date:DATE'),'One per line as <code>name:TYPE</code>. Add <code>:key</code> to the business key and <code>:pii</code> to personal data. Types: '+TYPES.join(', ')+'.')}`}
  case 1:return `<div class="form-grid">${fld('When a new column appears','evolve',bSel('evolve',b.evolve,[['addNewColumns','Accept it and add the column'],['rescue','Keep it in a side column'],['failOnNewColumns','Stop the pipeline']]),'You can change this later under Controls.')}</div>
    <dl class="kv"><dt>Table</dt><dd class="mono">bronze.${esc(t)}_raw on ${esc(eng?eng.id:'')}</dd><dt>Types</dt><dd>Every source column is stored as text</dd><dt>Lineage columns</dt><dd class="mono">_ingest_ts, _ingest_date, _source_file, _batch_id</dd>
    <dt>Rerun safety</dt><dd>${b.pattern==='full'?'The table is reloaded, so a rerun gives the same result':b.kind==='files'?'Each file is loaded once. A changed file is loaded again':'Only rows past the stored watermark are read'}</dd></dl>`;
  case 2:return `<div class="form-grid">
    ${fld('When a value cannot be cast','onBad',bSel('onBad',b.onBad,[['quarantine','Quarantine the row and continue'],['stop','Stop the pipeline and alert']]),b.onBad==='stop'?'Nothing reaches Silver until the data is fixed. Bad rows are saved with the reason.':'Good rows keep flowing. Bad rows wait in quarantine with the reason.')}
    ${fld('History','scd',bSel('scd',b.scd,[['1','Keep the latest version only (SCD Type 1)'],['2','Keep every version (SCD Type 2)']]),b.scd==='2'?'Adds valid_from, valid_to and is_current.':'An update overwrites the previous values.')}
  </div><label class="check"><input type="checkbox" id="b-maskPii" data-change="bf" data-k="maskPii" ${b.maskPii?'checked':''}> Hash columns marked as personal data (SHA-256)</label>
  <dl class="kv"><dt>Business key</dt><dd class="mono">${keys.length?esc(keys.join(', ')):'None marked. The first column is used. Add :key to a column in the Source step.'}</dd><dt>Casts</dt><dd>${cols.filter(c=>c.type!=='STRING').map(c=>`<span class="mono">${esc(c.name)} to ${esc(c.type)}</span>`).join(', ')||'None'}</dd><dt>Hashed</dt><dd class="mono">${b.maskPii?esc(cols.filter(c=>c.pii).map(c=>c.name).join(', '))||'No columns marked :pii':'Off'}</dd><dt>Duplicates</dt><dd>The newest row per key wins</dd></dl>
  <p class="muted small">After deploying you can refine each column under Mappings, and each check under Controls.</p>`;
  case 3:return `<div class="form-grid">
    ${fld('Gold table name','gold.name',bText('gold.name',b.gold.name,'agg_'+t+'_daily'))}
    ${fld('Model','gold.type',bSel('gold.type',b.gold.type,[['aggregate','Aggregate'],['dimension','Dimension'],['sql','My own SQL']]),b.gold.type==='dimension'?'One row per entity, with a surrogate key.':b.gold.type==='sql'?'Any single SELECT. It is rebuilt on every run.':'Measures summed at a declared grain.')}
    ${b.gold.type==='aggregate'?fld('Date column','gold.dateCol',bText('gold.dateCol',b.gold.dateCol,'order_date'))+fld('Group by','gold.dims',bText('gold.dims',b.gold.dims,'channel, country'),'Comma separated. With the date this is the grain.'):''}
  </div>${b.gold.type==='aggregate'?fld('Measures','gold.measures',bArea('gold.measures',b.gold.measures,4),'One SQL expression per line, each with an alias, for example <code>SUM(amount) AS revenue</code>.')
    :b.gold.type==='sql'?fld('Gold SQL','gold.sql',bArea('gold.sql',b.gold.sql,8),'Use <code>{silver}</code> for this pipeline\'s Silver table, and <code>{silver.other_table}</code> or <code>{gold.other_table}</code> to join tables of other pipelines.'):''}`;
  case 4:{const nx=nextRuns(b.cron,2);return `<div class="form-grid">
    ${fld('Schedule (cron, UTC)','cron',bText('cron',b.cron),`<span id="b-cron-text">${cronText(b.cron)}${nx.length?'. Next: '+stamp(nx[0]):''}</span>`)}
    ${fld('Retries per task','retries',`<input type="number" id="b-retries" min="0" max="5" data-input="bf" data-k="retries" value="${b.retries}">`,'For warehouse and network errors, with a growing wait.')}
  </div><dl class="kv"><dt>Runs on</dt><dd>The scheduler in this page, so a schedule fires only while the page is open. The deployed service runs it around the clock</dd><dt>Task order</dt><dd class="mono">bronze_${esc(t)} &rarr; checks &rarr; silver_${esc(t)} &rarr; checks &rarr; gold &rarr; checks</dd><dt>Concurrency</dt><dd>One run at a time, so loads stay in order</dd></dl>`}
  default:return `<dl class="kv"><dt>Pipeline</dt><dd class="mono">${esc(t)}_medallion</dd><dt>Warehouse</dt><dd>${eng?esc(eng.id)+' ('+esc(ctype(eng.type).name)+')':'None chosen'}</dd><dt>Source</dt><dd>${b.kind==='files'?esc(b.conn)+' / '+esc(b.path)+', '+b.format.toUpperCase()+' files':'table '+esc(b.path)}, ${b.pattern} load</dd>
    <dt>Bronze</dt><dd class="mono">bronze.${esc(t)}_raw</dd><dt>Silver</dt><dd><span class="mono">silver.${esc(t)}</span>, SCD Type ${b.scd}, values that cannot be cast ${b.onBad==='stop'?'stop the run':'are quarantined'}</dd><dt>Gold</dt><dd class="mono">gold.${esc((b.gold.name||'agg_'+t).replace(/[^a-z0-9_]/gi,'_').toLowerCase())}</dd>
    <dt>Columns</dt><dd>${cols.length} declared${keys.length?', key '+esc(keys.join(', ')):''}</dd><dt>Schedule</dt><dd>${cronText(b.cron)}, ${b.retries} retries</dd></dl>
    <div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn primary" data-act="bDeploy" data-run="1">Deploy and run now</button><button class="btn" data-act="bDeploy">Deploy only</button><button class="btn" data-act="bReset">Start over</button></div>
    <p class="muted small">Deploying registers the pipeline with a mapping and starter checks. Tables are created in the warehouse on the first run.</p>`;
  }
}
VIEWS.builder={nav:'Pipeline builder',render(){
  if(!engines().length)return head('Pipeline builder','A pipeline needs a warehouse to build its tables in.')+`<div class="banner info"><div><b>Connect a warehouse first.</b> Add a local SQLite lake and a landing area for your files.</div><button class="btn primary sm" data-act="go" data-view="connections">Add a connection</button></div>`;
  if(!S.builder)S.builder=defaultBuilder();const b=S.builder;planFetch();
  return head('Pipeline builder','Describe a dataset once. You get the Bronze, Silver and Gold SQL that will run, starter checks, and a schedule.',`<button class="btn" data-act="go" data-view="agent">Let the agent draft it</button>`)+
  `<ol class="steps">${STEPS.map((s,i)=>`<li><button data-act="bStep" data-i="${i}" ${i===b.step?'aria-current="step"':''} class="${i<b.step?'done':''}">${s}</button></li>`).join('')}</ol>
  <div class="cols split">
    <div class="panel"><header><h2>${STEPS[b.step]}</h2><span class="muted small">Step ${b.step+1} of ${STEPS.length}</span></header><div class="body" style="display:flex;flex-direction:column;gap:14px">${builderStep(b)}
      <div style="display:flex;gap:8px;justify-content:space-between"><button class="btn" data-act="bStep" data-i="${b.step-1}" ${b.step===0?'disabled':''}>Back</button>${b.step<STEPS.length-1?`<button class="btn primary" data-act="bStep" data-i="${b.step+1}">Next: ${STEPS[b.step+1]}</button>`:''}</div></div></div>
    <div id="codepane" style="display:flex;flex-direction:column;gap:8px;min-width:0">${builderCode()}</div>
  </div>`;
}};
const STEP_TAB=['bronze','bronze','silver','gold'];
ACT.bStep=d=>{const b=S.builder,i=Math.max(0,Math.min(STEPS.length-1,+d.i));b.step=i;if(STEP_TAB[i]&&!['spark','orch'].includes(b.tab))b.tab=STEP_TAB[i];render();$('#content').scrollTop=0};
ACT.bTab=d=>{S.builder.tab=d.tab;$('#codepane').innerHTML=builderCode()};
function bSet(v,d){const b=S.builder,k=d.k;if(k.startsWith('gold.'))b.gold[k.slice(5)]=v;else b[k]=v;return k}
INP.bf=(v,d)=>{const k=bSet(v,d);planFetch();if(k==='cron'){const t=$('#b-cron-text'),nx=nextRuns(v,1);if(t)t.textContent=cronText(v)+(nx.length?'. Next: '+stamp(nx[0]):'')}};
CHG.bf=(v,d)=>{const b=S.builder,k=bSet(v,d);if(k==='target'||k==='kind'){const cs=builderConns(b);b.conn=cs[0]?cs[0].id:'';if(targetType(b)==='local'&&!['csv','json'].includes(b.format))b.format='csv'}rerender()};
ACT.bReset=()=>{S.builder=defaultBuilder();S.ui.plan=null;render()};
ACT.bDetect=async(d,el)=>{const b=S.builder;el.disabled=true;el.textContent='Reading the source';try{const r=await api('POST','/api/columns',{cfg:builderCfg(b)});b.columns=r.columns;render();toast(`${r.count} columns read from the source. Check the types and mark the key.`)}catch(e){toast(e.message,'bad');el.disabled=false;el.textContent='Read columns from the source'}};
ACT.bBrowse=()=>{const b=S.builder;openDrawer('Files in the source folder','<div class="empty">Listing files</div>');api('POST','/api/files',{connection:b.conn,path:b.path,format:b.format}).then(r=>{
  $('#drawer-body').innerHTML=r.files.length?`<p class="muted">${r.total} ${b.format.toUpperCase()} file${r.total===1?'':'s'} under this folder${r.total>r.files.length?', showing the first '+r.files.length:''}.</p><div class="scroll-x"><table class="tbl"><thead><tr><th>File</th><th class="r">Size</th><th>Modified</th></tr></thead><tbody>${r.files.map(f=>`<tr><td class="mono">${esc(f.name)}</td><td class="r num">${fmt(f.size)} B</td><td class="num">${f.modified?stamp(f.modified):''}</td></tr>`).join('')}</tbody></table></div>`:`<div class="empty">No ${b.format.toUpperCase()} files were found under this folder.</div>`},e=>{$('#drawer-body').innerHTML=`<div class="banner">${esc(e.message)}</div>`})};
ACT.bDeploy=async d=>{const b=S.builder;try{const p=await api('POST','/api/pipelines',{cfg:builderCfg(b)});S.builder=null;S.ui.plan=null;S.ui.orchPid=p.id;if(d.run)await api('POST',`/api/pipelines/${p.id}/run`,{});await sync(true);go('orchestration');toast(d.run?`${p.id} deployed and its first run has started`:`${p.id} deployed. Select Run now to load it.`)}catch(e){toast(e.message,'bad')}};
