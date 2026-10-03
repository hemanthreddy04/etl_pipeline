/* grants: the statements each cloud warehouse would get. They can only be applied by the deployed service. */
service.grants=function(apply,roles){
  if(roles!==undefined&&roles!==null){store.data.roles=roles;store.audit('Changed access matrix','roles')}
  roles=store.data.roles.length?store.data.roles:DEFAULT_ROLES;
  const out=[];
  for(const conn of store.data.connections){
    if(!CLOUD_ENGINES[conn.type])continue;
    for(const role of roles){const who=String(role.principal||'').trim();if(!who)continue;
      for(const layer of LAYERS){const acc=(role.acc||{})[layer]||'none';if(acc==='none')continue;
        const schema=settings.schema_prefix+layer;
        if(conn.type==='bq')out.push({conn:conn.id,sql:`GRANT \`${acc==='write'?'roles/bigquery.dataEditor':'roles/bigquery.dataViewer'}\` ON SCHEMA \`${conn.endpoint||'<project of the service>'}.${schema}\` TO ${JSON.stringify(who)}`});
        else out.push({conn:conn.id,sql:`GRANT ${acc==='write'?'USE SCHEMA, SELECT, MODIFY':'USE SCHEMA, SELECT'} ON SCHEMA \`${(conn.options||{}).catalog||'main'}\`.\`${schema}\` TO \`${who.replace(/`/g,'')}\``})}}}
  if(apply){for(const g of out)g.result='failed: '+NEEDS_SERVICE(CLOUD_ENGINES[store.connection(g.conn).type]);
    store.audit('Applied grants','access',`0 of ${out.length} statements applied`)}
  return {statements:out}};

/* ---------- tools.py: the same list serves the MCP endpoint, the agent and the Try button ---------- */
const STR={type:'string'};
const _obj=(props,required)=>({type:'object',properties:props,required:required||[]});
function _run_sql(a){
  const sql=String(a.sql||'').trim().replace(/;+$/,'');
  if(!/^(select|with)\b/i.test(sql)||sql.includes(';'))throw new ApiError('Only a single SELECT statement is allowed through this tool');
  const conn=store.connection(a.connection||'')||store.data.connections.find(c=>ENGINE_TYPES[c.type]);
  if(!conn)throw new ApiError('There is no warehouse connection yet');
  const rows=get_engine(conn).query(`SELECT * FROM (${sql}) q LIMIT 50`);
  return {connection:conn.id,rows,row_count:rows.length}}
function _run_status(a){
  const pid=a.pipeline,run=store.data.runs.find(r=>r.pid===pid);
  if(!run)return {pipeline:pid,state:'never run'};
  const tasks={};for(const [k,v] of Object.entries(run.tasks))tasks[k]=v.status;
  return {pipeline:pid,run:run.id,state:run.status,failed_task:run.failedTask||null,reason:run.failMsg,rows:run.rows,quarantined:run.quarantined||0,tasks}}
const strip=a=>{const o={};for(const k in a)if(!k.startsWith('_'))o[k]=a[k];return o};
const TOOLS=[
  {name:'list_pipelines',write:false,desc:'List pipelines with their schedule, state and last run.',schema:_obj({}),
    fn:a=>({pipelines:store.data.pipelines.map(p=>({id:p.id,engine:p.engine,schedule:p.cron,state:p.status,writes:p.writes,last_run:(store.data.runs.find(r=>r.pid===p.id)||{status:null}).status}))})},
  {name:'list_connections',write:false,desc:'List connections by name, type and health. Secrets are never returned.',schema:_obj({}),
    fn:a=>({connections:store.data.connections.map(c=>({id:c.id,type:c.type,endpoint:c.endpoint,status:c.status||null}))})},
  {name:'list_tables',write:false,desc:'List Bronze, Silver and Gold tables with row counts and freshness.',schema:_obj({layer:STR}),
    fn:a=>({tables:service.tables_view().filter(t=>!a.layer||t.layer===a.layer).map(t=>({table:t.id,rows:t.rows,built:t.built,pipeline:t.pid}))})},
  {name:'get_table_schema',write:false,desc:'Columns and types of one table, named as layer.table.',schema:_obj({table:STR},['table']),
    fn:a=>{const t=service.tables_view().find(x=>x.id===a.table);return t?{table:t.id,columns:t.cols.map(c=>({name:c.name,type:c.type,key:c.key,personal_data:c.pii}))}:{error:`table ${a.table} not found`}}},
  {name:'preview_table',write:false,desc:'First rows of a table, named as layer.table.',schema:_obj({table:STR,limit:{type:'integer'}},['table']),
    fn:a=>service.preview_table(a.table,a.limit===undefined?5:a.limit)},
  {name:'run_sql',write:false,desc:'Run one read-only SELECT on a warehouse connection. At most 50 rows come back.',schema:_obj({sql:STR,connection:STR},['sql']),fn:_run_sql},
  {name:'list_source_files',write:false,desc:'List files under a folder of a file connection.',schema:_obj({connection:STR,path:STR,format:STR},['connection','path']),
    fn:a=>service.list_source_files(a.connection,a.path,a.format||'csv')},
  {name:'get_run_status',write:false,desc:'State of the latest run of a pipeline, with task states and the failure reason.',schema:_obj({pipeline:STR},['pipeline']),fn:_run_status},
  {name:'get_run_logs',write:false,desc:'Log lines of a run, oldest first.',schema:_obj({run:STR},['run']),
    fn:a=>({lines:store.data.logs.filter(l=>l.runId===a.run).reverse().map(l=>`${l.level} [${l.task}] ${l.msg}`).slice(0,80)})},
  {name:'list_checks',write:false,desc:'Data quality checks, optionally for one table.',schema:_obj({table:STR}),
    fn:a=>({checks:store.data.rules.filter(r=>!a.table||r.table===a.table).map(r=>({id:r.id,table:r.table,column:r.column,type:r.type,rule:r.param,on_failure:r.severity,enabled:r.on===undefined?true:r.on,pass_rate:r.pass}))})},
  {name:'get_approval',write:false,desc:'Result of a change that was waiting for approval.',schema:_obj({id:STR},['id']),
    fn:a=>{const x=store.data.approvals.find(x=>x.id===a.id);return x?{id:x.id,status:x.status,result:x.result===undefined?null:x.result}:{error:'unknown approval'}}},
  {name:'run_pipeline',write:true,desc:'Start a run of a pipeline. Set full to reload everything.',schema:_obj({pipeline:STR,full:{type:'boolean'}},['pipeline']),
    fn:a=>({run:service.start(a.pipeline,!!a.full,a._by||'tool').id,state:'running'})},
  {name:'retry_run',write:true,desc:'Retry a failed run from the task that failed.',schema:_obj({run:STR},['run']),fn:a=>({run:runner.retry_run(a.run).id,state:'running'})},
  {name:'create_pipeline',write:true,
    desc:"Create a Bronze, Silver and Gold pipeline. columns is one 'name:TYPE' per line, with ':key' on the business key and ':pii' on personal data. kind is files or table. gold.type is aggregate, dimension or sql.",
    schema:_obj({name:STR,target:STR,conn:STR,kind:STR,format:STR,path:STR,pattern:STR,watermark:STR,columns:STR,scd:STR,onBad:STR,cron:STR,
      gold:_obj({name:STR,type:STR,dateCol:STR,dims:STR,measures:STR,sql:STR})},['name','target','columns']),
    fn:a=>({pipeline:service.deploy_pipeline(strip(a),a._by||'tool').id})},
  {name:'add_check',write:true,desc:'Add a data quality check. severity is fail, quarantine or warn.',schema:_obj({table:STR,column:STR,type:STR,param:STR,severity:STR},['table','type']),
    fn:a=>({check:service.add_rule(a).id})},
  {name:'set_control',write:true,desc:'Change one control of a pipeline, for example onEmpty, onNewColumn, dedup, mask, breaker, gate or retries.',
    schema:_obj({pipeline:STR,key:STR,value:{}},['pipeline','key','value']),fn:a=>({controls:service.set_control(a.pipeline,a.key,a.value).ctl})},
  {name:'pause_pipeline',write:true,desc:'Pause or resume the schedule of a pipeline.',schema:_obj({pipeline:STR},['pipeline']),fn:a=>({state:service.toggle_pipeline(a.pipeline).status})}];
const BY_NAME={};for(const t of TOOLS)BY_NAME[t.name]=t;
const tools={};
tools.needs_approval=tool=>tool.write&&(store.data.toolApproval[tool.name]===undefined?true:!!store.data.toolApproval[tool.name]);
tools.describe=()=>TOOLS.map(t=>({name:t.name,desc:t.desc,write:t.write,approve:tools.needs_approval(t),schema:t.schema}));
tools.call=function(name,args,by,approved){
  /* run a tool. A change that needs approval is parked and returns {status: 'waiting_for_approval'}. */
  const tool=BY_NAME[name];
  if(!tool)throw new ApiError(`Unknown tool ${name}`);
  args={...(args||{})};
  if(tools.needs_approval(tool)&&!approved){
    const req={id:store.next_id('ap'),ts:now_ms(),tool:name,args,by,status:'pending',result:null};
    store.data.approvals.unshift(req);store.save();
    return {status:'waiting_for_approval',approval_id:req.id,note:"A person must approve this change on the MCP page of the control plane."}}
  args._by=by;
  const result=tool.fn(args);
  if(tool.write)store.audit('Tool call',name,`by ${by}`,by);
  return result};
tools.decide=function(approval_id,ok){
  const req=store.data.approvals.find(x=>x.id===approval_id);
  if(!req||req.status!=='pending')throw new ApiError('That request is no longer waiting');
  if(!ok)Object.assign(req,{status:'rejected',result:{error:'rejected by a person'}});
  else{try{Object.assign(req,{status:'approved',result:tools.call(req.tool,req.args,req.by,true)})}catch(e){Object.assign(req,{status:'failed',result:{error:String(e.message||e)}})}}
  store.audit(ok?'Approved change':'Rejected change',req.tool,`requested by ${req.by}`);
  return req};

/* ---------- mcp_rpc.py: the JSON-RPC handler behind /mcp. Here it is called in the page instead of over HTTP. ---------- */
const MCP_VERSIONS=['2025-06-18','2025-03-26','2024-11-05'];
function mcp_handle(msg,by){by=by||'mcp client';
  const method=msg.method,mid=msg.id;
  if(mid===undefined||mid===null)return null;
  const params=msg.params||{};let result;
  try{
    if(method==='initialize')result={protocolVersion:MCP_VERSIONS.includes(params.protocolVersion)?params.protocolVersion:MCP_VERSIONS[0],capabilities:{tools:{listChanged:false}},
      serverInfo:{name:'medallion-control-plane',version:'1.0.0'},instructions:"Tools to inspect and operate Bronze, Silver and Gold pipelines. Changes may wait for a person's approval."};
    else if(method==='ping')result={};
    else if(method==='tools/list')result={tools:TOOLS.map(t=>({name:t.name,description:t.desc+(t.write?' Changes data: may need approval.':''),inputSchema:t.schema}))};
    else if(method==='tools/call'){
      try{result={content:[{type:'text',text:pyjson(tools.call(params.name,params.arguments||{},by))}],isError:false}}
      catch(e){result={content:[{type:'text',text:String(e.message||e)}],isError:true}}}
    else return {jsonrpc:'2.0',id:mid,error:{code:-32601,message:`Method not found: ${method}`}};
    return {jsonrpc:'2.0',id:mid,result};
  }catch(e){return {jsonrpc:'2.0',id:mid,error:{code:-32603,message:String(e.message||e)}}}}

/* ---------- main.py: the routes ---------- */
const ROUTES=[];
function R(method,pattern,fn){const keys=[];ROUTES.push({method,keys,fn,rx:new RegExp('^'+pattern.replace(/\{(\w+)\}/g,(m,k)=>{keys.push(k);return '([^/]+)'})+'$')})}
const AGENT_OFF='The agent needs the deployed service with ANTHROPIC_API_KEY set. A page in a browser cannot hold an API key safely.';
R('GET','/api/state',()=>service.state_view());
R('POST','/api/connections',(P,b)=>service.save_connection(b,true));
R('PUT','/api/connections/{id}',(P,b)=>service.save_connection({...b,id:P.id},false));
R('DELETE','/api/connections/{id}',P=>service.delete_connection(P.id));
R('POST','/api/connections/{id}/test',P=>service.test_connection(P.id));
R('POST','/api/files',(P,b)=>service.list_source_files(b.connection,b.path||'',b.format||'csv'));
R('POST','/api/columns',(P,b)=>service.detect_columns(b.cfg||{}));
R('POST','/api/plan',(P,b)=>service.plan_sql(b.cfg,b.pipeline));
R('POST','/api/pipelines',(P,b)=>service.deploy_pipeline(b.cfg||{}));
R('DELETE','/api/pipelines/{id}',(P,b)=>service.delete_pipeline(P.id,!!b.dropTables));
R('POST','/api/pipelines/{id}/run',(P,b)=>service.start(P.id,!!b.full));
R('POST','/api/pipelines/{id}/toggle',P=>service.toggle_pipeline(P.id));
R('PUT','/api/pipelines/{id}/schedule',(P,b)=>service.set_schedule(P.id,b));
R('PUT','/api/pipelines/{id}/controls',(P,b)=>service.set_control(P.id,b.key,b.value));
R('POST','/api/pipelines/{id}/recommend',(P,b)=>service.recommend_rules(P.id,b.layer));
R('POST','/api/runs/{id}/retry',P=>runner.retry_run(P.id));
R('PUT','/api/mappings/{table}',(P,b)=>service.save_mapping(P.table,b.rows||[]));
R('POST','/api/rules',(P,b)=>service.add_rule(b));
R('POST','/api/rules/run',(P,b)=>service.run_checks(b.table));
R('PUT','/api/rules/{id}',(P,b)=>service.update_rule(P.id,b));
R('DELETE','/api/rules/{id}',P=>service.delete_rule(P.id));
R('POST','/api/tables/refresh',()=>service.refresh_tables());
R('GET','/api/tables/{id}/preview',P=>service.preview_table(P.id));
R('GET','/api/quarantine/{table}/{run}',P=>service.quarantine_rows(P.table,P.run));
R('DELETE','/api/quarantine/{table}/{run}',P=>service.discard_quarantine(P.table,P.run));
R('POST','/api/drift',(P,b)=>service.drift_action(b.table,b.col,b.action));
R('POST','/api/alerts/{id}/ack',P=>{const a=store.find('alerts',P.id);if(a){a.ack=true;store.audit('Acknowledged alert',a.title)}});
R('GET','/api/compute',()=>({compute:service.compute_view()}));
R('POST','/api/compute/{conn}/{id}',()=>service.toggle_compute());
R('POST','/api/governance/grants',(P,b)=>service.grants(!!b.apply,b.roles));
R('POST','/api/governance/erase',(P,b)=>service.erase(b.column||'',String(b.value===undefined?'':b.value),!!b.confirm));
R('GET','/api/governance/secrets',()=>({secrets:service.secrets_view()}));
R('GET','/api/tools',()=>({tools:tools.describe()}));
R('PUT','/api/tools/{name}',(P,b)=>{if(!BY_NAME[P.name])throw new NotFound('Unknown tool');store.data.toolApproval[P.name]=!!b.approve;store.audit('Changed tool approval',P.name,b.approve?'ask first':'runs without asking')});
R('POST','/api/tools/{name}',(P,b)=>({result:tools.call(P.name,b.args||{},'you',true)}));
R('POST','/api/approvals/{id}',(P,b)=>tools.decide(P.id,!!b.ok));
R('POST','/api/agent',()=>{throw new ApiError(AGENT_OFF)});
R('POST','/api/agent/approve',()=>{throw new ApiError(AGENT_OFF)});
R('POST','/api/agent/reset',()=>({ok:true}));
R('POST','/api/tick',()=>({started:runner.tick()}));
R('POST','/mcp',(P,b)=>mcp_handle(b,'mcp client'));
/* only in this copy: the landing areas hold files uploaded in the browser */
R('GET','/api/local/files/{conn}',P=>service.files_view(P.conn));
R('POST','/api/local/files/{conn}',(P,b)=>service.put_file(P.conn,b.folder,b.name,b.text));
R('DELETE','/api/local/files/{conn}',(P,b)=>service.delete_file(P.conn,b.name));
async function handle(method,path,body){
  method=String(method||'GET').toUpperCase();
  const url=String(path).split('?')[0];
  body=body===undefined||body===null?{}:clone(body);
  for(const r of ROUTES){
    if(r.method!==method)continue;
    const m=r.rx.exec(url);if(!m)continue;
    const P={};r.keys.forEach((k,i)=>{P[k]=decodeURIComponent(m[i+1])});
    const out=r.fn(P,body);
    if(method!=='GET')store.save();
    return clone(out===undefined||out===null?{ok:true}:out)}
  throw new ApiError('The service answered 404',404)}

/* ---------- keeping the work: state, uploaded files and table contents are saved in this browser ---------- */
const LS_KEY='medallion-control-plane-live-v1',LS_MAX=4.5e6;
let storage=null,tickTimer=null;
const info={saved:0,keeps:'nothing',restored:false,note:''};
function snapshot(){const dbs={};
  for(const [cid,eng] of Object.entries(ENGINES)){if(!eng.db)continue;const layers={};
    for(const layer of LAYERS){const sch=eng.schema(layer);
      layers[layer]=eng.query(`SELECT name FROM ${sch}.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).map(t=>{
        const res=eng.db.exec(`SELECT * FROM ${sch}."${String(t.name).replace(/"/g,'""')}"`);
        return {name:t.name,cols:eng.query(`PRAGMA ${sch}.table_info("${String(t.name).replace(/"/g,'""')}")`).map(r=>[r.name,r.type||'']),rows:res.length?res[0].values:[]}})}
    dbs[cid]=layers}
  return {v:1,saved:now_ms(),state:store.data,files:FILES,dbs}}
function forgetTables(){store.data.tstats={};for(const p of store.data.pipelines){p.loaded={};p.pending=[];p.wm=null}}
function restore(snap){
  store.data=Object.assign(EMPTY(),snap.state||{});FILES=snap.files||{};
  for(const k of Object.keys(ENGINES)){try{ENGINES[k].db&&ENGINES[k].db.close()}catch(e){}delete ENGINES[k]}
  /* a run that was in flight when the page closed can never finish */
  for(const r of store.data.runs)if(r.status==='running'){
    r.status='failed';r.end=now_ms();r.failMsg='the page was closed while this run was in progress';
    for(const t of Object.values(r.tasks||{})){if(t.status==='running'||t.status==='retry')t.status='failed';else if(t.status==='queued')t.status='upstream_failed'}}
  if(!snap.dbs){forgetTables();return}
  for(const [cid,layers] of Object.entries(snap.dbs)){
    const conn=store.connection(cid);if(!conn||!ENGINE_TYPES[conn.type])continue;
    const eng=get_engine(conn),db=eng._db();
    for(const layer of LAYERS)for(const t of (layers[layer]||[])){
      const fq=`${eng.schema(layer)}."${String(t.name).replace(/"/g,'""')}"`;
      db.run(`CREATE TABLE ${fq} (`+t.cols.map(([n,ty])=>`"${String(n).replace(/"/g,'""')}"`+(ty?' '+ty:'')).join(', ')+')');
      if(!t.rows.length)continue;
      db.run('BEGIN');const st=db.prepare(`INSERT INTO ${fq} VALUES (`+t.cols.map(()=>'?').join(', ')+')');
      try{for(const row of t.rows)st.run(row)}finally{st.free()}
      db.run('COMMIT')}}}
function persist(){
  if(!storage)return;
  try{
    const snap=snapshot();let text=JSON.stringify(snap),keeps='everything';
    if(text.length>LS_MAX){text=JSON.stringify({...snap,dbs:null});keeps='settings and files'}
    if(text.length>LS_MAX){storage.removeItem(LS_KEY);Object.assign(info,{saved:0,keeps:'nothing',note:'The uploaded files are too large to keep in this browser. They stay until the page is closed.'});return}
    storage.setItem(LS_KEY,text);
    Object.assign(info,{saved:snap.saved,keeps,note:keeps==='everything'?'':'The tables are too large to keep in this browser. After a reload, run each pipeline with a full reload to rebuild them.'});
  }catch(e){Object.assign(info,{saved:0,keeps:'nothing',note:'This browser does not allow the page to save its work, so it lasts until the page is closed.'})}}
const SAMPLE_NAME='vendor_returns/2026-10-01.csv';
const SAMPLE_CSV=['return_id,order_id,customer_email,reason,channel,refund_amount,return_date,updated_at',
  'R1001,O501,ava.keller@example.com,damaged,web,49.90,2026-09-30,2026-10-01 08:00:00','R1002,O502,noah.singh@example.com,wrong size,app,120.00,2026-09-30,2026-10-01 08:05:00',
  'R1003,O503,mia.laurent@example.com,changed mind,store,35.50,2026-09-30,2026-10-01 08:10:00','R1004,O504,kenji.ito@example.com,damaged,web,"12,90",2026-09-30,2026-10-01 08:15:00',
  'R1005,O505,sara.okafor@example.com,late delivery,web,18.00,2026-10-01,2026-10-01 09:00:00','R1005,O505,sara.okafor@example.com,late delivery,web,18.00,2026-10-01,2026-10-01 09:00:00',
  ',O506,li.wei@example.com,damaged,app,22.00,2026-10-01,2026-10-01 09:30:00','R1007,O507,omar.haddad@example.com,wrong size,store,75.25,2026-10-01,2026-10-01 10:00:00',
  'R1008,O508,ines.costa@example.com,changed mind,web,210.00,2026-10-01,2026-10-01 10:20:00','R1009,O509,tom.baker@example.com,damaged,app,64.10,not a date,2026-10-01 11:00:00',
  'R1010,O510,zoe.martin@example.com,wrong size,web,15.75,2026-10-01,2026-10-01 11:30:00','R1011,O511,raj.patel@example.com,late delivery,store,99.99,2026-10-01,2026-10-01 12:00:00',''].join('\n');
function seed(){
  /* a local engine and a landing area with one sample file, so a pipeline can be built straight away. No pipelines. */
  store.data.connections.push({id:'lake',status:'idle',tested:0,note:'',type:'local',endpoint:'in this browser',auth:'None',secret:'',options:{},cloud:'local'},
    {id:'landing',status:'idle',tested:0,note:'',type:'folder',endpoint:'in this browser',auth:'None',secret:'',options:{},cloud:'local'});
  FILES.landing={[SAMPLE_NAME]:{text:SAMPLE_CSV,size:SAMPLE_CSV.length,modified:now_ms(),sample:true}};
  service.test_connection('lake');service.test_connection('landing');
  store.data.logs=[];
  store.audit('Prepared the workspace','lake, landing','A local engine and a landing area holding one sample file. No pipelines.','setup')}
function wipe(){
  for(const k of Object.keys(ENGINES)){try{ENGINES[k].db&&ENGINES[k].db.close()}catch(e){}delete ENGINES[k]}
  FILES={};store.data=EMPTY()}
function init(opts){
  opts=opts||{};SQL=opts.SQL;storage=opts.storage||null;wipe();
  let restored=false;
  if(storage){
    try{const raw=storage.getItem(LS_KEY);if(raw){const snap=JSON.parse(raw);if(snap&&snap.v===1){restore(snap);restored=true;info.saved=snap.saved||0;info.keeps=snap.dbs?'everything':'settings and files'}}}
    catch(e){wipe();restored=false;info.note='The work saved in this browser could not be read, so the page started fresh.'}}
  if(!restored&&opts.seed!==false)seed();
  info.restored=restored;
  hooks.persist=storage?persist:null;
  if(tickTimer)clearInterval(tickTimer);
  if(opts.scheduler!==false)tickTimer=setInterval(()=>{try{runner.tick()}catch(e){}},20000);
  return info}
function reset(){
  if(Object.keys(RUNNING).length)throw new ApiError('Wait for the run in progress to finish first');
  wipe();try{if(storage)storage.removeItem(LS_KEY)}catch(e){}
  Object.assign(info,{saved:0,keeps:'nothing',restored:false,note:''});
  seed();store.save()}
return {init,handle,reset,info,mcp:(msg,by)=>clone(mcp_handle(clone(msg),by)),
  set onChange(fn){hooks.change=fn},
  idle:()=>Promise.all(Object.values(RUNNING)),
  _:{M,Q,service,runner,tools,store,settings,sha256,try_cast,parseCsv,norm_type,persist,snapshot,restore,files:()=>FILES}};
})();
if(typeof module!=='undefined')module.exports=BE;
