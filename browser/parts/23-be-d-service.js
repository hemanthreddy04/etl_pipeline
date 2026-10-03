/* ---------- service.py: everything the site and the tools can ask the control plane to do ---------- */
const FILE_TYPES={folder:'local',gcs:'bq',adls:'adb'};          // file connection -> engine that reads it
const DEFAULT_ROLES=[
  {id:'data_engineer',name:'Data engineers',principal:'',acc:{bronze:'write',silver:'write',gold:'write'}},
  {id:'analytics_engineer',name:'Analytics engineers',principal:'',acc:{bronze:'none',silver:'read',gold:'write'}},
  {id:'data_scientist',name:'Data scientists',principal:'',acc:{bronze:'none',silver:'read',gold:'read'}},
  {id:'analyst',name:'Analysts and BI tools',principal:'',acc:{bronze:'none',silver:'none',gold:'read'}}];
const service={};
function _pipeline(pid){const p=store.pipeline(pid);if(!p)throw new NotFound(`There is no pipeline called ${pid}`);return p}
function _engine(p){return get_engine(store.connection(p.target))}
function _pipeline_of(table_id){const p=store.data.pipelines.find(x=>x.writes.includes(table_id));if(!p)throw new NotFound(`No pipeline writes ${table_id}`);return p}

/* what the site shows */
service.tables_view=function(){const out=[];
  for(const p of store.data.pipelines){
    const conn=store.connection(p.target)||{},cls=ENGINE_TYPES[conn.type],declared=M.parse_cols(p.cfg.columns);
    const mapping=(store.data.mappings[p.writes[1]]||{}).rows||[];
    const pii_src=new Set(mapping.filter(r=>r.pii).map(r=>r.src).concat(declared.filter(c=>c.pii).map(c=>c.name))),pii_tgt=new Set(mapping.filter(r=>r.pii).map(r=>r.tgt));
    const names=M.table_names(p),gold_kind=(p.cfg.gold||{}).type||'aggregate';
    const spec={
      bronze:[names.bronze,declared.map(c=>[c.name,'STRING']).concat(LINEAGE.map(x=>[x,x==='_ingest_ts'?'TIMESTAMP':x==='_ingest_date'?'DATE':'STRING'])),
        p.pattern==='full'?'Full reload each run':'Incremental append, new files or rows only',['src:'+p.id]],
      silver:[names.silver,mapping.length?M.silver_columns(p,mapping):[],p.scd==='2'?'SCD Type 2, full history':`Upsert on ${p.keys.join(', ')} (SCD Type 1)`,[p.writes[0]]],
      gold:[names.gold,[],{aggregate:'Rebuilt each run',dimension:'Rebuilt each run',sql:'Rebuilt each run from your SQL'}[gold_kind]||'Rebuilt each run',[p.writes[1]]]};
    for(const layer of LAYERS){
      let [name,cols,pattern,up]=spec[layer];
      const tid=`${layer}.${name}`,st=store.data.tstats[tid],has=!!st,s=st||{},live=s.cols;
      if(live&&live.length)cols=live.map(([c,t])=>[c,t]);
      const pii=layer==='bronze'?pii_src:pii_tgt;
      out.push({id:tid,layer,name,platform:p.platform,owner:p.owner||'data-eng',
        tags:(cols.some(([c])=>pii.has(c))?['pii']:[]).concat(has?[]:['not built yet']),
        version:s.version||0,up,fmt:cls?cls.table_format:'Table',part:'None',cluster:'None',pattern,rows:s.rows||0,gb:(s.bytes||0)/1e9,fresh:s.fresh||0,ret:'',built:has,pid:p.id,
        cols:cols.map(([c,t])=>({name:c,type:String(t),key:p.keys.includes(c)&&layer!=='gold',pii:pii.has(c),mask:pii.has(c)&&layer!=='bronze'&&p.ctl.mask?'hash':'none',desc:''}))})}}
  return out};
service.state_view=function(){const d=store.data;
  return {settings:settings.public(),connections:d.connections,pipelines:d.pipelines,tables:service.tables_view(),
    sources:d.pipelines.map(p=>({id:'src:'+p.id,name:p.path,conn:p.source})),
    runs:d.runs.slice(0,300),logs:d.logs.slice(0,500),alerts:d.alerts,rules:d.rules,quarantine:d.quarantine,mappings:d.mappings,drift:d.drift,audit:d.audit.slice(0,200),
    approvals:d.approvals.slice(0,40),roles:d.roles.length?d.roles:DEFAULT_ROLES,now:now_ms()}};

/* connections */
service.save_connection=function(body,create){
  const cid=ident(String(body.id||'').trim().toLowerCase(),'connection name'),typ=body.type;
  if(!ENGINE_TYPES[typ]&&!CLOUD_ENGINES[typ]&&!FILE_TYPES[typ])throw new ApiError('Choose a connection type');
  let endpoint=String(body.endpoint||'').trim();
  if(!endpoint&&(typ==='local'||typ==='folder'))endpoint='in this browser';
  if(!endpoint&&typ!=='bq')throw new ApiError('Fill in where this system is');
  const options={};for(const [k,v] of Object.entries(body.options||{}))if(String(v).trim())options[k]=String(v).trim();
  const existing=store.connection(cid);
  if(create&&existing)throw new ApiError(`A connection named ${cid} already exists`);
  if(!create&&!existing)throw new NotFound(`There is no connection called ${cid}`);
  const conn=existing||{id:cid,status:'idle',tested:0,note:''};
  Object.assign(conn,{type:typ,endpoint,auth:String(body.auth||''),secret:String(body.secret||'').trim(),options,cloud:{local:'local',folder:'local',bq:'gcp',gcs:'gcp',adb:'azure',adls:'azure'}[typ]});
  if(!existing){store.data.connections.push(conn);if(typ==='folder'&&!FILES[cid])FILES[cid]={}}
  store.audit(create?'Added connection':'Edited connection',cid,typ);
  return service.test_connection(cid)};
service.test_connection=function(cid){
  const conn=store.connection(cid);
  if(!conn)throw new NotFound(`There is no connection called ${cid}`);
  const start=now_ms();
  try{let note;
    if(ENGINE_TYPES[conn.type]){const eng=get_engine(conn);note=eng.test();eng.ensure_schemas()}
    else if(conn.type==='folder'){const n=Object.keys(FILES[cid]||{}).length;note=`${n} file${n===1?'':'s'} uploaded`}
    else throw new EngineError(NEEDS_SERVICE({bq:'BigQuery',gcs:'Cloud Storage',adb:'Azure Databricks',adls:'Azure storage'}[conn.type]||'that system'));
    Object.assign(conn,{status:'healthy',note});
  }catch(e){Object.assign(conn,{status:'down',note:String(e.message||e).slice(0,400)})}
  conn.tested=now_ms();conn.ms=now_ms()-start;
  store.log(conn.status==='healthy'?'INFO':'ERROR','connections',cid,`Connection test ${conn.status==='healthy'?'passed':'failed'} in ${conn.ms} ms: ${conn.note}`);
  store.save();
  return conn};
service.delete_connection=function(cid){
  const used=store.data.pipelines.filter(p=>cid===p.target||cid===p.source).map(p=>p.id);
  if(used.length)throw new ApiError(`${cid} is used by ${used.join(', ')}. Delete or repoint those pipelines first.`);
  store.data.connections=store.data.connections.filter(c=>c.id!==cid);
  delete FILES[cid];if(ENGINES[cid]){try{ENGINES[cid].db&&ENGINES[cid].db.close()}catch(e){}delete ENGINES[cid]}
  store.audit('Removed connection',cid)};

/* pipelines */
function _new_rule(table,column,typ,param,severity){
  return {id:store.next_id('dq'),table,column:column||'*',type:typ,param:param||'',severity,on:true,min:100,pass:100,checked:0,failed:0,last:0}}
function _prepare(cfg){
  const target=store.connection(cfg.target||'');
  if(target&&CLOUD_ENGINES[target.type])throw new ApiError(NEEDS_SERVICE(CLOUD_ENGINES[target.type]));
  if(!target||!ENGINE_TYPES[target.type])throw new ApiError('Choose the warehouse this pipeline writes to');
  const cls=ENGINE_TYPES[target.type],p=M.build_pipeline(cfg,target,cls);
  if(p.kind==='files'){
    const src=store.connection(p.source);
    if(!src||FILE_TYPES[src.type]!==target.type){
      const need={local:'a local folder',bq:'a Cloud Storage bucket',adb:'an ADLS or cloud storage path'}[target.type];
      throw new ApiError(`Files for ${cls.label} come from ${need} connection. Choose one as the source.`)}
  }else p.source=target.id;
  if(!cron_valid(p.cron))throw new ApiError('The schedule is not a valid five field cron expression');
  return [p,target]}
service.deploy_pipeline=function(cfg,by){by=by||'pipeline builder';
  const [p,target]=_prepare(cfg);
  if(store.pipeline(p.id))throw new ApiError(`${p.id} already exists. Use another dataset name, or delete the existing pipeline.`);
  const clash=store.data.pipelines.find(x=>x.writes.some(w=>p.writes.includes(w)));
  if(clash)throw new ApiError(`${clash.id} already writes one of these tables. Choose another Gold table name.`);
  p.created=now_ms();
  M.gold_select(p,get_engine(target));                 // fails early on a bad Gold definition
  store.data.pipelines.push(p);
  store.data.mappings[p.writes[1]]=M.default_mapping(p);
  for(const [t,c,ty,pa,sev] of M.default_rules(p))store.data.rules.push(_new_rule(t,c,ty,pa,sev));
  store.audit('Deployed pipeline',p.id,`By ${by}. Writes ${p.writes.join(', ')}`);
  store.log('INFO',p.id,p.tasks[0].id,`Pipeline ${p.id} registered on ${p.engine}`);
  return p};
service.plan_sql=function(cfg,pid){let p,mapping,rules;
  if(pid){p=_pipeline(pid);mapping=(store.data.mappings[p.writes[1]]||M.default_mapping(p)).rows;rules=store.data.rules}
  else{p=_prepare(cfg||{})[0];mapping=M.default_mapping(p).rows;
    rules=M.default_rules(p).map((r,i)=>({table:r[0],column:r[1],type:r[2],param:r[3],severity:r[4],id:`dq${i}`,on:true,min:100}))}
  return M.plan(p,_engine(p),mapping,rules)};
service.delete_pipeline=function(pid,drop_tables){
  const p=_pipeline(pid);
  if(store.data.runs.some(r=>r.pid===pid&&r.status==='running'))throw new ApiError('Wait for the run in progress to finish before deleting this pipeline');
  const dropped=[];
  if(drop_tables){const eng=_engine(p),f=M.fqs(p,eng);
    for(const key of ['bronze','stg','silver','chk','new','quar','gold','gold_new']){
      try{eng.execute(eng.drop(f[key]));if(['bronze','silver','gold','quar'].includes(key))dropped.push(f[key])}
      catch(e){if(!(e instanceof EngineError))throw e;store.log('WARN',pid,'delete',`Could not drop ${f[key]}: ${e.message}`)}}}
  const d=store.data,gone=new Set(p.writes);
  d.pipelines=d.pipelines.filter(x=>x.id!==pid);
  d.rules=d.rules.filter(r=>!gone.has(r.table));
  d.quarantine=d.quarantine.filter(q=>!gone.has(q.table));
  d.drift=d.drift.filter(x=>!gone.has(x.table));
  d.runs=d.runs.filter(r=>r.pid!==pid);
  d.alerts=d.alerts.filter(a=>a.pid!==pid);
  for(const t of gone){delete d.mappings[t];delete d.tstats[t]}
  store.audit('Deleted pipeline',pid,drop_tables?'Tables dropped':'Tables kept in the warehouse');
  return {dropped}};
const toInt=(v,dflt)=>{const n=parseInt(v,10);return Number.isFinite(n)?n:dflt};
service.set_schedule=function(pid,body){
  const p=_pipeline(pid),cron=String(body.cron||p.cron).trim();
  if(!cron_valid(cron))throw new ApiError('That is not a valid cron expression. Use five fields, for example 0 * * * *');
  Object.assign(p,{cron,retries:Math.max(0,Math.min(5,toInt(body.retries===undefined?p.retries:body.retries,0)||0)),sla:Math.max(1,toInt(body.sla===undefined?p.sla:body.sla,p.sla)||p.sla)});
  store.audit('Changed schedule',pid,`${cron}, ${p.retries} retries`);
  return p};
service.toggle_pipeline=function(pid){const p=_pipeline(pid);
  p.status=p.status==='paused'?'active':'paused';
  store.audit(p.status==='paused'?'Paused pipeline':'Resumed pipeline',pid);
  return p};
service.set_control=function(pid,key,value){
  const p=_pipeline(pid);
  if(key==='retries')p.retries=Math.max(0,Math.min(5,Math.trunc(parseFloat(value||0))||0));
  else if(key==='breaker')p.ctl.breaker=Math.max(0,Math.min(100,parseFloat(value||0)||0));
  else if(key==='onNewColumn'||key==='onEmpty'){
    const allowed={onNewColumn:['accept','rescue','stop'],onEmpty:['skip','continue','fail']}[key];
    if(!allowed.includes(value))throw new ApiError(`${key} must be one of ${allowed.join(', ')}`);
    p.ctl[key]=value}
  else if(key in DEFAULT_CTL)p.ctl[key]=!!value;
  else throw new ApiError(`Unknown control ${key}`);
  store.audit('Changed control',pid,`${key} = ${key==='retries'?p.retries:pystr(p.ctl[key])}`);
  return p};

/* mappings and checks */
service.save_mapping=function(table,rows){
  const p=_pipeline_of(table),clean=[],seen=new Set();
  for(const r of rows){
    const tgt=ident(String(r.tgt||'').trim(),'target column');
    if(seen.has(tgt))throw new ApiError(`Target column ${tgt} is mapped twice`);
    seen.add(tgt);
    clean.push({src:String(r.src||'').trim(),stype:'STRING',tgt,ttype:norm_type(r.ttype),tx:String(r.tx||'trim').trim(),nullable:r.nullable===undefined?true:!!r.nullable,pii:!!r.pii})}
  const missing=p.keys.filter(k=>!seen.has(k));
  if(missing.length)throw new ApiError(`The business key ${missing.join(', ')} must stay in the mapping`);
  store.data.mappings[table]={source:p.writes[0],rows:clean};
  for(const r of clean)             // a required column gets a not-null check if it has none
    if(!r.nullable&&!store.data.rules.some(x=>x.table===table&&x.column===r.tgt&&x.type==='not_null'))store.data.rules.push(_new_rule(table,r.tgt,'not_null','','fail'));
  store.audit('Saved mapping',table,`${clean.length} columns`);
  return store.data.mappings[table]};
service.add_rule=function(body){
  const table=String(body.table||''),p=_pipeline_of(table),typ=body.type;
  if(!DIMENSION[typ])throw new ApiError('Choose a check type');
  const col=String(body.column||'*').trim();
  if(col!=='*')ident(col,'column');
  const severity=['fail','quarantine','warn'].includes(body.severity)?body.severity:'warn';
  const rule=_new_rule(table,col,typ,String(body.param||'').trim(),severity);
  if(ROW_RULES.includes(typ)&&typ!=='custom_sql'&&col==='*')throw new ApiError('This check needs a column name');
  if(M.rule_flag(rule,_engine(p),'x')===null&&['range','accepted_values','regex','referential'].includes(typ))
    throw new ApiError({range:'Give the range as two numbers, for example 0 to 100000',accepted_values:'List the allowed values, separated by commas',
      regex:'Give the pattern the value must match',referential:'Name the reference as layer.table.column'}[typ]);
  store.data.rules.push(rule);
  store.audit('Added check',`${table}.${col}`,typ,body._by||'you');
  return rule};
service.update_rule=function(rid,body){
  const r=store.find('rules',rid);
  if(!r)throw new NotFound('That check no longer exists');
  if('on' in body)r.on=!!body.on;
  if('min' in body)r.min=Math.max(0,Math.min(100,parseFloat(body.min||0)||0));
  if(['fail','quarantine','warn'].includes(body.severity))r.severity=body.severity;
  store.audit('Changed check',`${r.table}.${r.column}`,`${r.type}: on=${r.on?'True':'False'}, pass at least ${String(r.min)}%, on failure ${r.severity}`);
  return r};
service.delete_rule=function(rid){const r=store.find('rules',rid);
  store.data.rules=store.data.rules.filter(x=>x.id!==rid);
  if(r)store.audit('Removed check',`${r.table}.${r.column}`,r.type)};
service.recommend_rules=function(pid,layer){
  const p=_pipeline(pid),mapping={};for(const [t,m] of Object.entries(store.data.mappings))mapping[t]=m.rows;
  let added=0;
  for(const [t,c,ty,pa,sev] of Q.recommended(p,layer,mapping)){
    if(store.data.rules.some(r=>r.table===t&&r.column===c&&r.type===ty))continue;
    store.data.rules.push(_new_rule(t,c,ty,pa,sev));added+=1}
  store.audit('Added recommended checks',pid,`${added} on ${layer}`);
  return {added}};
service.run_checks=function(table){
  /* evaluate every enabled check against the tables as they are now. Nothing is written or quarantined. */
  let done=0,problems=0,skipped=0;
  for(const rule of store.data.rules){
    if(!(rule.on===undefined||rule.on)||(table&&rule.table!==table))continue;
    const p=store.data.pipelines.find(x=>x.writes.includes(rule.table));
    if(!p||!(rule.table in store.data.tstats)){skipped+=1;continue}
    let res;
    try{res=Q.evaluate(rule,_engine(p),p,store.data)}
    catch(e){if(!(e instanceof EngineError))throw e;store.log('WARN',p.id,'checks',`Check ${M.rule_label(rule)} on ${rule.table} could not run: ${e.message}`);skipped+=1;continue}
    if(res===null){skipped+=1;continue}
    Q.record(rule,res[0],res[1]);done+=1;problems+=Q.violated(rule,res[0],res[1])?1:0}
  store.audit('Ran checks',table||'all tables',`${done} evaluated, ${problems} failing, ${skipped} not applicable yet`);
  return {evaluated:done,failing:problems,skipped}};

/* tables, quarantine, drift */
function _table_ref(table_id){const p=_pipeline_of(table_id),dot=table_id.indexOf('.');return [p,_engine(p),table_id.slice(0,dot),table_id.slice(dot+1)]}
service.preview_table=function(table_id,limit){if(limit===undefined)limit=5;
  const [,eng,layer,name]=_table_ref(table_id);
  if(!eng.exists(layer,name))return {columns:[],rows:[],note:'This table has not been built yet. Run the pipeline first.'};
  const rows=eng.query(`SELECT * FROM ${eng.fq(layer,name)} LIMIT ${Math.max(1,Math.min(50,toInt(limit,5)))}`);
  return {columns:rows.length?Object.keys(rows[0]):eng.columns(layer,name).map(([c])=>c),rows,note:''}};
service.refresh_tables=function(){
  for(const p of store.data.pipelines){
    let eng;try{eng=_engine(p)}catch(e){if(e instanceof EngineError)continue;throw e}
    const names=M.table_names(p);
    for(const layer of LAYERS){const name=names[layer],tid=`${layer}.${name}`;let cols,s;
      try{cols=eng.columns(layer,name);if(!cols.length)continue;s=eng.stats(layer,name)}catch(e){if(e instanceof EngineError)continue;throw e}
      const old=store.data.tstats[tid]||{};
      store.data.tstats[tid]={rows:s.rows,bytes:s.bytes===undefined?null:s.bytes,fresh:old.fresh||now_ms(),version:old.version===undefined?1:old.version,cols:cols.map(([c,t])=>[c,t])}}}
  store.save()};
service.quarantine_rows=function(table_id,run_id){
  const [p,eng]=_table_ref(table_id),f=M.fqs(p,eng);
  const rows=eng.query(`SELECT _reason, _row FROM ${f.quar} WHERE _run_id = ${eng.lit(run_id)} LIMIT 50`);
  return {rows:rows.map(r=>{let data;try{data=JSON.parse(r._row);if(data===null||typeof data!=='object')data={row:r._row}}catch(e){data={row:r._row}}return {reason:r._reason,row:data}})}};
service.discard_quarantine=function(table_id,run_id){
  const [p,eng]=_table_ref(table_id),f=M.fqs(p,eng);
  eng.execute(`DELETE FROM ${f.quar} WHERE _run_id = ${eng.lit(run_id)}`);
  store.data.quarantine=store.data.quarantine.filter(q=>!(q.table===table_id&&q.run===run_id));
  store.audit('Discarded quarantined rows',table_id,run_id)};
service.drift_action=function(table_id,col,action){
  const d=store.data.drift.find(x=>x.table===table_id&&x.col===col);
  if(!d)throw new NotFound('That column is no longer waiting for a decision');
  const p=_pipeline_of(table_id);
  if(action==='map'){const silver=p.writes[1];
    if(!store.data.mappings[silver])store.data.mappings[silver]=M.default_mapping(p);
    const m=store.data.mappings[silver];
    if(!m.rows.some(r=>r.tgt===col))m.rows.push({src:col,stype:'STRING',tgt:ident(col,'column'),ttype:'STRING',tx:'trim',nullable:true,pii:false});
    d.status='mapped';store.audit('Mapped new column',silver,`${col}. It is filled from the next run on`)}
  else{d.status='ignored';store.audit('Kept new column in Bronze only',table_id,col)}
  return d};

/* compute and governance */
service.compute_view=function(){const out=[];
  for(const conn of store.data.connections){
    if(!ENGINE_TYPES[conn.type]&&!CLOUD_ENGINES[conn.type])continue;
    let items;
    try{items=get_engine(conn).compute()}catch(e){items=[{id:conn.id,kind:CLOUD_ENGINES[conn.type]||'Engine',spec:String(e.message||e).slice(0,200),state:'down',canToggle:false}]}
    const used=store.data.pipelines.filter(p=>p.target===conn.id).map(p=>p.id);
    for(const it of items)out.push({...it,conn:conn.id,platform:{local:'local',bq:'gcp',adb:'azure'}[conn.type],used})}
  return out};
service.toggle_compute=function(){throw new ApiError('This engine has no compute to start or stop')};
service.erase=function(column,value,confirm){
  /* delete every row for one person from Silver and Gold tables that carry the given column */
  ident(column,'column');
  const plan=[];
  for(const t of service.tables_view()){
    if(t.layer==='bronze'||!t.built||!t.cols.some(c=>c.name===column))continue;
    const [,eng,layer,name]=_table_ref(t.id),fq=eng.fq(layer,name);
    const n=Number(eng.scalar(`SELECT COUNT(*) AS n FROM ${fq} WHERE ${eng.to_str(eng.q(column))} = ${eng.lit(value)}`)||0);
    plan.push({table:t.id,rows:n});
    if(confirm&&n)eng.execute(`DELETE FROM ${fq} WHERE ${eng.to_str(eng.q(column))} = ${eng.lit(value)}`)}
  if(confirm)store.audit('Erased a person',column,`${plan.reduce((a,x)=>a+x.rows,0)} rows deleted from ${plan.filter(x=>x.rows).length} tables`);
  return {tables:plan,done:!!confirm}};
service.secrets_view=function(){return store.data.connections.filter(c=>c.secret).map(c=>({name:c.secret,used:c.id,found:'missing'}))};
service.list_source_files=function(cid,path,fmt){
  const src=store.connection(cid);
  if(!src||!FILE_TYPES[src.type])throw new ApiError('Choose a file connection');
  const want=FILE_TYPES[src.type];
  if(CLOUD_ENGINES[want])throw new ApiError(NEEDS_SERVICE(CLOUD_ENGINES[want]));
  const eng_conn=store.data.connections.find(c=>c.type===want);
  if(!eng_conn)throw new ApiError('Add the warehouse connection that reads this storage first');
  const files=get_engine(eng_conn).list_files(src,{path:path||'',format:fmt||'csv'});
  return {files:files.slice(0,200),total:files.length}};
service.detect_columns=function(cfg){
  /* look at the real source and suggest the column list: names from the file header or table, types from the values */
  const target=store.connection(cfg.target||'');
  if(target&&CLOUD_ENGINES[target.type])throw new ApiError(NEEDS_SERVICE(CLOUD_ENGINES[target.type]));
  if(!target||!ENGINE_TYPES[target.type])throw new ApiError('Choose the warehouse first');
  const eng=get_engine(target);let cols;
  if(cfg.kind==='table'){cols=eng.source_table(String(cfg.path||''))[1].map(([c,t])=>[c,norm_type(t)]).filter(([c])=>!LINEAGE.includes(c))}
  else{
    const src=store.connection(cfg.conn||'');
    if(!src||FILE_TYPES[src.type]!==target.type)throw new ApiError('Choose the file connection that belongs to this warehouse');
    const files=eng.list_files(src,cfg);
    if(!files.length)throw new ApiError('No files of that format were found in the folder');
    eng.ensure_schemas();
    const stage=eng.fq('bronze',ident(slug(cfg.name||'dataset'),'dataset name')+'__peek');
    try{
      const names=eng.stage_files(src,cfg,files.slice(-1),stage),probes=[];
      for(const c of names){const v=`NULLIF(TRIM(${eng.q(c)}), '')`;
        probes.push(`SUM(CASE WHEN ${v} IS NOT NULL THEN 1 ELSE 0 END) AS ${eng.q('n_'+c)}`);
        for(const [tag,typ] of [['i','BIGINT'],['d','DECIMAL(18,2)'],['t','TIMESTAMP'],['b','BOOLEAN']])probes.push(`SUM(CASE WHEN ${eng.try_cast(v,typ)} IS NOT NULL THEN 1 ELSE 0 END) AS ${eng.q(tag+'_'+c)}`);
        probes.push(`SUM(CASE WHEN LENGTH(${v}) = 10 AND ${eng.try_cast(v,'DATE')} IS NOT NULL THEN 1 ELSE 0 END) AS ${eng.q('y_'+c)}`)}
      const r=names.length?eng.query(`SELECT ${probes.join(', ')} FROM ${stage}`)[0]:{};
      cols=names.map(c=>{const n=Number(r['n_'+c]||0),hit=tag=>n>0&&Number(r[tag+'_'+c]||0)>=0.9*n;      // the few misfits are what the checks catch
        return [c,hit('i')?'BIGINT':hit('d')?'DECIMAL(18,2)':hit('y')?'DATE':hit('t')?'TIMESTAMP':'STRING']});
    }finally{eng.execute(eng.drop(stage))}}
  const found=cols.find(([c])=>c.toLowerCase()==='id'||c.toLowerCase().endsWith('_id')),key=found?found[0]:(cols.length?cols[0][0]:'');
  const out=cols.map(([c,t])=>`${c}:${t}`+(c===key?':key':/email|phone|full_name|first_name|last_name|address/i.test(c)?':pii':''));
  return {columns:out.join('\n'),count:cols.length}};
service.start=function(pid,full,trigger){trigger=trigger||'manual';
  const run=runner.start_run(pid,trigger,!!full);
  store.audit('Triggered run',pid,run.id+(full?', full reload':''),trigger==='manual'?'you':trigger);
  return run};

/* the landing areas: files uploaded in this browser */
const MAX_FILE=4*1024*1024;
service.files_view=function(cid){const conn=store.connection(cid);
  if(!conn||conn.type!=='folder')throw new NotFound('That is not a landing area');
  const all=FILES[cid]||{};
  return {files:Object.keys(all).sort().map(n=>({name:n,size:all[n].size,modified:all[n].modified,used:store.data.pipelines.filter(p=>p.source===cid&&(n===p.path||n.startsWith(String(p.path).replace(/^\/+|\/+$/g,'')+'/'))).map(p=>p.id)}))}};
service.put_file=function(cid,folder,name,text){const conn=store.connection(cid);
  if(!conn||conn.type!=='folder')throw new NotFound('That is not a landing area');
  folder=String(folder||'').trim().replace(/\\/g,'/').replace(/^\/+|\/+$/g,'');
  name=String(name||'').trim().replace(/\\/g,'/').split('/').pop();
  if(!folder)throw new ApiError('Name the folder the files go into, for example orders');
  if(!/^[A-Za-z0-9_\-/]+$/.test(folder))throw new ApiError('A folder name may use letters, digits, underscores, hyphens and slashes');
  if(!name)throw new ApiError('Give the file a name');
  if(!/\.(csv|json|jsonl|ndjson)$/i.test(name))throw new ApiError(`${name} is not a CSV or JSON file. The local engine reads .csv, .json, .jsonl and .ndjson.`);
  text=String(text===undefined||text===null?'':text);
  if(!text.trim())throw new ApiError(`${name} is empty`);
  if(text.length>MAX_FILE)throw new ApiError(`${name} is larger than 4 MB. This copy keeps data in the page, so use a smaller extract here and the deployed service for full-size data.`);
  if(!FILES[cid])FILES[cid]={};
  FILES[cid][folder+'/'+name]={text,size:new TextEncoder().encode(text).length,modified:now_ms()};
  store.audit('Uploaded file',cid,`${folder}/${name}`);
  service.test_connection(cid);
  return {name:folder+'/'+name}};
service.delete_file=function(cid,name){
  if(!FILES[cid]||!FILES[cid][name])throw new NotFound('That file is no longer there');
  delete FILES[cid][name];
  store.audit('Removed file',cid,name);
  service.test_connection(cid);
  return {ok:true}};
