/* ---------- notify.py: a channel only delivers when it has been configured, and none can be from a browser ---------- */
function deliver(title,detail,ctl){const sent=[],missing=[],available=settings.channels();
  for(const ch of ['email','chat','pager']){if(!ctl[ch])continue;if(!available[ch])missing.push(ch)}
  return [sent,missing]}

/* ---------- runner.py: a run walks the tasks in order, executes real SQL and records every step ---------- */
const runner={};
const RUNNING={};                                  // run id -> promise of the run in progress
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
class Ctx{
  constructor(p,run){
    this.p=p;this.run=run;this.eng=get_engine(store.connection(p.target));this.src=store.connection(p.source)||store.connection(p.target);
    this.f=M.fqs(p,this.eng);this.n=M.table_names(p);this.batch=run.id;this.full=!!run.full||p.pattern==='full';
    this.task=null;this.skip_rest=false;this.drift=[];this.handled=new Set();this.warnings=[]}
  log(msg,level){store.log(level||'INFO',this.p.id,this.task?this.task.id:'',msg,this.run.id)}
  sql(statements){for(const s of Array.isArray(statements)?statements:[statements])this.eng.execute(s)}
  rules(layer){const tables=new Set(this.p.writes.filter(w=>w.startsWith(layer+'.')));return store.data.rules.filter(r=>tables.has(r.table)&&(r.on===undefined||r.on))}
  mapping(){const m=store.data.mappings[`silver.${this.p.table}`];return m?m.rows:M.default_mapping(this.p).rows}
  stat(layer){
    /* refresh the catalog entry of the table this pipeline writes in a layer */
    const name=this.n[layer],tid=`${layer}.${name}`;let s,cols;
    try{s=this.eng.stats(layer,name);cols=this.eng.columns(layer,name)}catch(e){if(e instanceof EngineError)return;throw e}
    const old=store.data.tstats[tid]||{};
    store.data.tstats[tid]={rows:s.rows,bytes:s.bytes===undefined?null:s.bytes,fresh:now_ms(),version:Math.trunc(old.version||0)+1,cols:cols.map(([c,t])=>[c,t])}}
}
const notData=x=>!LINEAGE.includes(x)&&x!=='_rescued_data';
function task_bronze(c){
  const p=c.p,eng=c.eng,ctl=p.ctl;
  eng.ensure_schemas();
  let new_files=[],stage_cols;
  if(p.kind==='files'){
    const files=eng.list_files(c.src,p);
    new_files=c.full?files:files.filter(f=>p.loaded[f.name]!==f.modified);
    c.log(`Found ${files.length} file${files.length!==1?'s':''} in the source folder, ${new_files.length} to load`);
    if(!new_files.length)return _empty(c);
    stage_cols=eng.stage_files(c.src,p,new_files,c.f.stg);
  }else{
    let [src_fq,src_cols]=eng.source_table(p.path);
    src_cols=src_cols.filter(([x])=>notData(x));          // the source's own lineage is not carried over
    const since=c.full?null:p.wm;
    c.sql(eng.ctas(c.f.stg,M.stage_table_sql(p,eng,src_fq,src_cols,since)));
    stage_cols=src_cols.map(([x])=>x)}
  stage_cols=stage_cols.filter(notData);
  const staged=Number(eng.scalar(`SELECT COUNT(*) AS n FROM ${c.f.stg}`)||0);
  if(staged===0){c.sql(eng.drop(c.f.stg));return _empty(c)}

  const existing_all=eng.columns('bronze',c.n.bronze).map(([x])=>x);
  const existing=existing_all.length?existing_all.filter(notData):null;
  const declared=new Set(M.parse_cols(p.cfg.columns).map(x=>x.name)),known=existing!==null?new Set(existing):declared;
  c.drift=stage_cols.filter(x=>!known.has(x));
  if(c.drift.length){
    if(ctl.onNewColumn==='stop'){c.sql(eng.drop(c.f.stg));
      throw new DataStop(`new column${c.drift.length>1?'s':''} in the source (${c.drift.join(', ')}), and this pipeline is set to stop on schema changes`)}
    const mapped=new Set(c.mapping().map(r=>r.src));
    for(const col of c.drift){
      if(mapped.has(col)||store.data.drift.some(d=>d.table===p.writes[0]&&d.col===col))continue;
      store.data.drift.unshift({table:p.writes[0],col,type:'STRING',seen:now_ms(),status:'pending',pid:p.id})}
    c.log(`New column${c.drift.length>1?'s':''} in the source: ${c.drift.join(', ')}. `+(ctl.onNewColumn==='rescue'?'Kept in the _rescued_data column':'Added to Bronze'),'WARN')}
  if(existing!==null&&ctl.onNewColumn==='rescue'&&!existing_all.includes('_rescued_data'))c.sql(eng.add_column(c.f.bronze,'_rescued_data','STRING'));
  c.sql(M.bronze_sql(p,eng,stage_cols,existing,c.batch,c.full));
  const rows=Number(eng.scalar(`SELECT COUNT(*) AS n FROM ${c.f.bronze} WHERE _batch_id = ${eng.lit(c.batch)}`)||0);
  const wm=p.watermark;
  if(p.kind==='table'&&wm&&stage_cols.includes(wm)){const top=eng.scalar(`SELECT MAX(${eng.q(wm)}) AS v FROM ${c.f.stg}`);if(top!==null&&top!==undefined)p.wm=String(top)}
  c.sql(eng.drop(c.f.stg));
  for(const f of new_files)p.loaded[f.name]=f.modified;
  if(!p.pending.includes(c.batch))p.pending=(c.full?[]:p.pending).concat([c.batch]);
  c.run.rows.bronze=rows;
  c.stat('bronze');
  c.log(`Landed ${commas(rows)} rows as text with lineage columns`)}
function _empty(c){
  const mode=c.p.ctl.onEmpty||'skip';
  if(mode==='fail')throw new DataStop('the source delivered nothing new, and this pipeline is set to fail on an empty source');
  if(mode==='skip'){c.skip_rest=true;
    /* the hint after the first sentence is an addition of this copy; the service in the repository logs the first sentence only */
    c.log('The source has nothing new. The run ends here and no table is changed'+(c.p.pending&&c.p.pending.length?'. Rows from an earlier run are still waiting in Bronze. Use Retry on that failed run, or a full reload, to carry them into Silver':''));return}
  if(!c.eng.exists('bronze',c.n.bronze))throw new DataStop('the source is empty and nothing has ever been loaded, so there is nothing to continue with');
  c.log('The source has nothing new. Continuing with no new rows','WARN')}
function _apply(c,rule,checked,failed,note,failures,can_quarantine){
  /* record a check result and decide what it does. Returns true when its rows must be quarantined. */
  Q.record(rule,checked,failed);
  const label=M.rule_label(rule);
  if(!Q.violated(rule,checked,failed)){
    c.log(`Check passed: ${label}`+(note?` (${note})`:'')+(failed?`, ${commas(failed)} of ${commas(checked)} failing is inside the allowed threshold`:''));return false}
  const what=`${label} on ${rule.table}: ${commas(failed)} of ${commas(checked)} failed`+(note?` (${note})`:''),sev=rule.severity||'warn';
  if(sev==='fail'){failures.push(what);c.log(`Check failed and stops the pipeline: ${what}`,'ERROR');return !!can_quarantine}
  if(sev==='quarantine'&&can_quarantine){c.log(`Check failed, rows go to quarantine: ${what}`,'WARN');return true}
  c.warnings.push(what);
  c.log(`Check warned: ${what}`+(sev==='warn'?'':'. This check is evaluated after the table is written, so it cannot quarantine rows'),'WARN');
  return false}
const gated=p=>p.ctl.gate===undefined?true:!!p.ctl.gate;
function task_dq(c,layer){
  const p=c.p,eng=c.eng,gate=layer==='gold'&&gated(p);
  const ctx={batches:layer==='bronze'?[c.batch]:null,drift:layer==='bronze'?c.drift:null,bronze_rows:c.run.rows.bronze,silver_rows:c.run.rows.silver,
    quarantined:c.run.quarantined||0,in_run:true,override:gate?{[`gold.${c.n.gold}`]:c.f.gold_new}:{}};
  const failures=[];let n=0;
  for(const rule of c.rules(layer)){
    if(c.handled.has(rule.id))continue;
    const res=Q.evaluate(rule,eng,p,store.data,ctx);
    if(res===null)continue;
    n+=1;_apply(c,rule,res[0],res[1],res[2],failures)}
  if(failures.length){
    if(gate)c.sql(eng.drop(c.f.gold_new));
    throw new DataStop(failures.join('; ')+(gate?'. Gold was not published, so readers keep the previous version':''))}
  if(gate){
    c.sql(eng.ctas(c.f.gold,`SELECT * FROM ${c.f.gold_new}`));
    c.sql(eng.drop(c.f.gold_new));
    c.stat('gold');
    c.log('Gold checks passed. The new version is published')}
  const L=layer.charAt(0).toUpperCase()+layer.slice(1);
  c.log(n?`${n} ${L} check${n!==1?'s':''} evaluated`:`No ${L} checks are switched on`)}
function task_silver(c){
  const p=c.p,eng=c.eng,ctl=p.ctl;
  const bronze_cols=eng.columns('bronze',c.n.bronze).map(([x])=>x);
  if(!bronze_cols.length)throw new DataStop('the Bronze table does not exist yet');
  const mapping=c.mapping(),row_rules=c.rules('silver').filter(r=>ROW_RULES.includes(r.type));
  const batches=(p.pending&&p.pending.length?p.pending:[c.batch]).slice();
  try{
    const [statements,flags]=M.silver_check_sql(p,eng,mapping,row_rules,bronze_cols,batches,c.full);
    c.sql(statements);
    const counts=flags.map(([,name])=>`SUM(CASE WHEN ${name} THEN 1 ELSE 0 END) AS ${name}`).join(', ');
    const r=eng.query(`SELECT COUNT(*) AS n${counts?', '+counts:''} FROM ${c.f.chk}`)[0],total=Number(r.n||0);
    const failures=[],exclude=[],reasons=[];
    for(const [rule,name] of flags){
      c.handled.add(rule.id);
      if(_apply(c,rule,total,Number(r[name]||0),'',failures,true)){exclude.push(name);reasons.push(`CASE WHEN ${name} THEN ${eng.lit(M.rule_label(rule))} END`)}}
    let quarantined=0,where='';
    if(exclude.length){where='('+exclude.join(' OR ')+')';quarantined=Number(eng.scalar(`SELECT COUNT(*) AS n FROM ${c.f.chk} WHERE ${where}`)||0)}
    const share=total?quarantined/total*100:0,breaker=Number(ctl.breaker===undefined?5:ctl.breaker);
    if(quarantined&&!failures.length&&share>breaker)failures.push(`${pyfixed(share,1)}% of the rows would be quarantined, above the ${String(breaker)}% limit`);
    if(quarantined){
      c.sql(eng.create_if_missing(c.f.quar,[['_run_id','STRING'],['_quarantined_at','TIMESTAMP'],['_reason','STRING'],['_row','STRING']]));
      c.sql(`DELETE FROM ${c.f.quar} WHERE _run_id = ${eng.lit(c.run.id)}`);
      c.sql(`INSERT INTO ${c.f.quar} (_run_id, _quarantined_at, _reason, _row)\nSELECT ${eng.lit(c.run.id)}, ${eng.now()}, ${eng.join_reasons(reasons)}, _row\nFROM ${c.f.chk}\nWHERE ${where}`);
      const top=eng.query(`SELECT _reason AS reason, COUNT(*) AS n FROM ${c.f.quar} WHERE _run_id = ${eng.lit(c.run.id)} GROUP BY _reason ORDER BY n DESC LIMIT 1`);
      store.data.quarantine=store.data.quarantine.filter(x=>x.run!==c.run.id);
      store.data.quarantine.unshift({table:p.writes[1],rows:quarantined,reason:top.length?top[0].reason:'',path:c.f.quar.replace(/[`"]/g,''),ts:now_ms(),run:c.run.id,pid:p.id});
      c.run.quarantined=quarantined}
    if(failures.length)throw new DataStop(failures.join('; ')+(quarantined?`. ${commas(quarantined)} bad rows were saved to quarantine with the reason`:''));
    c.sql(M.silver_new_sql(p,eng,mapping,exclude));
    const merged=Number(eng.scalar(`SELECT COUNT(*) AS n FROM ${c.f.new}`)||0);
    const ex=eng.columns('silver',c.n.silver).map(([x])=>x),existing=ex.length?ex:null;
    c.sql(M.silver_write_sql(p,eng,mapping,existing,c.full));
    c.run.rows.silver=merged;
    const removed=total-quarantined-merged;
    c.log(`Cast and validated ${commas(total)} rows. ${commas(merged)} merged, ${commas(quarantined)} quarantined`+(removed>0?`, ${commas(removed)} duplicates removed`:''));
    p.pending=[];
    c.stat('silver');
  }finally{
    /* the staging tables hold unmasked values, so they never outlive the task */
    for(const key of ['chk','new'])try{c.sql(eng.drop(c.f[key]))}catch(e){if(!(e instanceof EngineError))throw e}}}
function task_gold(c){
  const p=c.p,eng=c.eng;
  c.sql(M.gold_sql(p,eng));
  const target=gated(p)?c.f.gold_new:c.f.gold,rows=Number(eng.scalar(`SELECT COUNT(*) AS n FROM ${target}`)||0);
  c.run.rows.gold=rows;
  if(!gated(p))c.stat('gold');
  c.log(`Built ${commas(rows)} rows`+(gated(p)?'. Waiting for the Gold checks before publishing':' and published'))}
function _task_fn(task){
  if(task.layer==='bronze')return task_bronze;
  if(task.layer==='silver')return task_silver;
  if(task.layer==='gold')return task_gold;
  const layer=task.id.replace('dq_','');
  return c=>task_dq(c,layer)}

/* run lifecycle */
runner.start_run=function(pid,trigger,full,resume){trigger=trigger||'manual';
  const p=store.pipeline(pid);
  if(!p)throw new ApiError(`There is no pipeline called ${pid}`);
  if(p.status==='paused'&&trigger!=='manual')throw new ApiError(`${pid} is paused`);
  if(store.data.runs.some(r=>r.pid===pid&&r.status==='running'))throw new ApiError(`${pid} already has a run in progress`);
  let run;
  if(resume){
    run=resume;const tasks=p.tasks;
    let failed_at=tasks.findIndex(t=>(run.tasks[t.id]||{}).status!=='success');if(failed_at<0)failed_at=0;
    if(tasks[failed_at].id==='dq_gold'&&gated(p)&&failed_at>0)failed_at-=1;          // the unpublished Gold build was dropped, so build it again
    for(const t of tasks.slice(failed_at))run.tasks[t.id]={status:'queued',tries:0,start:null,end:null};
    Object.assign(run,{status:'running',end:null,failMsg:'',failedTask:null});
  }else{
    run={id:store.next_id('run_'),pid,start:now_ms(),end:null,dur:0,status:'running',trigger,rows:{bronze:0,silver:0,gold:0},quarantined:0,failMsg:'',failedTask:null,
      full:!!full,note:full?'full reload':'',tasks:{}};
    for(const t of p.tasks)run.tasks[t.id]={status:'queued',tries:0,start:null,end:null};
    store.data.runs.unshift(run)}
  store.log('INFO',pid,p.tasks[0].id,`Run ${run.id} ${resume?'resumed':'started'} by ${trigger}`+(full?' as a full reload':''),run.id);
  store.save();
  RUNNING[run.id]=sleep(20).then(()=>_execute(pid,run.id)).catch(e=>{try{_finish(store.pipeline(pid),store.find('runs',run.id),null,'failed',String(e&&e.message||e))}catch(_){}}).finally(()=>{delete RUNNING[run.id]});
  return run};
async function _execute(pid,run_id){
  const p=store.pipeline(pid),run=store.find('runs',run_id);
  if(!p||!run)return;
  let c;
  try{c=new Ctx(p,run)}catch(e){return _finish(p,run,null,'failed',String(e.message||e))}
  let fail_msg='',status='success';
  for(const task of p.tasks){
    const st=run.tasks[task.id];
    if(st.status==='success')continue;
    if(c.skip_rest){st.status='skipped';continue}
    c.task=task;
    const fn=_task_fn(task);
    st.start=st.start||now_ms();
    for(;;){
      st.status='running';st.tries+=1;store.save();
      await sleep(0);                                   // let the page draw the step before the SQL runs
      try{fn(c);Object.assign(st,{status:'success',end:now_ms()});break}
      catch(e){
        if(e instanceof DataStop){Object.assign(st,{status:'failed',end:now_ms()});fail_msg=e.message;c.log(`Stopped: ${e.message}`,'ERROR');break}
        /* an engine problem: worth retrying */
        const msg=e instanceof ApiError?e.message:`${e&&e.name||'Error'}: ${e&&e.message||e}`;
        if(st.tries<=(p.retries||0)){
          st.status='retry';
          const wait=Math.min(settings.retry_base_seconds*Math.pow(2,st.tries-1),300);
          c.log(`Attempt ${st.tries} failed: ${msg}. Retry ${st.tries} of ${p.retries} in ${String(wait)} s`,'WARN');
          store.save();
          await sleep(wait*1000);
          continue}
        Object.assign(st,{status:'failed',end:now_ms()});fail_msg=msg;
        c.log(`Task failed after ${st.tries} attempt${st.tries>1?'s':''}: ${msg}`,'ERROR');break}}
    if(st.status==='failed'){
      status='failed';run.failedTask=task.id;
      for(const t of p.tasks)if(run.tasks[t.id].status==='queued')run.tasks[t.id].status='upstream_failed';
      break}}
  _finish(p,run,c,status,fail_msg)}
function _finish(p,run,c,status,fail_msg){
  if(!p||!run)return;
  const ctl=p.ctl;
  Object.assign(run,{status,end:now_ms(),failMsg:fail_msg});
  run.dur=Math.max(1,Math.round((run.end-run.start)/1000));
  const last=p.tasks[p.tasks.length-1].id;
  if(status==='success'){
    const skipped=c!==null&&c.skip_rest;
    if(!skipped)p.history=(p.history||[]).slice(-19).concat([{run:run.id,rows:run.rows.bronze}]);
    store.log('INFO',p.id,last,skipped?`Run ${run.id} ended early: nothing new to load`:
      `Run ${run.id} succeeded in ${run.dur} s. ${commas(run.rows.bronze)} rows landed, ${commas(run.rows.silver)} merged, ${commas(run.quarantined||0)} quarantined`,run.id);
    const notes=c?c.warnings.slice():[];
    if(run.quarantined)notes.unshift(`${commas(run.quarantined)} rows quarantined from ${p.writes[1]}`);
    if(notes.length)_alert(p,'warn',`${p.name}: ${notes[0]}`,notes.slice(1,3).join(' ')||'The run continued.',ctl.alertWarn,'quality');
    else if(ctl.alertOk&&!skipped)_alert(p,'info',`${p.name} succeeded`,`${commas(run.rows.bronze)} rows landed.`,true,'orchestration',false);
  }else{
    if(!run.failedTask)run.failedTask=p.tasks[0].id;
    store.log('ERROR',p.id,run.failedTask,`Run ${run.id} failed at ${run.failedTask}: ${fail_msg}`,run.id);
    _alert(p,'bad',`${p.name} failed at ${run.failedTask}`,`${fail_msg.slice(0,1).toUpperCase()+fail_msg.slice(1)}. Everything downstream was held.`,ctl.alertFail,'orchestration')}
  store.save()}
function _alert(p,sev,title,detail,enabled,view,store_it){
  if(!enabled){store.log('WARN',p.id,'alerts',`Not alerted (switched off for this pipeline): ${title}`);return}
  const [sent,missing]=deliver(title,detail,p.ctl);
  const where=(sent.length?' Sent to '+sent.join(', ')+'.':'')+(missing.length?' Not delivered to '+missing.join(', ')+' because that channel is not configured.':'');
  if(store_it===undefined||store_it)store.alert(sev==='bad'?'bad':'warn',title,detail+where,view,p.id)}
runner.retry_run=function(run_id){const run=store.find('runs',run_id);
  if(!run||run.status!=='failed')throw new ApiError('Only a failed run can be retried');
  return runner.start_run(run.pid,run.trigger||'manual',false,run)};

/* scheduler: start every pipeline whose cron matches this minute, and raise freshness alerts */
runner.tick=function(){
  const t=new Date();t.setUTCSeconds(0,0);
  const key=t.toISOString().slice(0,16),started=[];
  for(const p of store.data.pipelines.slice()){
    if(p.status!=='active'||p.lastSched===key||!cron_match(p.cron,t))continue;
    p.lastSched=key;
    try{runner.start_run(p.id,'schedule');started.push(p.id)}
    catch(e){if(!(e instanceof ApiError))throw e;store.log('WARN',p.id,'scheduler',`Scheduled run skipped: ${e.message}`)}}
  _freshness();
  return started};
function _freshness(){
  for(const rule of store.data.rules){
    if(rule.type!=='freshness'||!(rule.on===undefined||rule.on))continue;
    const p=store.data.pipelines.find(x=>x.writes.includes(rule.table)),fresh=(store.data.tstats[rule.table]||{}).fresh;
    if(!p||!fresh)continue;
    const age=(now_ms()-fresh)/60000,limit=Q.minutes(rule.param),late=age>limit,was_late=(rule.pass===undefined?100:rule.pass)<100;
    Q.record(rule,1,late?1:0);
    if(late&&!was_late)_alert(p,'warn',`${rule.table} is late`,`Last written ${pyfixed(age,0)} minutes ago against a target of ${pyfixed(limit,0)}.`,p.ctl.alertSla,'monitoring')}}
