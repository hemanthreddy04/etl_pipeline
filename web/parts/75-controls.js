/* ---------- Controls: what each pipeline does, and what happens when a check fails ---------- */
const LAYERS=['bronze','silver','gold'];
const ACTION_RANK={fail:3,quarantine:2,warn:1};
const ACTIONS=[['fail','Stop the pipeline'],['quarantine','Quarantine the rows'],['warn','Warn and continue']];
const SCN={
 clean:{name:'Clean data',desc:'Every row is valid. This is the normal path.'},
 bad_cast:{name:'Values that cannot be cast',layer:'silver',types:['castable'],pct:0.3,what:'values that cannot be cast to their type',desc:'A few rows carry text where a number or date is expected, such as "12,90".'},
 null_keys:{name:'Missing business keys',layer:'silver',types:['not_null'],pct:0.5,what:'an empty business key',desc:'Some rows arrive without the key that identifies them.'},
 dup_keys:{name:'Duplicate keys',layer:'silver',types:['unique'],pct:1.2,what:'a key that appears more than once',desc:'The source sends the same record twice.'},
 out_of_range:{name:'Values outside the allowed range',layer:'silver',types:['range','accepted_values','regex'],pct:2,what:'values outside the allowed range or list',desc:'Negative amounts, unknown status codes or malformed emails.'},
 flood:{name:'A large share of bad rows',layer:'silver',types:['castable'],pct:8,what:'values that cannot be cast to their type',desc:'8% of the rows are broken, usually because the source changed its format.'},
 new_column:{name:'A new column appears',layer:'bronze',types:['schema'],desc:'The source adds a column nobody announced.'},
 empty:{name:'Nothing new in the source',layer:'bronze',types:['row_count'],desc:'No new file arrived, or the source table has no rows past the watermark.'},
 volume_drop:{name:'Volume drops by 60%',layer:'bronze',types:['volume_anomaly'],desc:'Far fewer rows than usual, with nothing visibly wrong. The volume check needs three earlier runs to compare with.'},
 gold_mismatch:{name:'Gold totals do not match Silver',layer:'gold',types:['reconciliation'],pct:1.8,desc:'A join in the Gold model drops or doubles rows.'}
};
function ctl(p){if(!p.ctl)p.ctl={onNewColumn:'accept',onEmpty:'skip',dedup:true,mask:true,breaker:5,gate:true,alertFail:true,alertWarn:true,alertSla:true,alertOk:false,email:true,chat:false,pager:false};return p.ctl}
function pipeRules(p,layer){return S.rules.filter(r=>p.writes.includes(r.table)&&(!layer||r.table.startsWith(layer+'.')))}
function gateTask(p,layer,type){const lt=p.tasks.filter(t=>t.layer===layer);if(type==='load'||(layer==='silver'&&type==='castable'))return lt[0]||p.tasks[0];return p.tasks.find(t=>t.id==='dq_'+layer)||lt[lt.length-1]||p.tasks[p.tasks.length-1]}
function channels(c){return [c.email&&'email',c.chat&&'chat',c.pager&&'pager'].filter(Boolean)}

/* Work out, from the current controls, what a run does when it meets a given kind of data. The run engine follows this plan. */
function predict(p,sid){
  const c=ctl(p),sc=SCN[sid]||SCN.clean,rows=((lastRun(p.id)||{rows:{}}).rows.bronze)||1000;
  const o={sid,steps:[],final:'success',warn:false,undetected:0,leak:0,failTask:null,failMsg:'',failLayer:null,quar:0,quarTask:null,badRows:0,badTable:null,hits:[],alerts:[],volFactor:1,skipAfter:null,na:false};
  const has=l=>p.writes.some(w=>w.startsWith(l+'.'));
  const say=(layer,level,text,type)=>o.steps.push({layer,level,text,task:gateTask(p,layer,type).id});
  const fail=(layer,msg,type)=>{o.final='failed';o.failLayer=layer;o.failTask=gateTask(p,layer,type).id;o.failMsg=msg};
  const judge=(layer,types,pct,bad)=>{const rs=pipeRules(p,layer).filter(r=>r.on!==false&&types.includes(r.type));if(!rs.length)return {res:'none'};
    const hit=rs.filter(r=>pct>100-(r.min==null?100:r.min)+1e-9);if(!hit.length)return {res:'within'};
    hit.forEach(r=>o.hits.push({id:r.id,pass:+(100-pct).toFixed(2),failed:bad,checked:Math.max(1,Math.round(rows))}));
    const top=hit.slice().sort((a,b)=>ACTION_RANK[b.severity]-ACTION_RANK[a.severity])[0];return {res:top.severity,rule:top}};
  if(sc.layer&&!has(sc.layer))o.na=true;
  for(const layer of LAYERS){if(!has(layer))continue;
    const L=cap(layer),n=pipeRules(p,layer).filter(r=>r.on!==false).length,active=!o.na&&sc.layer===layer,cn=n===1?'1 '+L+' check runs and passes.':n+' '+L+' checks run and pass.';
    if(o.final==='failed'){say(layer,'hold',`${L} does not run. It is held until the cause is fixed and the run is retried.`,'load');continue}
    if(o.final==='skipped'){say(layer,'hold',`${L} is skipped. There is nothing new to process.`,'load');continue}
    if(layer==='bronze'){
      if(active&&sid==='empty'){
        if(c.onEmpty==='skip'){o.final='skipped';o.volFactor=0;o.skipAfter=gateTask(p,layer,'load').id;say(layer,'ok','The source is empty. The run ends here and no table is changed.','load');continue}
        if(c.onEmpty==='fail'){fail(layer,'the source delivered no rows, and this pipeline is set to fail on an empty source','load');say(layer,'bad','The source is empty, and this pipeline is set to fail when that happens.','load');continue}
        o.volFactor=0;const j=judge(layer,['row_count'],100,0);
        if(j.res==='fail'){fail(layer,'Bronze received 0 rows, below the row count check');say(layer,'bad','Bronze lands 0 rows. The row count check stops the pipeline.');continue}
        o.warn=true;if(j.res==='none'||j.res==='within'){o.undetected=1;say(layer,'warn','Bronze lands 0 rows and no enabled check objects. Silver and Gold run on nothing new and nobody is told.')}
        else{o.alerts.push('The source was empty');say(layer,'warn','Bronze lands 0 rows. The row count check raises a warning and the run continues.')}
        continue}
      if(active&&sid==='new_column'){
        if(c.onNewColumn==='stop'){fail(layer,'a new column appeared in the source, and this pipeline is set to stop on schema changes','load');say(layer,'bad','A new column appears, and this pipeline is set to stop on schema changes. Nothing is loaded.','load');continue}
        say(layer,'ok',c.onNewColumn==='accept'?'Bronze adds the new column and lands every row. Silver ignores the column until you map it.':'Bronze lands every row and keeps the unexpected values in a side column, so nothing is lost.','load');
        const j=judge(layer,['schema'],100,0);
        if(j.res==='fail'){fail(layer,'the schema no longer matches the source contract');say(layer,'bad','The schema check stops the pipeline because the contract changed.');continue}
        o.warn=true;if(j.res==='none'||j.res==='within'){o.undetected=1;say(layer,'warn','No enabled schema check reports the change, so nobody is told about the new column.')}
        else{o.alerts.push('Schema drift: a new column appeared');say(layer,'warn','The schema check raises a schema drift warning and the run continues.')}
        continue}
      if(active&&sid==='volume_drop'){
        o.volFactor=0.4;say(layer,'ok',`Bronze lands ${num(rows*0.4)} rows, 60% fewer than usual.`,'load');
        const j=judge(layer,['volume_anomaly'],100,0);
        if(j.res==='fail'){fail(layer,'row volume fell 60% below the 7 day average');say(layer,'bad','The volume check stops the pipeline before the thin data reaches Silver.');continue}
        o.warn=true;if(j.res==='none'||j.res==='within'){o.undetected=1;say(layer,'warn','No enabled volume check notices. Reports will quietly show lower numbers.')}
        else{o.alerts.push('Volume fell 60% below the 7 day average');say(layer,'warn','The volume check raises a warning and the run continues with the rows it has.')}
        continue}
      say(layer,'ok',`Bronze lands about ${num(rows)} rows unchanged and adds lineage columns.`,'load');say(layer,'ok',n?cn:'No Bronze checks are switched on.');continue}
    if(layer==='silver'){
      if(active){
        const bad=Math.max(1,Math.round(rows*o.volFactor*sc.pct/100)),ty=sc.types[0];o.badTable=p.writes.find(w=>w.startsWith('silver.'));
        if(sid==='dup_keys'&&c.dedup){say(layer,'ok',`Deduplication keeps the newest row for each key and removes ${num(bad)} duplicates, so the unique check passes.`,'load');say(layer,'ok','The remaining rows are cast, validated and merged.');continue}
        const j=judge(layer,sc.types,sc.pct,bad);
        if(j.res==='none'){o.undetected=bad;o.leak=bad;o.warn=true;say(layer,'warn',`No enabled check looks for ${sc.what}. ${num(bad)} bad rows are merged into Silver and flow on to Gold.`,ty);continue}
        if(j.res==='within'){o.leak=bad;say(layer,'ok',`${num(bad)} rows have ${sc.what} (${sc.pct}%). That is inside the threshold you allow, so the check passes and the rows stay in Silver.`,ty);continue}
        const nm=`${j.rule.type.replace(/_/g,' ')} check on ${j.rule.column}`,gt=j.rule.type;
        if(j.res==='fail'){o.badRows=bad;fail(layer,`${num(bad)} rows have ${sc.what} (${sc.pct}%)`,gt);say(layer,'bad',`The ${nm} finds ${num(bad)} rows with ${sc.what} and stops the pipeline. The bad rows are saved to quarantine with the reason.`,gt);continue}
        if(j.res==='quarantine'){
          if(sc.pct>c.breaker){o.badRows=bad;fail(layer,`${sc.pct}% of rows would be quarantined, above the ${c.breaker}% limit`,gt);say(layer,'bad',`The ${nm} would quarantine ${num(bad)} rows (${sc.pct}%). That is above your ${c.breaker}% limit, so the run stops instead of loading a partial table.`,gt);continue}
          o.quar=bad;o.quarTask=gateTask(p,layer,gt).id;o.warn=true;o.alerts.push(`${num(bad)} rows quarantined from ${o.badTable}`);
          say(layer,'warn',`The ${nm} moves ${num(bad)} rows with ${sc.what} to quarantine. The other rows are merged into Silver.`,gt);continue}
        o.warn=true;o.leak=bad;o.alerts.push(`${num(bad)} rows with ${sc.what} in ${o.badTable}`);say(layer,'warn',`The ${nm} warns about ${num(bad)} rows with ${sc.what}. They stay in Silver.`,gt);continue}
      say(layer,'ok',`Silver casts types${c.dedup?', removes duplicates':''}${c.mask?', masks personal data':''} and merges the rows.`,'load');say(layer,'ok',n?cn:'No Silver checks are switched on.');continue}
    if(active&&sid==='gold_mismatch'){
      const j=judge(layer,['reconciliation'],sc.pct,1);
      if(j.res==='fail'){fail(layer,`Gold totals differ from Silver by ${sc.pct}%`);say(layer,'bad',c.gate?`The reconciliation check finds Gold ${sc.pct}% away from Silver and stops the run. Gold is not published, so dashboards keep the previous version.`:`The reconciliation check finds Gold ${sc.pct}% away from Silver and stops the run. Publishing does not wait for checks here, so dashboards already show the wrong numbers.`);continue}
      o.warn=true;if(j.res==='none'){o.undetected=1;say(layer,'warn',`No enabled reconciliation check compares Gold with Silver. Gold is published ${sc.pct}% off and nobody is told.`)}
      else if(j.res==='within'){o.warn=false;say(layer,'ok',`Gold is ${sc.pct}% away from Silver, inside the tolerance you allow. It is published.`)}
      else{o.alerts.push(`Gold totals differ from Silver by ${sc.pct}%`);say(layer,'warn',`The reconciliation check warns that Gold is ${sc.pct}% away from Silver. Gold is published anyway.`)}
      continue}
    say(layer,'ok',`Gold rebuilds the business tables${o.leak?', including the '+num(o.leak)+' rows that were let through':''}.`,'load');
    say(layer,'ok',n?`${cn} ${c.gate?'Gold is published.':'Gold was already published before the checks ran.'}`:'No Gold checks are switched on. Gold is published.');
  }
  return o;
}
function outcome(o){return o.na?['queued','Does not apply']:o.final==='failed'?['fail','Stops at '+cap(o.failLayer)]:o.final==='skipped'?['queued','Ends early, nothing changed']:o.undetected?['fail','Bad data gets through']:o.warn?['warn','Succeeds with warnings']:['pass','Succeeds']}
function alertLine(p,o){const c=ctl(p),ch=channels(c).join(', ');
  if(o.final==='failed')return c.alertFail&&ch?`An alert goes to ${ch}.`:'Alerting on failure is off, so nobody is told.';
  if(o.undetected)return 'No alert is sent, because nothing noticed.';
  if(o.alerts.length)return c.alertWarn&&ch?`A warning goes to ${ch}.`:'Warnings are not sent anywhere, so this is only visible in the logs.';
  return c.alertOk&&ch?`A success notice goes to ${ch}.`:'No alert is needed.'}

function genControls(p){const c=ctl(p),y=v=>v?'true':'false';
  const checks=l=>{const rs=pipeRules(p,l);return rs.length?'  checks:\n'+rs.map(r=>`    - table: ${r.table}\n      column: "${r.column}"\n      type: ${r.type}${r.param?`\n      rule: "${r.param.replace(/\\/g,'\\\\').replace(/"/g,'\\"')}"`:''}\n      pass_at_least: ${r.min==null?100:r.min}%\n      on_failure: ${r.severity}\n      enabled: ${y(r.on!==false)}`).join('\n')+'\n':'  checks: []\n'};
  const has=l=>p.writes.some(w=>w.startsWith(l+'.'));
  return `# controls/${p.id}.yml
# What this pipeline does at each step, and what happens when a check fails.
pipeline: ${p.id}

run:
  retries: ${p.retries}
  stop_if_quarantine_above: ${c.breaker}%
  alerts:
    on_failure: ${y(c.alertFail)}
    on_warning: ${y(c.alertWarn)}
    on_missed_freshness: ${y(c.alertSla)}
    on_success: ${y(c.alertOk)}
    channels: [${channels(c).join(', ')}]
${has('bronze')?`
bronze:
  on_new_column: ${c.onNewColumn}
  on_empty_source: ${c.onEmpty}
${checks('bronze')}`:''}${has('silver')?`
silver:
  deduplicate: ${y(c.dedup)}
  mask_personal_data: ${y(c.mask)}
${checks('silver')}`:''}${has('gold')?`
gold:
  publish_only_after_checks: ${y(c.gate)}
${checks('gold')}`:''}`}

function ctlLayer(p,layer){
  const c=ctl(p),rs=pipeRules(p,layer);if(!p.writes.some(w=>w.startsWith(layer+'.')))return '';
  const sel=(k,label,opts,hint)=>`<label class="field"><span>${label}</span><select id="ctl-${k}" data-change="ctlSet" data-k="${k}">${opts.map(([v,l])=>`<option value="${v}" ${c[k]===v?'selected':''}>${l}</option>`).join('')}</select>${hint?`<small>${hint}</small>`:''}</label>`;
  const sw=(k,label)=>`<label class="check"><span class="switch"><input type="checkbox" id="ctl-${k}" data-change="ctlSet" data-k="${k}" ${c[k]?'checked':''}><i></i></span>${label}</label>`;
  const beh=layer==='bronze'?`<div class="form-grid">${sel('onNewColumn','When a new column appears',[['accept','Accept it and add the column'],['rescue','Keep it in a side column'],['stop','Stop the pipeline']])}${sel('onEmpty','When the source is empty',[['skip','End the run and change nothing'],['continue','Continue with no rows'],['fail','Fail the run']])}</div>`
    :layer==='silver'?`<div style="display:flex;gap:10px 22px;flex-wrap:wrap">${sw('dedup','Remove duplicates, newest row per key wins')}${sw('mask','Mask personal data')}</div>`
    :`<div>${sw('gate','Publish Gold only after its checks pass')}</div>`;
  return `<div class="panel"><header>${disc(layer)}<h2>${cap(layer)}</h2><span class="muted small" id="ctl-count-${layer}">${rs.filter(r=>r.on!==false).length} of ${rs.length} checks on</span><button class="btn sm" data-act="ctlRecommend" data-layer="${layer}">Add recommended</button><button class="btn sm" data-act="dqAdd" data-pid="${p.id}" data-layer="${layer}">Add check</button></header>
    <div class="body">${beh}</div>
    <div class="scroll-x" style="border-top:1px solid var(--line)"><table class="tbl"><thead><tr><th>On</th><th>Check</th><th>Pass at least</th><th>If it fails</th><th></th></tr></thead><tbody>
    ${rs.map(r=>`<tr><td><label class="switch"><input type="checkbox" id="rule-on-${r.id}" ${r.on!==false?'checked':''} data-change="ruleSet" data-id="${r.id}" data-f="on" aria-label="Run this check"><i></i></label></td>
      <td><b>${esc(r.type.replace(/_/g,' '))}</b><div class="mono small">${esc(r.table.split('.')[1])}.${esc(r.column)}</div><div class="small muted" style="min-width:150px;max-width:300px">${esc(r.param||(r.type==='not_null'?'never empty':r.type==='unique'?'no repeats':''))}</div></td>
      <td><span style="display:flex;align-items:center;gap:4px"><input type="number" id="rule-min-${r.id}" min="0" max="100" step="0.1" value="${r.min==null?100:r.min}" data-change="ruleSet" data-id="${r.id}" data-f="min" aria-label="Pass at least, percent" style="min-width:70px;width:70px">%</span></td>
      <td><select id="rule-sev-${r.id}" style="min-width:176px" data-change="ruleSet" data-id="${r.id}" data-f="severity" aria-label="If it fails">${ACTIONS.map(([k,l])=>`<option value="${k}" ${r.severity===k?'selected':''}>${l}</option>`).join('')}</select></td>
      <td class="r"><button class="btn sm" data-act="dqDel" data-id="${r.id}" aria-label="Remove check">Remove</button></td></tr>`).join('')||`<tr><td colspan="5" class="empty">No checks on ${cap(layer)} yet. Select Add recommended to start with the usual ones.</td></tr>`}
    </tbody></table></div></div>`;
}
function ctlPlan(p){
  const sid=SCN[S.ui.ctlScn]?S.ui.ctlScn:'clean',o=predict(p,sid),oc=outcome(o);
  const lv={ok:['pass','OK'],warn:['warn','Warning'],bad:['fail','Stops'],hold:['queued','Held']};
  return `<div class="panel"><header><h2>What would happen</h2>${st(oc[0],oc[1])}</header>
    <div class="body" style="display:flex;flex-direction:column;gap:8px"><label class="field"><span>If the next run meets</span><select id="ctl-scn" data-change="ctlScn">${Object.keys(SCN).map(k=>`<option value="${k}" ${k===sid?'selected':''}>${SCN[k].name}</option>`).join('')}</select><small>${SCN[sid].desc}</small></label></div>
    <div style="border-top:1px solid var(--line)">${o.na?`<div class="empty">This pipeline has no ${cap(SCN[sid].layer)} step, so this case cannot happen here.</div>`:o.steps.map(s=>`<div class="plan-step ${s.level}">${disc(s.layer)}<div class="grow">${esc(s.text)}</div>${st(lv[s.level][0],lv[s.level][1])}</div>`).join('')}</div>
    <div class="body" style="border-top:1px solid var(--line)"><div><b>Alerting.</b> ${o.na?'Nothing to report.':alertLine(p,o)}</div>
      <p class="muted small" style="margin-top:8px">A forecast worked out from these settings, with example row counts. Real runs follow the same rules and report what they actually found.</p></div></div>
  <div class="panel"><header><h2>Coverage</h2><span class="muted small">Every case, with the current controls</span></header><table class="tbl"><tbody>${Object.keys(SCN).map(k=>{const x=outcome(predict(p,k));return `<tr class="click${k===sid?' sel':''}" tabindex="0" data-act="ctlPick" data-k="${k}"><td>${SCN[k].name}</td><td class="r">${st(x[0],x[1])}</td></tr>`}).join('')}</tbody></table></div>`;
}
function ctlRun(p){const c=ctl(p),av=S.settings.channels||{},sw=(k,label,note)=>`<label class="check"><input type="checkbox" id="ctl-${k}" data-change="ctlSet" data-k="${k}" ${c[k]?'checked':''}> ${label}${note?` <span class="muted small">${note}</span>`:''}</label>`;
  return `<div class="panel"><header><h2>Run behaviour</h2></header><div class="body" style="display:flex;flex-direction:column;gap:14px">
    <div class="form-grid"><label class="field"><span>Retries per task</span><input type="number" id="ctl-retries" min="0" max="5" value="${p.retries}" data-change="ctlSet" data-k="retries"><small>A retry helps with outages. A failed check is never retried, because bad data does not fix itself.</small></label>
      <label class="field"><span>Stop the run if quarantine exceeds</span><span style="display:flex;align-items:center;gap:6px"><input type="number" id="ctl-breaker" min="0" max="100" step="0.5" value="${c.breaker}" data-change="ctlSet" data-k="breaker">%</span><small>Prevents loading a table that is mostly missing.</small></label></div>
    <div><div class="small muted" style="font-weight:600;margin-bottom:6px">Send an alert</div><div style="display:flex;gap:8px 20px;flex-wrap:wrap">${sw('alertFail','When a run fails')}${sw('alertWarn','When a check warns or quarantines')}${sw('alertSla','When Gold is late')}${sw('alertOk','When a run succeeds')}</div></div>
    <div><div class="small muted" style="font-weight:600;margin-bottom:6px">Send it to</div><div style="display:flex;gap:8px 20px;flex-wrap:wrap">${sw('email','Email',av.email?'':'(not set up on the service)')}${sw('chat','Team chat',av.chat?'':'(not set up on the service)')}${sw('pager','On-call pager',av.pager?'':'(not set up on the service)')}</div>
      <p class="muted small" style="margin-top:6px">Every alert is also kept on the Overview. A channel delivers once its address is set on the service: ALERT_WEBHOOK_URL, PAGER_WEBHOOK_URL, or the SMTP settings.</p></div>
  </div></div>`}
VIEWS.controls={nav:'Controls',render(){
  const pipes=scopedPipes();let p=pipe(S.ui.orchPid);if(!p||!inScope(p.platform)){p=pipes[0];S.ui.orchPid=p?p.id:null}
  if(!p)return head('Controls','Controls appear here for each pipeline you create.',`<button class="btn primary" data-act="go" data-view="builder">New pipeline</button>`);
  return head('Controls','Decide what a pipeline does at every step: which checks run, how strict each one is, and what happens when one fails. Every run follows these settings.',
    `<button class="btn" data-act="ctlYaml">View as file</button><button class="btn" data-act="openPipe" data-id="${p.id}">Open pipeline</button>`)+
  `<div class="panel"><header><label class="field" style="flex-direction:row;align-items:center;gap:8px"><span>Pipeline</span><select id="ctl-pipe" data-change="orchPipe">${pipes.map(x=>`<option value="${x.id}" ${x.id===p.id?'selected':''}>${esc(x.name)}</option>`).join('')}</select></label>${plat(p.platform)}<span class="muted small">${esc(p.mode)}, ${cronText(p.cron).toLowerCase()}</span></header></div>
  <div class="cols wide-left"><div style="display:flex;flex-direction:column;gap:18px;min-width:0">${LAYERS.map(l=>ctlLayer(p,l)).join('')}${ctlRun(p)}</div>
    <div id="ctl-plan" class="ctl-side">${ctlPlan(p)}</div></div>`;
}};
function ctlRefresh(){const p=pipe(S.ui.orchPid);if(!p||S.view!=='controls')return;const el=$('#ctl-plan');if(el)el.innerHTML=ctlPlan(p);
  LAYERS.forEach(l=>{const e=$('#ctl-count-'+l);if(e){const rs=pipeRules(p,l);e.textContent=`${rs.filter(r=>r.on!==false).length} of ${rs.length} checks on`}})}
/* a control changes locally at once, so the forecast updates, and is saved to the service in the background */
function saveQuiet(fn){fn().then(()=>{LAST=''},e=>{toast(e.message,'bad');sync(true)})}
CHG.ctlScn=v=>{S.ui.ctlScn=v;ctlRefresh()};
ACT.ctlPick=d=>{S.ui.ctlScn=d.k;ctlRefresh()};
CHG.ctlSet=(v,d)=>{const p=pipe(S.ui.orchPid),c=ctl(p),k=d.k;
  if(k==='retries'){p.retries=Math.max(0,Math.min(5,Math.round(+v)||0));v=p.retries}else if(k==='breaker'){c.breaker=Math.max(0,Math.min(100,+v||0));v=c.breaker}else c[k]=v;
  saveQuiet(()=>api('PUT',`/api/pipelines/${p.id}/controls`,{key:k,value:v}));ctlRefresh();
  if(k==='alertFail'&&!v)toast('Failures of this pipeline will no longer alert anyone','warn');if(k==='gate'&&!v)toast('Gold will be published before its checks run','warn');
  if(['email','chat','pager'].includes(k)&&v&&!(S.settings.channels||{})[k])toast('That channel is not set up on the service yet, so nothing will be delivered to it','warn')};
CHG.ruleSet=(v,d)=>{const r=S.rules.find(x=>x.id===d.id);if(!r)return;const body={};
  if(d.f==='on'){r.on=!!v;body.on=r.on}else if(d.f==='min'){r.min=Math.max(0,Math.min(100,+v||0));body.min=r.min}else{r.severity=v;body.severity=v}
  saveQuiet(()=>api('PUT',`/api/rules/${r.id}`,body));if(S.view==='controls')ctlRefresh();else rerender()};
ACT.ctlYaml=()=>{const p=pipe(S.ui.orchPid);openDrawer('Controls as a file',`<p class="muted">The same settings as a file you can keep in a repository, so a change to them is reviewed like code.</p>${codeBlock(`controls/${p.id}.yml`,genControls(p))}`)};
ACT.ctlRecommend=d=>act(()=>api('POST',`/api/pipelines/${S.ui.orchPid}/recommend`,{layer:d.layer}),r=>r.added?`${r.added} recommended ${cap(d.layer)} checks added`:`${cap(d.layer)} already has the recommended checks`);
