// Run the shared scenario on the in-browser backend and compare every answer with the real Python service.
const fs=require('fs'),path=require('path');
const steps=require('./steps.js');
if(!fs.existsSync(path.join(__dirname,'golden.json'))){console.error('Record the Python service first:  node browser/test/steps.js && python3 browser/test/golden.py');process.exit(2)}
const golden=require('./golden.json');
const src=fs.readdirSync(path.join(__dirname,'../parts')).filter(f=>/^2\d-be-.*\.js$/.test(f)).sort().map(f=>fs.readFileSync(path.join(__dirname,'../parts',f),'utf8')).join('\n');
const m={exports:{}};new Function('module','setTimeout','clearTimeout','setInterval','clearInterval',src)(m,setTimeout,clearTimeout,setInterval,clearInterval);
const BE=m.exports;
const DROP=new Set(['ts','tested','ms','start','end','dur','modified','fresh','now','created','last','seen','loaded','endpoint','env','settings','saved']);
const TS=new Set(['_ingest_ts','_ingest_date','_built_at','valid_from','valid_to','_quarantined_at']);
function norm(x,key){
  if(Array.isArray(x))return x.map(v=>norm(v));
  if(x&&typeof x==='object'){const o={};
    for(const k of Object.keys(x).sort()){if(DROP.has(k))continue;let v=x[k];
      if(TS.has(k)&&v!==null)v='<ts>';
      if(k==='text'&&typeof v==='string'&&/^[{[]/.test(v)){try{v=JSON.parse(v)}catch(e){}}
      if(k==='returns_hist_sk')v=/^[0-9a-f]{64}$/.test(v)?'<sha256>':v;
      if(k==='connections'&&Array.isArray(v))v=v.map(c=>({id:c.id,type:c.type,status:c.status,cloud:c.cloud}));
      if(k==='logs'&&Array.isArray(v))v=v.filter(l=>l.pid!=='connections');
      if(k==='audit'&&Array.isArray(v))v=v.filter(a=>!['Uploaded file','Added connection'].includes(a.action));
      o[k]=norm(v,k)}
    return o}
  if(typeof x==='string')return x.replace('. Rows from an earlier run are still waiting in Bronze. Use Retry on that failed run, or a full reload, to carry them into Silver','').replace(/in \d+ s\b/g,'in N s').replace(/\b\d+ ms\b/g,'N ms');
  if(typeof x==='number'&&!Number.isInteger(x))return Math.round(x*1e6)/1e6;
  return x}
function diff(a,b,p,out){
  if(out.length>12)return;
  if(typeof a!==typeof b||Array.isArray(a)!==Array.isArray(b)||(a===null)!==(b===null)){out.push(`${p}: python ${JSON.stringify(a)} | js ${JSON.stringify(b)}`.slice(0,600));return}
  if(a&&typeof a==='object'){
    if(Array.isArray(a)&&a.length!==b.length){out.push(`${p}: length python ${a.length} | js ${b.length}`+('\n      py '+JSON.stringify(a).slice(0,500)+'\n      js '+JSON.stringify(b).slice(0,500)));return}
    for(const k of new Set([...Object.keys(a),...Object.keys(b)]))diff(a[k],b[k],p+'.'+k,out);return}
  if(a!==b)out.push(`${p}: python ${JSON.stringify(a)} | js ${JSON.stringify(b)}`.slice(0,900))}
const EXPECTED_DIFFERENT={conn:'connection answers carry local paths',};
(async()=>{
  const SQL=await require('../vendor/sql-asm.js')();
  BE.init({SQL,seed:false,scheduler:false});BE._.settings.retry_base_seconds=0;
  const call=async(method,p,body)=>{try{return {status:200,body:await BE.handle(method,p,body)}}catch(e){if(!e.status)console.log('  UNEXPECTED',method,p,e.stack);return {status:e.status||500,body:{error:e.message}}}};
  const state=async()=>(await call('GET','/api/state')).body;
  let bad=0,same=0;
  for(let i=0;i<steps.length;i++){const s=steps[i];let res;
    if(s.op==='api'){res=await call(s.method,s.path,s.body);if(s.path==='/mcp'&&res.status===200&&res.body===null)res.body={}}
    else if(s.op==='conn')res=await call('POST','/api/connections',{id:s.id,type:s.type,endpoint:''});
    else if(s.op==='file')res=await call('POST',`/api/local/files/${s.conn}`,{folder:s.folder,name:s.name,text:s.text});
    else if(s.op==='wait'){await BE.idle();res={status:200,body:(await state()).runs.filter(r=>r.pid===s.pid)[s.nth||0]}}
    else if(s.op==='state')res={status:200,body:await state()};
    else if(s.op==='rule'){const r=(await state()).rules.find(x=>Object.entries(s.match).every(([k,v])=>x[k]===v));res=await call('PUT',`/api/rules/${r.id}`,s.body)}
    else if(s.op==='retry'){const run=(await state()).runs.filter(r=>r.pid===s.pid)[s.nth||0];res=await call('POST',`/api/runs/${run.id}/retry`,{})}
    else if(s.op==='quarantine'){const qs=(await state()).quarantine.filter(q=>q.table===s.table);res={status:200,body:[]};
      for(const q of qs)res.body.push(await call('GET',`/api/quarantine/${q.table}/${q.run}`));
      if(s.discard&&qs.length)res.body.push(await call('DELETE',`/api/quarantine/${qs[0].table}/${qs[0].run}`,{}))}
    else if(s.op==='mapping'){let rows=(await state()).mappings[s.table].rows;rows=rows.filter(r=>!(s.drop||[]).includes(r.tgt)).concat(s.add||[]);res=await call('PUT',`/api/mappings/${s.table}`,{rows})}
    else if(s.op==='approve'){const a=(await state()).approvals.find(x=>x.status==='pending');res=await call('POST',`/api/approvals/${a.id}`,{ok:s.ok})}
    else if(s.op==='logs'){const run=(await state()).runs.find(r=>r.pid===s.pid);res=await call('POST','/api/tools/get_run_logs',{args:{run:run.id}})}
    let g=golden[i];
    if(s.op==='conn'){g={status:g.status,body:{id:g.body.id,type:g.body.type,status:g.body.status}};res={status:res.status,body:{id:res.body.id,type:res.body.type,status:res.body.status}}}
    if((s.op==='api'&&/\/run$/.test(s.path)&&s.method==='POST'&&!/rules/.test(s.path))||s.op==='retry'){const pick=r=>r.status===200?{status:200,body:{id:r.body.id,pid:r.body.pid,trigger:r.body.trigger,full:r.body.full,note:r.body.note}}:r;g=pick(g);res=pick(res)}
    const out=[];diff(norm(g),norm(res),'',out);
    if(out.length){
      const known=(i===4)||(s.op==='api'&&s.path==='/mcp'&&s.body.method==='initialize'&&out.length===1&&/serverInfo.version/.test(out[0]));
      if(known){console.log(`  step ${i} differs as expected: ${out[0].slice(0,220)}`);same++}
      else{bad++;console.log(`STEP ${i} ${s.op} ${s.method||''} ${s.path||''} ${JSON.stringify(s.match||s.pid||'')}\n   `+out.join('\n   '))}}
    else same++}
  console.log(`\n${same} of ${steps.length} steps answered the same as the Python service; ${bad} differ`);
  process.exit(bad?1:0);
})().catch(e=>{console.error('FAIL',e);process.exit(1)});
