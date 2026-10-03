'use strict';
/* ---------- helpers ---------- */
const $=(s,r=document)=>r.querySelector(s);
const $$=(s,r=document)=>[...r.querySelectorAll(s)];
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const MIN=60e3,HOUR=3600e3,DAY=86400e3;
const fmt=n=>{n=Number(n)||0;const a=Math.abs(n);return a>=1e9?(n/1e9).toFixed(2)+'B':a>=1e6?(n/1e6).toFixed(1)+'M':a>=1e4?(n/1e3).toFixed(0)+'K':a>=1e3?(n/1e3).toFixed(1)+'K':String(Math.round(n))};
const num=n=>Math.round(Number(n)||0).toLocaleString('en-US');
const usd=n=>'$'+(Number(n)||0).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
const dur=s=>{s=Math.max(0,Math.round(s));return s<60?s+'s':s<3600?Math.floor(s/60)+'m '+String(s%60).padStart(2,'0')+'s':Math.floor(s/3600)+'h '+String(Math.floor(s%3600/60)).padStart(2,'0')+'m'};
const ago=ts=>{const d=Date.now()-ts;if(d<0){const f=-d;return f<HOUR?'in '+Math.max(1,Math.round(f/MIN))+' min':f<DAY?'in '+Math.round(f/HOUR)+' h':'in '+Math.round(f/DAY)+' d'}
  return d<MIN?'just now':d<HOUR?Math.round(d/MIN)+' min ago':d<DAY?Math.round(d/HOUR)+' h ago':Math.round(d/DAY)+' d ago'};
const iso=ts=>new Date(ts).toISOString();
const dstr=ts=>iso(ts).slice(0,10);
const clock=ts=>iso(ts).slice(11,19);
const stamp=ts=>iso(ts).slice(0,16).replace('T',' ')+' UTC';
const cap=s=>s.charAt(0).toUpperCase()+s.slice(1);

/* ---------- state: a cache of what the service holds ---------- */
const S={view:'overview',platform:'all',env:'dev',settings:{env:'dev',channels:{},agent:false,protected:true},
  connections:[],tables:[],pipelines:[],sources:[],runs:[],logs:[],alerts:[],rules:[],quarantine:[],audit:[],approvals:[],roles:[],mappings:{},drift:[],
  cache:{},ready:false,
  ui:{layerTable:{},orchPid:null,lineageSel:null,mapTable:null,govTab:'access',logLevel:'all',logPid:'all',logText:'',conceptCat:'All',conceptText:'',monRange:14},
  builder:null,agent:{msgs:[],busy:false,pending:false}};
const CONSUMERS=[];

/* ---------- small renderers ---------- */
const ST={success:['ok','Succeeded'],running:['run','Running'],failed:['bad','Failed'],queued:['','Queued'],retry:['run','Retrying'],upstream_failed:['warn','Upstream failed'],skipped:['','Skipped'],
  healthy:['ok','Healthy'],degraded:['warn','Degraded'],down:['bad','Unreachable'],active:['ok','Active'],paused:['warn','Paused'],pass:['ok','Pass'],warn:['warn','Warning'],fail:['bad','Fail'],
  terminated:['','Stopped'],starting:['run','Starting'],stopping:['run','Stopping'],enabled:['ok','Enabled'],disabled:['','Disabled'],idle:['','Not run yet'],testing:['run','Testing'],
  open:['bad','Open'],ack:['','Acknowledged'],pending:['warn','Pending'],deployed:['ok','Deployed']};
const st=(s,label)=>{const d=ST[s]||['',s];return `<span class="st ${d[0]}">${esc(label||d[1])}</span>`};
const plat=p=>p==='azure'?'<span class="chip az">Azure</span>':p==='gcp'?'<span class="chip gcp">GCP</span>':'<span class="chip">Local</span>';
const disc=l=>`<i class="disc disc-${l}" aria-hidden="true"></i>`;
const layerChip=l=>`<span class="chip">${disc(l)}${cap(l)}</span>`;
const head=(title,sub,actions='')=>`<div class="ph"><div><h1>${title}</h1><p>${sub}</p></div><div class="ph-act">${actions}</div></div>`;
const inScope=p=>S.platform==='all'||p===S.platform||p==='any';
const pipe=id=>S.pipelines.find(p=>p.id===id);
const tbl=id=>S.tables.find(t=>t.id===id);
const conn=id=>S.connections.find(c=>c.id===id);

/* ---------- toast, drawer, audit, log ---------- */
function toast(msg,kind='ok'){const el=document.createElement('div');el.className='toast '+(kind==='ok'?'':kind);const s=document.createElement('span');s.textContent=msg;el.appendChild(s);$('#toasts').appendChild(el);setTimeout(()=>el.remove(),4200)}
function openDrawer(title,html){$('#drawer-title').textContent=title;$('#drawer-body').innerHTML=html;$('#drawer').hidden=false;$('#scrim').hidden=false;const f=$('#drawer-body input,#drawer-body select,#drawer-body textarea,#drawer-body button');if(f)f.focus()}
function closeDrawer(){$('#drawer').hidden=true;if(!$('#rail').classList.contains('open'))$('#scrim').hidden=true;S.ui.drawerRun=null}

/* ---------- talking to the service ---------- */
function token(){try{return sessionStorage.getItem('mcp-token')||''}catch(e){return S.ui.token||''}}
function setToken(v){S.ui.token=v;try{sessionStorage.setItem('mcp-token',v)}catch(e){}}
async function api(method,path,body){
  const chip=$('#live-chip');let r;
  try{r=await fetch(path,{method,headers:{'content-type':'application/json',...(token()?{authorization:'Bearer '+token()}:{})},body:body===undefined?undefined:JSON.stringify(body)})}
  catch(e){chip.className='st bad';chip.textContent='Offline';throw new Error('The service cannot be reached')}
  chip.className='st ok';chip.textContent='Live';
  if(r.status===401){askToken();throw new Error('Enter the access token to continue')}
  const json=(r.headers.get('content-type')||'').includes('json');const data=json?await r.json():await r.blob();
  if(!r.ok)throw new Error(json&&data&&data.error?data.error:`The service answered ${r.status}`);
  return data}
/* run a change, then pull fresh state; errors become a message instead of a crash */
async function act(fn,done){try{const out=await fn();await sync(true);if(done)toast(typeof done==='function'?done(out):done);return out}catch(e){toast(e.message,'bad');return null}}
let LAST='';
async function sync(force){
  const d=await api('GET','/api/state');const sig=JSON.stringify({...d,now:0});
  if(sig===LAST&&!force&&S.ready)return false;LAST=sig;
  Object.assign(S,{settings:d.settings,env:d.settings.env,connections:d.connections,pipelines:d.pipelines,tables:d.tables,sources:d.sources,runs:d.runs,logs:d.logs,alerts:d.alerts,rules:d.rules,
    quarantine:d.quarantine,mappings:d.mappings,drift:d.drift,audit:d.audit,approvals:d.approvals,roles:d.roles,ready:true});
  $('#env-chip').textContent=S.env;refresh();if(S.ui.drawerRun&&!$('#drawer').hidden)ACT.openRun({id:S.ui.drawerRun,keep:'1'});return true}
/* data that is fetched on demand (previews, compute, tools): render "loading", fetch once, render again */
function remote(key,loader){const c=S.cache[key];if(c)return c;S.cache[key]={loading:true};
  loader().then(d=>{S.cache[key]={data:d}},e=>{S.cache[key]={error:e.message}}).finally(()=>refresh());return S.cache[key]}
function remoteHtml(c,fn){return c.loading?'<div class="empty">Loading from the service</div>':c.error?`<div class="banner">${esc(c.error)}</div>`:fn(c.data)}
function askToken(){if($('#token-form'))return;openDrawer('Access token',`<p class="muted">This control plane is protected. Enter the access token that was set when the service was deployed.</p><form data-form="saveToken" id="token-form"><label class="field"><span>Access token</span><input type="password" id="tk" name="token" autocomplete="off" required></label><div><button class="btn primary">Continue</button></div></form>`)}
function copyText(text,btn){const done=()=>{if(btn){const o=btn.textContent;btn.textContent='Copied';setTimeout(()=>btn.textContent=o,1400)}};
  try{navigator.clipboard.writeText(text).then(done,()=>fallbackCopy(text,done))}catch(e){fallbackCopy(text,done)}}
function fallbackCopy(text,done){const ta=document.createElement('textarea');ta.value=text;document.body.appendChild(ta);ta.select();try{document.execCommand('copy');done()}catch(e){toast('Copy is blocked here. Select the text and copy it manually.','warn')}ta.remove()}

/* ---------- code block with light highlighting ---------- */
const KW='def|import|from|return|if|else|elif|for|in|as|with|raise|not|and|or|None|True|False|lambda|SELECT|FROM|WHERE|GROUP|BY|ORDER|AS|ON|AND|OR|NOT|NULL|IS|JOIN|LEFT|INNER|MERGE|USING|WHEN|MATCHED|THEN|UPDATE|SET|INSERT|INTO|VALUES|ROW|CREATE|REPLACE|TABLE|PARTITION|CLUSTER|ALL|CAST|CASE|END|ELSE|DISTINCT|UNION|OVER|QUALIFY|TRUE|FALSE|true|false|null';
const HL=new RegExp('(#.*$|--\\s.*$)|("(?:[^"\\\\\\n]|\\\\.)*"|\'(?:[^\'\\\\\\n]|\\\\.)*\')|\\b('+KW+')\\b|\\b(\\d+(?:\\.\\d+)?)\\b','gm');
function hl(src){let out='',i=0;src.replace(HL,(m,c,s,k,n,idx)=>{out+=esc(src.slice(i,idx));out+=`<span class="${c?'com':s?'str':k?'kw':'nm'}">${esc(m)}</span>`;i=idx+m.length;return m});return out+esc(src.slice(i))}
const CODE_STORE={};
function codeBlock(file,src){const id='c'+Object.keys(CODE_STORE).length;CODE_STORE[id]=src;return `<div class="code"><div class="code-head"><span>${esc(file)}</span><button data-act="copyCode" data-id="${id}">Copy</button></div><pre tabindex="0"><code>${hl(src)}</code></pre></div>`}

/* ---------- routing and rendering ---------- */
const VIEWS={},ACT={},CHG={},INP={},FORMS={};
const NAV=[['Operate',['overview','pipelines','orchestration','monitoring']],['Layers',['bronze','silver','gold']],['Build',['builder','controls','mappings','quality']],
  ['Platform',['connections','compute','mcp','agent']],['Govern',['lineage','governance','cicd']],['Learn',['concepts']]];
FORMS.saveToken=f=>{setToken(f.token.trim());closeDrawer();sync(true).then(()=>toast('Signed in'),e=>toast(e.message,'bad'))};
function renderNav(){
  const open=S.alerts.filter(a=>!a.ack).length;
  const counts={pipelines:S.pipelines.filter(p=>inScope(p.platform)).length,connections:S.connections.filter(c=>inScope(c.cloud)).length,mcp:S.approvals.filter(a=>a.status==='pending').length||undefined,quality:S.rules.length,
    bronze:S.tables.filter(t=>t.layer==='bronze'&&inScope(t.platform)).length,silver:S.tables.filter(t=>t.layer==='silver'&&inScope(t.platform)).length,gold:S.tables.filter(t=>t.layer==='gold'&&inScope(t.platform)).length};
  $('#nav').innerHTML=NAV.map(([g,ids])=>`<div class="nav-group"><h6>${g}</h6>${ids.map(id=>{const v=VIEWS[id];const lay=['bronze','silver','gold'].includes(id)?disc(id):'';
    const c=id==='overview'&&open?`<span class="cnt hot" title="Open alerts">${open}</span>`:counts[id]!=null?`<span class="cnt">${counts[id]}</span>`:'';
    return `<a href="#${id}" data-act="go" data-view="${id}" ${S.view===id?'aria-current="page"':''}>${lay}${v.nav}${c}</a>`}).join('')}</div>`).join('');
}
function render(){for(const k in CODE_STORE)delete CODE_STORE[k];renderNav();const v=VIEWS[S.view];if(!S.ready){$('#view').innerHTML='<div class="empty">Connecting to the service</div>';return}$('#view').innerHTML=v.render();if(v.after)v.after()}
function rerender(){setTimeout(()=>{const a=document.activeElement,id=a&&a.id;render();const e=id&&document.getElementById(id);if(e)e.focus()},0)}
function refresh(){const a=document.activeElement;if(a&&$('#view').contains(a)&&/INPUT|TEXTAREA|SELECT/.test(a.tagName))return;const c=$('#content'),sc=c.scrollTop;const inner=$$('#view [data-keep]').map(e=>[e.dataset.keep,e.scrollTop]);render();inner.forEach(([k,t])=>{const e=$(`#view [data-keep="${k}"]`);if(e)e.scrollTop=t});c.scrollTop=sc}
function go(view,silent){if(!VIEWS[view])view='overview';S.view=view;$('#qres').hidden=true;if(!silent){try{history.replaceState(null,'','#'+view)}catch(e){try{location.hash=view}catch(_){}}}render();$('#content').scrollTop=0;closeRail()}
function closeRail(){$('#rail').classList.remove('open');if($('#drawer').hidden)$('#scrim').hidden=true}
ACT.go=d=>go(d.view);
ACT.toggleRail=()=>{const r=$('#rail');r.classList.toggle('open');$('#scrim').hidden=!r.classList.contains('open')};
ACT.closeDrawer=()=>closeDrawer();
ACT.closeAll=()=>{$('#drawer').hidden=true;$('#rail').classList.remove('open');$('#scrim').hidden=true;S.ui.drawerRun=null};
ACT.copyCode=(d,el)=>copyText(CODE_STORE[d.id]||el.closest('.code').querySelector('code').textContent,el);
CHG.setPlatform=v=>{S.platform=v;render()};

document.addEventListener('click',e=>{
  const t=e.target.closest('[data-act]');
  if(!e.target.closest('.search'))$('#qres').hidden=true;
  if(!t)return;
  if(e.target.closest('input,select,textarea,label')&&!t.matches('input,select,textarea,label,button'))return;
  if(t.tagName==='A')e.preventDefault();
  const fn=ACT[t.dataset.act];if(fn)fn(t.dataset,t,e);
});
document.addEventListener('change',e=>{const t=e.target.closest('[data-change]');if(t&&CHG[t.dataset.change])CHG[t.dataset.change](t.type==='checkbox'?t.checked:t.value,t.dataset,t)});
document.addEventListener('input',e=>{const t=e.target.closest('[data-input]');if(t&&INP[t.dataset.input])INP[t.dataset.input](t.value,t.dataset,t)});
document.addEventListener('submit',e=>{e.preventDefault();const f=e.target.closest('[data-form]');if(f&&FORMS[f.dataset.form])FORMS[f.dataset.form](Object.fromEntries(new FormData(f)),f)});
document.addEventListener('keydown',e=>{
  if(e.key==='Escape'){ACT.closeAll();$('#qres').hidden=true;return}
  if((e.key==='Enter'||e.key===' ')&&e.target.matches&&e.target.matches('[data-act]:not(button):not(a):not(input):not(select):not(textarea)')){e.preventDefault();e.target.dispatchEvent(new MouseEvent('click',{bubbles:true}))}
});
window.addEventListener('hashchange',()=>{const h=location.hash.slice(1);if(h&&h!==S.view&&VIEWS[h])go(h,true)});
