/* ---------- the systems this service can connect to ---------- */
const CONN_TYPES=[
 {id:'bq',name:'BigQuery',cloud:'gcp',role:'warehouse',field:'Project id',ph:'my-sandbox-project',auth:['Service account of this service'],opts:[['location','Location','US']],optional:true,
  hint:'Leave the project empty to use the project this service runs in. The service account needs BigQuery Data Editor and BigQuery Job User.'},
 {id:'gcs',name:'Cloud Storage bucket',cloud:'gcp',role:'storage',field:'Bucket URL',ph:'gs://my-landing-bucket',auth:['Service account of this service'],
  hint:'Files are read by BigQuery. The service account needs Storage Object Viewer on the bucket.'},
 {id:'adb',name:'Azure Databricks',cloud:'azure',role:'warehouse',field:'Workspace URL',ph:'https://adb-1234567890123456.7.azuredatabricks.net',auth:['Access token read from a secret'],secret:true,
  opts:[['warehouse','SQL warehouse id',''],['catalog','Unity Catalog name','main']],hint:'Statements run on the SQL warehouse. The token needs USE CATALOG, CREATE SCHEMA and CREATE TABLE on the catalog.'},
 {id:'adls',name:'ADLS or cloud storage path',cloud:'azure',role:'storage',field:'Storage URL',ph:'abfss://landing@account.dfs.core.windows.net',auth:['Read through the Databricks warehouse'],
  hint:'The path must be an external location, or a volume path, that the SQL warehouse is allowed to read.'},
 {id:'local',name:'Local SQLite lake',cloud:'local',role:'warehouse',field:'Folder for the database files',ph:'./data/lake',auth:['None'],hint:'A zero-setup engine for trying a pipeline before pointing it at a cloud warehouse.'},
 {id:'folder',name:'Local folder',cloud:'local',role:'storage',field:'Folder path',ph:'./samples/landing',auth:['None'],hint:'A folder on the machine that runs this service. CSV and JSON files.'}
];
const ctype=id=>CONN_TYPES.find(t=>t.id===id)||CONN_TYPES[0];
const FILE_FOR={local:'folder',bq:'gcs',adb:'adls'};
const engines=()=>S.connections.filter(c=>ctype(c.type).role==='warehouse');
const TYPES=['STRING','INT','BIGINT','DECIMAL(18,2)','DOUBLE','BOOLEAN','DATE','TIMESTAMP'];
const DIM={not_null:'Completeness',unique:'Uniqueness',range:'Validity',accepted_values:'Validity',regex:'Validity',castable:'Validity',referential:'Consistency',freshness:'Timeliness',row_count:'Completeness',volume_anomaly:'Completeness',reconciliation:'Accuracy',schema:'Consistency',custom_sql:'Consistency'};
const ROW_RULES=['not_null','castable','range','accepted_values','regex','referential','custom_sql'];

/* ---------- runs ---------- */
function lastRun(pid){return S.runs.find(r=>r.pid===pid)}
function runTasks(run){
  const p=pipe(run.pid);if(!p)return [];const end=run.end||Date.now(),span=Math.max(1,end-run.start);
  return p.tasks.map(t=>{const s=(run.tasks||{})[t.id]||{status:'queued'};const a=s.start?(s.start-run.start)/span*100:0;const b=s.start?((s.end||end)-s.start)/span*100:0;
    return {t,status:s.status,left:Math.min(98,a),width:Math.max(b,s.start?2:0),tries:s.tries||0,secs:s.start?Math.max(0,((s.end||end)-s.start)/1000):0}})}

/* ---------- charts ---------- */
const CHARTS={};
function niceMax(v){if(v<=0)return 1;const p=Math.pow(10,Math.floor(Math.log10(v))),m=v/p;return (m<=1?1:m<=2?2:m<=4?4:m<=8?8:10)*p}
function lineChart(id,label,labels,series,yfmt){
  yfmt=yfmt||fmt;const W=720,H=250,L=46,Rr=92,Tp=14,B=28,max=niceMax(Math.max(1,...series.flatMap(s=>s.values)));
  const n=labels.length,x=i=>L+(W-L-Rr)*(n>1?i/(n-1):0),y=v=>Tp+(H-Tp-B)*(1-v/max);
  let g='';for(let i=0;i<=4;i++){const v=max*i/4,yy=y(v);g+=`<line class="grid" x1="${L}" x2="${W-Rr}" y1="${yy}" y2="${yy}"/><text x="${L-8}" y="${yy+4}" text-anchor="end">${yfmt(v)}</text>`}
  const step=Math.ceil(n/7);labels.forEach((l,i)=>{if((n-1-i)%step===0)g+=`<text x="${x(i)}" y="${H-8}" text-anchor="middle">${esc(l)}</text>`});
  // end labels, pushed apart so they never overlap
  const ends=series.map((s,i)=>({i,y:y(s.values[n-1])})).sort((a,b)=>a.y-b.y);for(let i=1;i<ends.length;i++)if(ends[i].y-ends[i-1].y<14)ends[i].y=ends[i-1].y+14;
  const marks=series.map(s=>`<polyline fill="none" stroke="var(--${s.color})" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" points="${s.values.map((v,i)=>x(i).toFixed(1)+','+y(v).toFixed(1)).join(' ')}"/><circle class="ring" cx="${x(n-1)}" cy="${y(s.values[n-1])}" r="4.5" fill="var(--${s.color})"/>`).join('');
  const lbls=ends.map(e=>`<text class="lbl" x="${x(n-1)+10}" y="${e.y+4}">${esc(series[e.i].name)}</text>`).join('');
  CHARTS[id]={labels,series,x,y,W,H,yfmt,L,Rr,Tp,B};
  return `<div class="legend">${series.map(s=>`<span><i style="background:var(--${s.color})"></i>${esc(s.name)}</span>`).join('')}</div>
  <div class="chart" id="${id}"><svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(label)}">${g}${marks}${lbls}<line class="xhair" x1="0" x2="0" y1="${Tp}" y2="${H-B}" visibility="hidden"/></svg><div class="tip" hidden></div></div>
  <details class="small"><summary>View as table</summary><div class="scroll-x"><table class="tbl"><thead><tr><th>Day</th>${series.map(s=>`<th class="r">${esc(s.name)}</th>`).join('')}</tr></thead><tbody>${labels.map((l,i)=>`<tr><td>${esc(l)}</td>${series.map(s=>`<td class="r num">${num(s.values[i])}</td>`).join('')}</tr>`).join('')}</tbody></table></div></details>`;
}
function attachCharts(){
  for(const id in CHARTS){const box=document.getElementById(id);if(!box){delete CHARTS[id];continue}
    const c=CHARTS[id],svg=box.querySelector('svg'),tip=box.querySelector('.tip'),xh=box.querySelector('.xhair'),n=c.labels.length;
    const move=e=>{const r=svg.getBoundingClientRect(),vx=(e.clientX-r.left)/r.width*c.W;let i=Math.round((vx-c.L)/(c.W-c.L-c.Rr)*(n-1));i=Math.max(0,Math.min(n-1,i));
      xh.setAttribute('x1',c.x(i));xh.setAttribute('x2',c.x(i));xh.setAttribute('visibility','visible');
      tip.textContent='';const b=document.createElement('b');b.textContent=c.labels[i];tip.appendChild(b);
      c.series.forEach(s=>{const row=document.createElement('div');row.className='row';const k=document.createElement('i');k.style.background=`var(--${s.color})`;const nm=document.createElement('span');nm.textContent=s.name;const v=document.createElement('strong');v.textContent=num(s.values[i]);row.append(k,nm,v);tip.appendChild(row)});
      tip.hidden=false;const px=c.x(i)/c.W*r.width;tip.style.left=(px>r.width*0.6?px-tip.offsetWidth-12:px+12)+'px';tip.style.top='8px'};
    svg.addEventListener('pointermove',move);svg.addEventListener('pointerleave',()=>{tip.hidden=true;xh.setAttribute('visibility','hidden')});
  }
}
function hbars(rows,cls,vfmt){const max=Math.max(1,...rows.map(r=>r[1]));return `<div class="bars">${rows.map(r=>`<span>${esc(r[0])}</span><div class="track"><div class="bar ${r[2]||cls||''}" style="width:${(r[1]/max*100).toFixed(1)}%" title="${esc(r[0])}: ${vfmt(r[1])}"></div></div><span class="num">${vfmt(r[1])}</span>`).join('')}</div>`}
function dailySeries(days){
  const day0=Math.floor(Date.now()/DAY)*DAY,labels=[],b=[],s=[],g=[],ok=[],bad=[];
  for(let d=days-1;d>=0;d--){const a=day0-d*DAY,z=a+DAY;const rs=S.runs.filter(r=>r.start>=a&&r.start<z&&r.status!=='running'&&inScope(pipe(r.pid)?pipe(r.pid).platform:'any'));
    labels.push(d===0?'Today':dstr(a).slice(5));b.push(rs.reduce((x,r)=>x+r.rows.bronze,0));s.push(rs.reduce((x,r)=>x+r.rows.silver,0));g.push(rs.reduce((x,r)=>x+r.rows.gold,0));ok.push(rs.filter(r=>r.status==='success').length);bad.push(rs.filter(r=>r.status==='failed').length)}
  return {labels,b,s,g,ok,bad};
}

/* ---------- DAG and lineage drawings ---------- */
function dagSvg(p,run){
  const lvl={};p.tasks.forEach(t=>{lvl[t.id]=t.deps.length?Math.max(...t.deps.map(d=>lvl[d]||0))+1:0});
  const groups=[];p.tasks.forEach(t=>{(groups[lvl[t.id]]=groups[lvl[t.id]]||[]).push(t)});
  const NW=196,NH=42,GX=40,GY=16,rowsMax=Math.max(...groups.map(g=>g.length)),W=16+groups.length*(NW+GX)-GX+16,H=16+rowsMax*(NH+GY)-GY+16,pos={};
  groups.forEach((g,i)=>{const off=(rowsMax-g.length)*(NH+GY)/2;g.forEach((t,j)=>{pos[t.id]={x:16+i*(NW+GX),y:16+off+j*(NH+GY)}})});
  const edges=p.tasks.flatMap(t=>t.deps.filter(d=>pos[d]).map(d=>{const a=pos[d],b=pos[t.id],x1=a.x+NW,y1=a.y+NH/2,x2=b.x-6,y2=b.y+NH/2,mx=(x1+x2)/2;return `<path class="edge" d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}" marker-end="url(#arr)"/>`})).join('');
  const nodes=p.tasks.map(t=>{const q=pos[t.id],stt=run&&run.tasks?run.tasks[t.id]?run.tasks[t.id].status:'idle':run?(runTasks(run).find(x=>x.t.id===t.id)||{}).status:'idle';const lab=t.label.length>23?t.label.slice(0,22)+'…':t.label;
    return `<g class="node" tabindex="0" role="button" data-act="openTask" data-pid="${p.id}" data-task="${t.id}" aria-label="${esc(t.label)}, ${esc((ST[stt]||['',stt])[1])}"><rect x="${q.x}" y="${q.y}" width="${NW}" height="${NH}" rx="5"/><circle class="l-${t.layer}" cx="${q.x+13}" cy="${q.y+NH/2}" r="5"/><text x="${q.x+25}" y="${q.y+18}">${esc(lab)}</text><text class="sub" x="${q.x+25}" y="${q.y+33}">${esc((ST[stt]||['',stt])[1])}</text><circle class="dot-${stt}" cx="${q.x+NW-13}" cy="${q.y+NH/2}" r="4.5"/></g>`}).join('');
  return `<div class="graph scroll-x"><svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="Task graph for ${esc(p.name)}: ${p.tasks.length} tasks in ${groups.length} stages"><defs><marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="currentColor" opacity=".55"/></marker></defs>${edges}${nodes}</svg></div>`;
}
function lineageGraph(){
  const tabs=S.tables.filter(t=>inScope(t.platform));const ids=new Set(tabs.map(t=>t.id));
  const cols=[['Sources','source',S.sources.filter(s=>tabs.some(t=>t.up.includes(s.id))).map(s=>({id:s.id,name:s.name,up:[]}))],
    ['Bronze','bronze',tabs.filter(t=>t.layer==='bronze')],['Silver','silver',tabs.filter(t=>t.layer==='silver')],['Gold','gold',tabs.filter(t=>t.layer==='gold')],
    ['Consumers','consumer',CONSUMERS.filter(c=>c.up.some(u=>ids.has(u)))]];
  const NW=168,NH=30,GX=40,GY=12,rowsMax=Math.max(...cols.map(c=>c[2].length),1),W=12+cols.length*(NW+GX)-GX+12,H=34+rowsMax*(NH+GY)+6,pos={},all=[];
  cols.forEach(([h,layer,items],i)=>items.forEach((it,j)=>{pos[it.id]={x:12+i*(NW+GX),y:34+j*(NH+GY)};all.push({...it,layer:layer,label:it.name||it.id})}));
  // selection: everything upstream and downstream of the chosen node stays lit
  const sel=S.ui.lineageSel&&pos[S.ui.lineageSel]?S.ui.lineageSel:null,lit=new Set();
  if(sel){const upOf=id=>(all.find(a=>a.id===id)||{up:[]}).up||[];const walkUp=id=>{lit.add(id);upOf(id).forEach(u=>{if(!lit.has(u))walkUp(u)})};const down=new Set();const walkDown=id=>{down.add(id);all.filter(a=>(a.up||[]).includes(id)&&!down.has(a.id)).forEach(a=>walkDown(a.id))};walkUp(sel);walkDown(sel);down.forEach(d=>lit.add(d))}
  const edges=all.flatMap(a=>(a.up||[]).filter(u=>pos[u]).map(u=>{const s=pos[u],e=pos[a.id],x1=s.x+NW,y1=s.y+NH/2,x2=e.x,y2=e.y+NH/2,mx=(x1+x2)/2;const dim=sel&&!(lit.has(u)&&lit.has(a.id));return `<path class="edge${dim?' dim':''}" d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}"/>`})).join('');
  const heads=cols.map(([h],i)=>`<text class="colhead" x="${12+i*(NW+GX)}" y="20">${h}</text>`).join('');
  const nodes=all.map(a=>{const q=pos[a.id],lab=a.label.length>22?a.label.slice(0,21)+'…':a.label;return `<g class="node${sel===a.id?' sel':''}${sel&&!lit.has(a.id)?' dim':''}" tabindex="0" role="button" data-act="lineageSel" data-id="${esc(a.id)}" aria-label="${esc(a.label)}"><rect x="${q.x}" y="${q.y}" width="${NW}" height="${NH}" rx="4"/><circle class="l-${a.layer}" cx="${q.x+12}" cy="${q.y+NH/2}" r="4.5"/><text x="${q.x+23}" y="${q.y+19}">${esc(lab)}</text></g>`}).join('');
  return `<div class="graph scroll-x"><svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" role="img" aria-label="Lineage from sources through Bronze, Silver and Gold to consumers">${heads}${edges}${nodes}</svg></div>`;
}
