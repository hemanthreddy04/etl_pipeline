/* ---------- global search ---------- */
INP.search=v=>{const q=v.trim().toLowerCase(),box=$('#qres');if(q.length<2){box.hidden=true;return}
  const out=[];
  Object.keys(VIEWS).forEach(id=>{if(VIEWS[id].nav.toLowerCase().includes(q))out.push(['Screen',VIEWS[id].nav,'view',id])});
  S.tables.filter(t=>t.id.toLowerCase().includes(q)||t.cols.some(c=>c.name.toLowerCase()===q)).slice(0,6).forEach(t=>out.push(['Table',t.id,'table',t.id]));
  S.pipelines.filter(p=>p.id.toLowerCase().includes(q)).forEach(p=>out.push(['Pipeline',p.id,'pipe',p.id]));
  S.connections.filter(c=>c.id.toLowerCase().includes(q)).slice(0,4).forEach(c=>out.push(['Connection',c.id,'view','connections']));
  CONCEPTS.filter(c=>c[0].toLowerCase().includes(q)).slice(0,5).forEach(c=>out.push(['Concept',c[0],'concept',c[0]]));
  box.innerHTML=out.slice(0,12).map(r=>`<button data-act="searchGo" data-kind="${r[2]}" data-id="${esc(r[3])}"><span class="k">${r[0]}</span><span>${esc(r[1])}</span></button>`).join('')||'<div class="empty">Nothing matches.</div>';box.hidden=false};
ACT.searchGo=d=>{$('#qres').hidden=true;$('#q').value='';
  if(d.kind==='view')go(d.id);else if(d.kind==='pipe'){S.ui.orchPid=d.id;go('orchestration')}
  else if(d.kind==='table'){const t=tbl(d.id);S.ui.layerTable[t.layer]=t.id;go(t.layer)}
  else if(d.kind==='concept'){S.ui.conceptCat='All';S.ui.conceptText=d.id;go('concepts')}};

/* ---------- boot: start the SQL engine, load state, then keep it fresh ---------- */
(function boot(){
  $('#q').setAttribute('data-input','search');
  const h=(location.hash||'').slice(1);S.view=VIEWS[h]?h:'overview';
  render();
  const fail=msg=>{const c=$('#live-chip');c.className='st bad';c.textContent='Not running';$('#view').innerHTML=`<div class="banner"><div>${esc(msg)}</div></div>`};
  let ls=null;try{ls=window.localStorage;ls.getItem('medallion-probe')}catch(e){ls=null}
  const start=()=>initSqlJs().then(SQL=>{
    const info=BE.init({SQL,storage:ls});
    BE.onChange=()=>{sync().catch(()=>{})};
    const loop=async()=>{
      try{await sync()}catch(e){if(!S.ready)fail('This copy could not start: '+e.message)}
      const busy=S.runs.some(r=>r.status==='running')||S.ui.testing;
      setTimeout(loop,busy?1200:6000)};
    loop();
    if(info.restored)toast('Your work from last time was restored from this browser');
    if(info.note)toast(info.note,'warn');
  },e=>fail('The SQL engine could not start: '+(e&&e.message||e)));
  if(typeof initSqlJs==='function'){start();return}
  /* the engine file next to the page did not load: try the same build from the public CDN before giving up */
  const missing='The SQL engine did not load, so this copy cannot run. Reload the page to try again.';
  const s=document.createElement('script');s.src='https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.10.3/sql-asm.js';
  s.onload=()=>{if(typeof initSqlJs==='function')start();else fail(missing)};s.onerror=()=>fail(missing);document.head.appendChild(s);
})();
