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

/* ---------- boot: load state from the service, then keep it fresh ---------- */
(function boot(){
  $('#q').setAttribute('data-input','search');
  const h=(location.hash||'').slice(1);S.view=VIEWS[h]?h:'overview';
  render();
  const loop=async()=>{
    try{await sync()}
    catch(e){if(!S.ready&&!/token/.test(e.message))$('#view').innerHTML=`<div class="banner">The service cannot be reached: ${esc(e.message)}</div>`}
    const busy=S.runs.some(r=>r.status==='running')||S.ui.testing;
    setTimeout(loop,busy?1200:6000)};
  loop();
})();
