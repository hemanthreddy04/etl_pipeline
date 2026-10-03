/* ---------- Layers: Bronze, Silver, Gold ---------- */
const LAYER_INFO={
 bronze:{lead:'Raw data, exactly as it arrived.',rules:['Append or reload only. Nothing is cleaned, renamed or filtered.','Every source column stays text, so a bad value can never break a load.','Lineage columns record when each row arrived, from which file, and in which run.','New source columns are accepted here. What to do with them is decided in Silver.']},
 silver:{lead:'Clean, typed and deduplicated. One trusted row per business key.',rules:['Types are cast here. A value that cannot be cast is quarantined or stops the run, by your choice.','Duplicates are removed by keeping the newest record for each key.','Writes are upserts or history-keeping SCD Type 2, so a rerun never doubles data.','Personal data is hashed before anyone downstream can read it.']},
 gold:{lead:'Shaped for the business: facts, dimensions and aggregates.',rules:['Modelled around questions people ask, with a declared grain for every table.','Rebuilt from Silver on every run.','With the publish gate on, a new version replaces the old one only after its checks pass.','This is the only layer dashboards and analysts should read.']}};
function fullName(t){const p=pipe(t.pid),c=p&&conn(p.target),pre=S.settings.prefix||'';if(!c)return t.id;
  return c.type==='bq'?`${c.endpoint||'(service project)'}.${pre}${t.layer}.${t.name}`:c.type==='adb'?`${(c.options||{}).catalog||'main'}.${pre}${t.layer}.${t.name}`:`${pre}${t.layer}.${t.name}`}
function layerView(layer){return {nav:cap(layer),render(){
  const tabs=scopedTables().filter(t=>t.layer===layer),info=LAYER_INFO[layer];let sel=tbl(S.ui.layerTable[layer]);if(!sel||!tabs.includes(sel)){sel=tabs[0];S.ui.layerTable[layer]=sel?sel.id:null}
  const tab=S.ui.layerTab||'schema',drift=layer==='bronze'?S.drift.filter(d=>d.status==='pending'&&tabs.some(t=>t.id===d.table)):[];
  let detail='';
  if(sel){const writer=pipe(sel.pid),wrote=S.runs.filter(r=>r.pid===sel.pid&&r.status==='success'&&r.rows[layer]>0).slice(0,8);
    let body;
    if(tab==='schema')body=sel.cols.length?`<div class="scroll-x"><table class="tbl"><thead><tr><th>Column</th><th>Type</th><th>Role</th></tr></thead><tbody>${sel.cols.map(c=>`<tr><td class="mono">${esc(c.name)}</td><td class="mono">${esc(c.type)}</td><td><div class="tags">${c.key?'<span class="chip">Key</span>':''}${c.pii?`<span class="chip pii">Personal data${c.mask==='hash'?', hashed':''}</span>`:''}${/^_/.test(c.name)?'<span class="chip">Lineage</span>':''}</div></td></tr>`).join('')}</tbody></table></div>${sel.built?'':'<p class="muted small" style="padding:8px 14px">These are the planned columns. The real schema appears after the first run.</p>'}`
      :'<div class="empty">The columns of this table are known after the first run.</div>';
    else if(tab==='props')body=`<div class="body"><dl class="kv"><dt>Full name</dt><dd class="mono">${esc(fullName(sel))}</dd><dt>Format</dt><dd>${esc(sel.fmt)}</dd><dt>Load pattern</dt><dd>${esc(sel.pattern)}</dd><dt>Rows</dt><dd class="num">${sel.built?num(sel.rows):'Not built yet'}</dd><dt>Reads from</dt><dd class="mono">${sel.up.map(u=>esc(u.startsWith('src:')?(writer?writer.source+' / '+writer.path:u):u)).join(', ')}</dd><dt>Written by</dt><dd>${writer?`<button class="linkish" data-act="openPipe" data-id="${writer.id}">${esc(writer.name)}</button>`:'No pipeline'}</dd><dt>Last written</dt><dd class="num">${sel.fresh?stamp(sel.fresh)+', '+ago(sel.fresh):'Never'}</dd></dl></div>`;
    else if(tab==='preview'){const c=remote(`prev:${sel.id}:${sel.fresh}`,()=>api('GET',`/api/tables/${sel.id}/preview`));
      body=remoteHtml(c,d=>d.rows.length?`<div class="scroll-x"><table class="tbl"><thead><tr>${d.columns.map(x=>`<th>${esc(x)}</th>`).join('')}</tr></thead><tbody>${d.rows.map(r=>`<tr>${d.columns.map(x=>`<td class="mono" style="white-space:nowrap;max-width:260px;overflow:hidden;text-overflow:ellipsis" title="${esc(r[x])}">${r[x]==null?'<span class="muted">null</span>':esc(r[x])}</td>`).join('')}</tr>`).join('')}</tbody></table></div><p class="muted small" style="padding:8px 14px">The first ${d.rows.length} rows, read from the warehouse just now.</p>`:`<div class="empty">${esc(d.note||'The table is empty.')}</div>`)}
    else body=`<div class="scroll-x"><table class="tbl"><thead><tr><th>Run</th><th>Written</th><th>Trigger</th><th class="r">Rows from this run</th></tr></thead><tbody>${wrote.map(r=>`<tr class="click" tabindex="0" data-act="openRun" data-id="${r.id}"><td class="mono">${r.id}</td><td class="num">${stamp(r.end||r.start)}</td><td>${esc(r.trigger)}</td><td class="r num">${num(r.rows[layer])}</td></tr>`).join('')||'<tr><td colspan="4" class="empty">No run has written this table yet.</td></tr>'}</tbody></table></div>`;
    detail=`<div class="panel"><header><h2 class="mono">${esc(sel.id)}</h2>${plat(sel.platform)}<div class="tags">${sel.tags.map(t=>`<span class="chip${t==='pii'?' pii':''}">${esc(t)}</span>`).join('')}</div></header>
      <div class="tabs" role="tablist">${[['schema','Schema'],['props','Properties'],['preview','Preview'],['history','Writes']].map(([k,l])=>`<button role="tab" aria-selected="${tab===k}" data-act="layerTab" data-tab="${k}">${l}</button>`).join('')}</div>${body}</div>`}
  return head(cap(layer)+' layer',info.lead,`<button class="btn" data-act="refreshTables">Refresh from warehouse</button>`+(layer==='silver'?`<button class="btn" data-act="go" data-view="mappings">Edit mappings</button>`:layer==='gold'?`<button class="btn" data-act="go" data-view="lineage">View lineage</button>`:''))+
  `<div class="layer-band ${layer}">${disc(layer)}<div><h2>What belongs in ${cap(layer)}</h2><ul>${info.rules.map(r=>`<li>${r}</li>`).join('')}</ul></div></div>
  ${drift.map(d=>`<div class="banner"><div style="flex:1"><b>New column in the source: ${esc(d.col)}</b><br>${esc(d.table)} started receiving <span class="mono">${esc(d.col)}</span> ${ago(d.seen)}. Bronze took it in. Silver ignores it until you map it.</div><button class="btn sm" data-act="drift" data-table="${esc(d.table)}" data-col="${esc(d.col)}" data-action="map">Add to Silver</button><button class="btn sm" data-act="drift" data-table="${esc(d.table)}" data-col="${esc(d.col)}" data-action="ignore">Keep in Bronze only</button></div>`).join('')}
  <div class="panel"><header><h2>Tables</h2><span class="muted small">${tabs.length} in scope, ${fmt(tabs.reduce((a,t)=>a+t.rows,0))} rows</span></header><div class="scroll-x"><table class="tbl"><thead><tr><th>Table</th><th>Engine</th><th class="r">Rows</th><th>Last written</th><th>Load pattern</th></tr></thead><tbody>
    ${tabs.map(t=>`<tr class="click${sel===t?' sel':''}" tabindex="0" data-act="layerSel" data-layer="${layer}" data-id="${t.id}"><td class="mono"><b>${esc(t.name)}</b></td><td>${plat(t.platform)}</td><td class="r num">${t.built?num(t.rows):''}</td><td class="num">${t.fresh?ago(t.fresh):'Not built yet'}</td><td>${esc(t.pattern)}</td></tr>`).join('')||'<tr><td colspan="5" class="empty">No tables in this layer yet. They appear when a pipeline is created.</td></tr>'}
  </tbody></table></div></div>${detail}`;
}}}
['bronze','silver','gold'].forEach(l=>VIEWS[l]=layerView(l));
ACT.layerSel=d=>{S.ui.layerTable[d.layer]=d.id;if(d.go)go(d.layer);else render()};
ACT.layerTab=d=>{S.ui.layerTab=d.tab;render()};
ACT.refreshTables=()=>act(()=>api('POST','/api/tables/refresh',{}),'Row counts and schemas were read again from the warehouse').then(()=>{S.cache={}});
ACT.drift=d=>act(()=>api('POST','/api/drift',{table:d.table,col:d.col,action:d.action}),d.action==='map'?`${d.col} was added to the Silver mapping and is filled from the next run on`:`${d.col} stays in Bronze only`);

/* ---------- Mappings ---------- */
const TX=['trim','lower','upper','initcap','cast','digits only'];
function mapRows(id){const d=S.ui.mapDraft;return d&&d.id===id?d.rows:S.mappings[id].rows}
function mapDraft(id){if(!S.ui.mapDraft||S.ui.mapDraft.id!==id)S.ui.mapDraft={id,rows:JSON.parse(JSON.stringify(S.mappings[id].rows))};return S.ui.mapDraft.rows}
VIEWS.mappings={nav:'Mappings',render(){
  const ids=Object.keys(S.mappings).filter(id=>{const t=tbl(id);return t&&inScope(t.platform)});let id=S.ui.mapTable;if(!ids.includes(id)){id=ids[0];S.ui.mapTable=id}
  if(!id)return head('Source to target mappings','A mapping appears here for each pipeline you create.',`<button class="btn primary" data-act="go" data-view="builder">New pipeline</button>`);
  const m=S.mappings[id],rows=mapRows(id),t=tbl(id),p=pipe(t.pid),dirty=!!(S.ui.mapDraft&&S.ui.mapDraft.id===id),errs=dirty?mapErrors(id):[];
  const opt=(list,v)=>(list.includes(v)?list:[v,...list]).map(x=>`<option ${x===v?'selected':''}>${esc(x)}</option>`).join('');
  return head('Source to target mappings','Each row says how one Bronze column becomes one Silver column. This is the transformation the Silver task runs.',
    `<button class="btn" data-act="mapAdd">Add column</button>${dirty?`<button class="btn" data-act="mapDiscard">Discard changes</button>`:''}<button class="btn primary" data-act="mapSave" ${dirty&&!errs.length?'':'disabled'}>Save mapping</button>`)+
  `<div class="panel"><header><label class="field" style="flex-direction:row;align-items:center;gap:8px"><span>Target table</span><select id="map-table" data-change="mapTable">${ids.map(x=>`<option ${x===id?'selected':''}>${esc(x)}</option>`).join('')}</select></label><span class="muted small">Source: <span class="mono">${esc(m.source)}</span></span>${plat(t.platform)}${dirty?st('pending','Unsaved changes'):''}</header>
  <div class="scroll-x"><table class="tbl"><thead><tr><th>Source column</th><th>Target column</th><th>Target type</th><th>Transformation</th><th>Required</th><th>Hash</th><th></th></tr></thead><tbody>
  ${rows.map((r,i)=>`<tr><td><input type="text" id="map-${i}-src" class="mono" value="${esc(r.src)}" data-change="mapEdit" data-i="${i}" data-f="src" aria-label="Source column"></td>
    <td><input type="text" id="map-${i}-tgt" class="mono" value="${esc(r.tgt)}" data-change="mapEdit" data-i="${i}" data-f="tgt" aria-label="Target column"></td>
    <td><select id="map-${i}-ttype" data-change="mapEdit" data-i="${i}" data-f="ttype" aria-label="Target type">${opt(TYPES,r.ttype)}</select></td>
    <td><input type="text" id="map-${i}-tx" class="mono" list="tx-list" value="${esc(r.tx)}" data-change="mapEdit" data-i="${i}" data-f="tx" aria-label="Transformation"></td>
    <td><input type="checkbox" id="map-${i}-req" ${r.nullable?'':'checked'} data-change="mapEdit" data-i="${i}" data-f="req" aria-label="Required"></td>
    <td><input type="checkbox" id="map-${i}-pii" ${r.pii?'checked':''} data-change="mapEdit" data-i="${i}" data-f="pii" aria-label="Hash as personal data"></td>
    <td class="r"><button class="btn sm" data-act="mapDel" data-i="${i}" ${p.keys.includes(r.tgt)?'disabled title="The business key cannot be removed"':''}>Remove</button></td></tr>`).join('')}
  </tbody></table></div><datalist id="tx-list">${TX.map(x=>`<option value="${x}">`).join('')}</datalist></div>
  ${errs.length?`<div class="banner"><div><b>${errs.length} thing${errs.length>1?'s':''} to fix before saving</b><br>${errs.map(esc).join('<br>')}</div></div>`:''}
  <p class="muted small">Transformations take a preset (${TX.join(', ')}) or any SQL expression over the Bronze columns, for example <code>on_hand - reserved</code>. A saved mapping applies from the next run. Use Full reload on the pipeline to apply it to rows that are already in Silver. Marking a column required adds a not-null check for it.</p>`;
}};
function mapErrors(id){const rows=mapRows(id),m=S.mappings[id],src=tbl(m.source),errs=[],seen={};
  rows.forEach((r,i)=>{if(!r.tgt)errs.push(`Row ${i+1} has no target column name.`);else if(/[^A-Za-z0-9_]/.test(r.tgt)||/^\d/.test(r.tgt))errs.push(`Target column ${r.tgt} may only use letters, digits and underscores.`);
    if(seen[r.tgt])errs.push(`Target column ${r.tgt} is mapped twice.`);seen[r.tgt]=1;
    if(TX.includes(r.tx)||!r.tx){if(!r.src)errs.push(`Row ${i+1} has no source column.`);else if(src&&src.built&&!src.cols.some(c=>c.name===r.src))errs.push(`Source column ${r.src} is not in ${m.source}.`)}});
  return errs}
CHG.mapTable=v=>{S.ui.mapTable=v;S.ui.mapDraft=null;render()};
CHG.mapEdit=(v,d)=>{const rows=mapDraft(S.ui.mapTable),r=rows[+d.i];if(d.f==='req')r.nullable=!v;else if(d.f==='pii')r.pii=v;else r[d.f]=String(v).trim();rerender()};
ACT.mapAdd=()=>{const rows=mapDraft(S.ui.mapTable);rows.push({src:'',stype:'STRING',tgt:'new_column',ttype:'STRING',tx:'trim',nullable:true,pii:false});render();const el=$(`#map-${rows.length-1}-src`);if(el)el.focus()};
ACT.mapDel=d=>{mapDraft(S.ui.mapTable).splice(+d.i,1);render()};
ACT.mapDiscard=()=>{S.ui.mapDraft=null;render()};
ACT.mapSave=()=>{const id=S.ui.mapTable;act(()=>api('PUT',`/api/mappings/${id}`,{rows:mapRows(id)}),'Mapping saved. It applies from the next run.').then(r=>{if(r){S.ui.mapDraft=null;render()}})};

/* ---------- Data quality ---------- */
const ruleState=r=>r.on===false?'disabled':!r.checked?'idle':r.pass>=(r.min==null?100:r.min)?'pass':r.severity==='fail'?'fail':'warn';
const RULE_TYPES=Object.keys(DIM);
const RULE_HELP={not_null:'Leave the rule detail empty.',unique:'Leave the rule detail empty.',range:'Two numbers, such as 0 to 100000, or "at least 0".',accepted_values:'The allowed values, separated by commas.',regex:'A regular expression the value must match.',
  castable:'On a Bronze table: the type the text must convert to, such as DATE. On Silver the type comes from the mapping.',referential:'The column that must contain the value, as layer.table.column.',freshness:'The limit, such as "newer than 2 hours".',
  row_count:'The minimum, such as "at least 1 row".',volume_anomaly:'The allowed swing, such as "within 30% of the recent average". Needs three earlier runs.',reconciliation:'On Gold: "within 0.1% of SUM(amount) in Silver". On Silver: "within 5%" of the rows that arrived in Bronze.',
  schema:'Fails when a column appears that nobody has decided on.',custom_sql:'A SQL condition every row must satisfy, such as quantity > 0 AND price >= 0.'};
VIEWS.quality={nav:'Data quality',render(){
  const tabs=scopedTables(),ids=new Set(tabs.map(t=>t.id)),rules=S.rules.filter(r=>ids.has(r.table)),on=rules.filter(r=>r.on!==false&&r.checked);
  const score=rs=>rs.length?rs.reduce((a,r)=>a+r.pass,0)/rs.length:null;
  const dims=[...new Set(Object.values(DIM))].map(d=>[d,score(on.filter(r=>DIM[r.type]===d)),on.filter(r=>DIM[r.type]===d).length]).filter(x=>x[1]!=null);
  const grade=v=>st(v>=99.9?'pass':v>=99?'warn':'fail',v>=99.9?'Healthy':v>=99?'Watch':'Failing');
  return head('Data quality','Every check with its latest result. Checks run inside each pipeline run, and you can evaluate them against the tables as they are now.',
    `<button class="btn" data-act="go" data-view="controls">Controls per pipeline</button><button class="btn" data-act="dqAdd">Add check</button><button class="btn primary" data-act="dqRun">Run all checks now</button>`)+
  `<div class="cols split">
    <div class="panel"><header><h2>Score by layer</h2></header><table class="tbl"><tbody>${['bronze','silver','gold'].map(l=>{const rs=on.filter(r=>r.table.startsWith(l)),v=score(rs);return `<tr><td>${layerChip(l)}</td><td class="muted">${rules.filter(r=>r.table.startsWith(l)).length} checks</td><td class="r num"><b>${v==null?'':v.toFixed(2)+'%'}</b></td><td class="r">${v==null?st('idle','No results yet'):grade(v)}</td></tr>`}).join('')}</tbody></table></div>
    <div class="panel"><header><h2>Score by dimension</h2></header>${dims.length?`<table class="tbl"><tbody>${dims.map(([d,v,n])=>`<tr><td><b>${d}</b></td><td class="muted">${n} checks</td><td class="r num"><b>${v.toFixed(2)}%</b></td><td class="r">${grade(v)}</td></tr>`).join('')}</tbody></table>`:'<div class="empty">Scores appear once checks have run.</div>'}</div>
  </div>
  <div class="panel"><header><h2>Checks</h2><span class="muted small">${rules.filter(r=>ruleState(r)==='pass').length} passing, ${rules.filter(r=>ruleState(r)==='warn').length} warning, ${rules.filter(r=>ruleState(r)==='fail').length} failing</span></header><div class="scroll-x"><table class="tbl"><thead><tr><th>On</th><th>Table</th><th>Column</th><th>Check</th><th>Rule</th><th>On failure</th><th class="r">Pass rate</th><th class="r">Failed</th><th>Last run</th><th>State</th><th></th></tr></thead><tbody>
    ${rules.map(r=>`<tr><td><label class="switch"><input type="checkbox" id="q-on-${r.id}" ${r.on!==false?'checked':''} data-change="ruleSet" data-id="${r.id}" data-f="on" aria-label="Run this check"><i></i></label></td><td class="mono">${esc(r.table)}</td><td class="mono">${esc(r.column)}</td><td>${esc(r.type.replace(/_/g,' '))}<div class="muted small">${DIM[r.type]||''}</div></td><td class="muted" style="max-width:260px;overflow-wrap:break-word">${esc(r.param)}</td>
      <td><select id="sev-${r.id}" data-change="ruleSet" data-id="${r.id}" data-f="severity" aria-label="On failure">${ACTIONS.map(([k,l])=>`<option value="${k}" ${r.severity===k?'selected':''}>${l}</option>`).join('')}</select></td>
      <td class="r num">${r.checked?r.pass.toFixed(2)+'%':''}</td><td class="r num">${r.checked?num(r.failed):''}</td><td class="num">${r.checked?ago(r.last):'Not run yet'}</td><td>${st(ruleState(r))}</td><td class="r"><button class="btn sm" data-act="dqDel" data-id="${r.id}" aria-label="Remove check">Remove</button></td></tr>`).join('')||'<tr><td colspan="11" class="empty">No checks yet. Each pipeline starts with a few, and you can add more under Controls.</td></tr>'}
  </tbody></table></div></div>
  <div class="panel"><header><h2>Quarantine</h2><span class="muted small">Rows held back from Silver, with the reason</span></header><div class="scroll-x"><table class="tbl"><thead><tr><th>Table</th><th class="r">Rows</th><th>Main reason</th><th>Stored in</th><th>Run</th><th>When</th><th></th></tr></thead><tbody>
    ${S.quarantine.filter(q=>ids.has(q.table)).map(q=>`<tr><td class="mono">${esc(q.table)}</td><td class="r num">${num(q.rows)}</td><td>${esc(q.reason)}</td><td class="mono small" style="overflow-wrap:anywhere">${esc(q.path)}</td><td class="mono">${esc(q.run)}</td><td class="num">${ago(q.ts)}</td><td class="r" style="white-space:nowrap"><button class="btn sm" data-act="qView" data-table="${esc(q.table)}" data-run="${esc(q.run)}">View rows</button> <button class="btn sm" data-act="qDrop" data-table="${esc(q.table)}" data-run="${esc(q.run)}">Discard</button></td></tr>`).join('')||'<tr><td colspan="7" class="empty">Nothing in quarantine.</td></tr>'}
  </tbody></table></div></div>`;
}};
ACT.dqDel=d=>act(()=>api('DELETE',`/api/rules/${d.id}`,{}),'Check removed');
ACT.dqRun=()=>act(()=>api('POST','/api/rules/run',{}),r=>`${r.evaluated} checks evaluated on the current tables, ${r.failing} failing${r.skipped?', '+r.skipped+' not applicable yet':''}`);
ACT.dqAdd=d=>{const pp=d&&d.pid?pipe(d.pid):null;const tabs=pp?pp.writes.map(tbl).filter(t=>t&&t.layer===d.layer):scopedTables();if(!tabs.length){toast('There are no tables to check yet. Create a pipeline first.','warn');return}
  openDrawer('Add a check',`<form data-form="dqSave">
  <label class="field"><span>Table</span><select id="dq-table" name="table">${tabs.map(t=>`<option>${esc(t.id)}</option>`).join('')}</select></label>
  <label class="field"><span>Column</span><input type="text" id="dq-column" name="column" placeholder="customer_id, or * for the whole table" required></label>
  <label class="field"><span>Check type</span><select id="dq-type" name="type" data-change="dqType">${RULE_TYPES.map(t=>`<option value="${t}">${t.replace(/_/g,' ')} (${DIM[t]})</option>`).join('')}</select></label>
  <label class="field"><span>Rule detail</span><input type="text" id="dq-param" name="param"><small id="dq-help">${RULE_HELP.not_null}</small></label>
  <label class="field"><span>On failure</span><select id="dq-sev" name="severity">${ACTIONS.map(([k,l])=>`<option value="${k}" ${k==='quarantine'?'selected':''}>${l}</option>`).join('')}</select><small>Rows can only be quarantined by row-level checks on a Silver table. Elsewhere that choice behaves as a warning.</small></label>
  <div><button class="btn primary">Add check</button></div></form>`)};
CHG.dqType=v=>{$('#dq-help').textContent=RULE_HELP[v]||''};
FORMS.dqSave=f=>act(()=>api('POST','/api/rules',{table:f.table,column:f.column.trim(),type:f.type,param:f.param.trim(),severity:f.severity}),'Check added. It runs with the next pipeline run.').then(r=>{if(r)closeDrawer()});
ACT.qView=d=>{openDrawer(`Quarantined rows, ${d.run}`,'<div class="empty">Loading from the warehouse</div>');api('GET',`/api/quarantine/${d.table}/${d.run}`).then(res=>{
  const cols=res.rows.length?Object.keys(res.rows[0].row):[];$('#drawer-body').innerHTML=res.rows.length?`<p class="muted">Up to 50 rows, exactly as they arrived in Bronze, with the reason each was held back.</p><div class="scroll-x"><table class="tbl"><thead><tr><th>Reason</th>${cols.map(c=>`<th>${esc(c)}</th>`).join('')}</tr></thead><tbody>${res.rows.map(r=>`<tr><td>${esc(r.reason)}</td>${cols.map(c=>`<td class="mono" style="white-space:nowrap">${r.row[c]==null?'<span class="muted">null</span>':esc(r.row[c])}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`:'<div class="empty">No rows are stored for this run any more.</div>'},e=>{$('#drawer-body').innerHTML=`<div class="banner">${esc(e.message)}</div>`})};
ACT.qDrop=d=>act(()=>api('DELETE',`/api/quarantine/${d.table}/${d.run}`,{}),'Quarantined rows discarded');
