/* ---------- Connections ---------- */
const ABBR={bq:'BQ',gcs:'GCS',adb:'DBX',adls:'ADLS',local:'SQL',folder:'DIR'};
const ROLE_LABEL={warehouse:'Warehouses',storage:'File sources'};
VIEWS.connections={nav:'Connections',render(){
  const cs=S.connections.filter(c=>inScope(c.cloud));
  const tile=c=>{const t=ctype(c.type),o=c.options||{};return `<div class="tile"><header><span class="mono-badge" style="width:auto;min-width:30px;padding:0 6px">${ABBR[c.type]||'?'}</span><h3 class="mono">${esc(c.id)}</h3>${st(S.ui.testing===c.id?'testing':c.status||'idle')}</header>
    <div>${esc(t.name)} ${plat(t.cloud)}</div><div class="mono small">${esc(c.endpoint||'(project of the service)')}</div>
    ${Object.keys(o).length?`<div class="small muted">${Object.entries(o).map(([k,v])=>`${esc(k)}: <span class="mono">${esc(v)}</span>`).join(', ')}</div>`:''}
    ${c.secret?`<div class="small muted">Secret reference: <span class="mono">${esc(c.secret)}</span></div>`:''}
    ${c.note?`<div class="small" style="overflow-wrap:anywhere">${esc(c.note)}</div>`:''}<div class="small muted">${c.tested?'Tested '+ago(c.tested)+(c.ms!=null?', '+c.ms+' ms':''):'Not tested yet'}</div>
    <footer>${c.type==='folder'?`<button class="btn sm primary" data-act="filesOpen" data-id="${c.id}">Files</button>`:''}<button class="btn sm" data-act="connTest" data-id="${c.id}" ${S.ui.testing===c.id?'disabled':''}>Test</button><button class="btn sm" data-act="connEdit" data-id="${c.id}">Edit</button><button class="btn sm ${S.ui.confirmDel===c.id?'danger':''}" data-act="connDel" data-id="${c.id}">${S.ui.confirmDel===c.id?'Confirm remove':'Remove'}</button></footer></div>`};
  return head('Connections','The warehouses that build the tables, and the storage the files come from. A connection holds an address and the name of a secret. It never holds the secret itself.',`<button class="btn primary" data-act="connNew">New connection</button>`)+
  Object.keys(ROLE_LABEL).map(role=>{const list=cs.filter(c=>ctype(c.type).role===role);return list.length?`<section style="display:flex;flex-direction:column;gap:10px"><h2>${ROLE_LABEL[role]} <span class="muted small">${list.length}</span></h2><div class="tile-grid">${list.map(tile).join('')}</div></section>`:''}).join('')+
  (cs.length?'':`<div class="panel"><div class="empty">No connections yet. Start with a local SQLite lake, then add a landing area for the files.</div></div>`)+
  `<div class="panel"><header><h2>What each connection needs</h2></header><div class="scroll-x"><table class="tbl"><thead><tr><th>System</th><th>You provide</th><th>How it signs in</th></tr></thead><tbody>${CONN_TYPES.map(t=>`<tr><td><b>${esc(t.name)}</b><div>${plat(t.cloud)}</div></td><td>${t.noEndpoint?(t.role==='storage'?'The files, uploaded here':'Nothing'):esc(t.field)}${(t.opts||[]).map(o=>', '+esc(o[1].toLowerCase())).join('')}${t.secret?', the name of a secret holding the token':''}</td><td class="muted">${esc(t.hint)}</td></tr>`).join('')}</tbody></table></div></div>`;
}};
ACT.connTest=async d=>{S.ui.testing=d.id;render();try{const c=await api('POST',`/api/connections/${d.id}/test`,{});S.ui.testing=null;await sync(true);toast(c.status==='healthy'?`${c.id} is reachable (${c.ms} ms)`:`${c.id} is not reachable: ${c.note}`,c.status==='healthy'?'ok':'bad')}catch(e){S.ui.testing=null;render();toast(e.message,'bad')}};
ACT.connDel=d=>{if(S.ui.confirmDel!==d.id){S.ui.confirmDel=d.id;render();return}S.ui.confirmDel=null;act(()=>api('DELETE',`/api/connections/${d.id}`,{}),d.id+' removed')};
function connForm(c){const t=ctype(c?c.type:(S.ui.newType||'folder')),o=(c&&c.options)||{},clouds=[['local','In this browser'],['gcp','Google Cloud, on the deployed service'],['azure','Azure, on the deployed service']];
  return `<form data-form="connSave"><input type="hidden" name="orig" value="${c?esc(c.id):''}">
  <label class="field"><span>System</span><select id="cn-type" name="type" data-change="connType" ${c?'disabled':''}>${clouds.map(([k,l])=>`<optgroup label="${l}">${CONN_TYPES.filter(x=>x.cloud===k).map(x=>`<option value="${x.id}" ${x.id===t.id?'selected':''}>${esc(x.name)}</option>`).join('')}</optgroup>`).join('')}</select><small>${esc(t.hint)}</small></label>
  <label class="field"><span>Connection name</span><input type="text" id="cn-id" name="id" value="${c?esc(c.id):''}" placeholder="lowercase_with_underscores" required ${c?'readonly':''}></label>
  ${t.noEndpoint?'':`<label class="field"><span>${esc(t.field)}</span><input type="text" id="cn-endpoint" name="endpoint" value="${c?esc(c.endpoint):''}" placeholder="${esc(t.ph)}" ${t.optional?'':'required'}></label>`}
  ${(t.opts||[]).map(([k,label,dflt])=>`<label class="field"><span>${esc(label)}</span><input type="text" id="cn-opt-${k}" name="opt_${k}" value="${esc(o[k]!=null?o[k]:dflt)}"></label>`).join('')}
  ${t.secret?`<label class="field"><span>Secret reference</span><input type="text" id="cn-secret" name="secret" value="${c?esc(c.secret):''}" placeholder="DATABRICKS_TOKEN" required><small>The name of an environment variable on the service, or of a secret in Google Secret Manager, that holds the token. Do not paste the token itself here.</small></label>`:''}
  <div><button class="btn primary">${c?'Save and test':'Add and test'}</button></div></form>`}
ACT.connNew=()=>{S.ui.newType='folder';openDrawer('New connection',connForm(null))};
ACT.connEdit=d=>openDrawer('Edit '+d.id,connForm(conn(d.id)));
CHG.connType=v=>{S.ui.newType=v;const id=$('#cn-id').value;$('#drawer-body').innerHTML=connForm(null);$('#cn-id').value=id};
FORMS.connSave=async f=>{const type=f.orig?conn(f.orig).type:f.type,options={};Object.keys(f).filter(k=>k.startsWith('opt_')).forEach(k=>options[k.slice(4)]=f[k]);
  const body={id:(f.id||'').trim().toLowerCase().replace(/[^a-z0-9_]/g,'_'),type,endpoint:f.endpoint||'',secret:f.secret||'',options,auth:ctype(type).auth[0]};
  try{const c=await api(f.orig?'PUT':'POST',f.orig?`/api/connections/${f.orig}`:'/api/connections',body);closeDrawer();await sync(true);
    toast(c.status==='healthy'?`${c.id} saved and reachable`:`${c.id} saved, but the test failed: ${c.note}`,c.status==='healthy'?'ok':'bad')}catch(e){toast(e.message,'bad')}};

/* ---------- Compute ---------- */
VIEWS.compute={nav:'Compute',render(){
  const c=remote('compute:'+S.connections.map(x=>x.id+x.status).join(),()=>api('GET','/api/compute'));
  return head('Compute','The engines behind each warehouse connection, read live from the platform.',`<button class="btn" data-act="computeRefresh">Refresh</button>`)+
  `<div class="panel">${remoteHtml(c,d=>{const rows=d.compute.filter(x=>inScope(x.platform));return rows.length?`<div class="scroll-x"><table class="tbl"><thead><tr><th>Name</th><th>Type</th><th>Connection</th><th>State</th><th>Used by</th><th></th></tr></thead><tbody>
    ${rows.map(x=>`<tr><td><b class="mono">${esc(x.name||x.id)}</b>${x.inUse?' <span class="chip">In use by this connection</span>':''}</td><td>${esc(x.kind)}<div class="muted small">${esc(x.spec)}</div></td><td class="mono">${esc(x.conn)} ${plat(x.platform)}</td><td>${st(x.state)}</td><td class="small">${x.used.map(esc).join(', ')||'No pipeline yet'}</td>
      <td class="r">${x.canToggle?`<button class="btn sm" data-act="computeToggle" data-conn="${esc(x.conn)}" data-id="${esc(x.id)}" data-start="${x.state==='running'?'':'1'}" ${/ing$/.test(x.state)&&x.state!=='running'?'disabled':''}>${x.state==='running'?'Stop':'Start'}</button>`:''}</td></tr>`).join('')}
    </tbody></table></div>`:'<div class="empty">No warehouse connection yet. Compute appears here once one is added.</div>'})}</div>
  <div class="panel"><header><h2>What runs where</h2></header><div class="scroll-x"><table class="tbl"><thead><tr><th>Engine</th><th>Compute</th><th>What you manage</th></tr></thead><tbody>
    <tr><td><b>BigQuery</b></td><td>On-demand query slots</td><td>Nothing to start or stop. Cost follows the bytes each statement scans, so partition large Silver and Gold tables.</td></tr>
    <tr><td><b>Azure Databricks</b></td><td>A SQL warehouse</td><td>Start and stop it here. Give it an auto-stop time so it does not run idle between pipeline runs.</td></tr>
    <tr><td><b>Local SQLite</b></td><td>This page</td><td>Nothing. It is meant for trying a pipeline, not for large data.</td></tr>
  </tbody></table></div></div>`;
}};
ACT.computeRefresh=()=>{Object.keys(S.cache).filter(k=>k.startsWith('compute:')).forEach(k=>delete S.cache[k]);render()};
ACT.computeToggle=d=>act(()=>api('POST',`/api/compute/${d.conn}/${d.id}`,{start:d.start==='1'}),d.start==='1'?'Start requested. It takes a few minutes.':'Stop requested').then(()=>setTimeout(ACT.computeRefresh,2500));

/* ---------- MCP ---------- */
VIEWS.mcp={nav:'MCP and tools',render(){
  const c=remote('tools',()=>api('GET','/api/tools')),url='https://<your deployed service>/mcp',pending=S.approvals.filter(a=>a.status==='pending');
  const cfg={mcpServers:{'medallion-control-plane':{type:'http',url,...(S.settings.protected?{headers:{Authorization:'Bearer <your APP_TOKEN>'}}:{})}}};
  return head('MCP and tools','This service is itself an MCP server. An agent that connects to it can inspect and operate your pipelines with the tools below. Tools that only read run freely. Tools that change something wait for your approval unless you switch that off.')+
  `<div class="banner info"><div><b>The endpoint needs the deployed service.</b> An MCP address lives on a server, so no outside agent can connect to this copy. The tools are the same ones, and Try runs them here against your tables.</div></div>`+
  (pending.length?`<div class="panel"><header><h2>Waiting for your approval</h2><span class="muted small">${pending.length}</span></header><div class="list">${pending.map(a=>`<div><div class="grow"><b class="mono">${esc(a.tool)}</b><span class="muted small">Asked by ${esc(a.by)}, ${ago(a.ts)}</span><pre class="mono small" style="white-space:pre-wrap;margin:6px 0 0">${esc(JSON.stringify(a.args,null,1))}</pre></div><button class="btn sm primary" data-act="approval" data-id="${a.id}" data-ok="1">Approve</button><button class="btn sm" data-act="approval" data-id="${a.id}">Reject</button></div>`).join('')}</div></div>`:'')+
  `<div class="cols split"><div class="panel"><header><h2>Endpoint</h2></header><div class="body"><dl class="kv"><dt>URL</dt><dd class="mono">${esc(url)}</dd><dt>Transport</dt><dd>Streamable HTTP</dd><dt>Sign-in</dt><dd>${S.settings.protected?'The access token of this control plane, as a Bearer header':'None. Set APP_TOKEN on the service before exposing this address.'}</dd></dl></div></div>
    <div style="min-width:0">${codeBlock('Client configuration',JSON.stringify(cfg,null,2))}</div></div>
  <div class="panel"><header><h2>Tools</h2></header>${remoteHtml(c,d=>`<div class="scroll-x"><table class="tbl"><thead><tr><th>Tool</th><th>What it does</th><th>Access</th><th>Ask before running</th><th></th></tr></thead><tbody>${d.tools.map(t=>`<tr><td class="mono"><b>${esc(t.name)}</b></td><td>${esc(t.desc)}</td><td>${t.write?'<span class="chip pii">Changes something</span>':'<span class="chip">Read only</span>'}</td>
      <td>${t.write?`<label class="switch"><input type="checkbox" id="tool-ap-${t.name}" ${t.approve?'checked':''} data-change="toolApprove" data-tool="${t.name}" aria-label="Ask before running ${esc(t.name)}"><i></i></label>`:''}</td>
      <td class="r"><button class="btn sm" data-act="toolTry" data-tool="${t.name}">Try</button></td></tr>`).join('')}</tbody></table></div>`)}</div>
  <div class="panel"><header><h2>Decided requests</h2></header><div class="scroll-x"><table class="tbl"><thead><tr><th>When</th><th>Tool</th><th>Asked by</th><th>Outcome</th></tr></thead><tbody>${S.approvals.filter(a=>a.status!=='pending').slice(0,10).map(a=>`<tr><td class="num">${ago(a.ts)}</td><td class="mono">${esc(a.tool)}</td><td>${esc(a.by)}</td><td>${st(a.status==='approved'?'pass':'fail',cap(a.status))}</td></tr>`).join('')||'<tr><td colspan="4" class="empty">No requests yet. They appear when an agent or MCP client asks to change something.</td></tr>'}</tbody></table></div></div>`;
}};
CHG.toolApprove=(v,d)=>act(()=>api('PUT',`/api/tools/${d.tool}`,{approve:v}),v?`${d.tool} now asks first`:`${d.tool} now runs without asking. Keep this for changes you trust agents to make.`).then(()=>{delete S.cache.tools;render()});
ACT.approval=d=>act(()=>api('POST',`/api/approvals/${d.id}`,{ok:d.ok==='1'}),r=>r.status==='approved'?`${r.tool} approved and done`:r.status==='failed'?`${r.tool} was approved but failed: ${(r.result||{}).error||''}`:`${r.tool} rejected`);
ACT.toolTry=d=>{const t=((S.cache.tools||{}).data||{tools:[]}).tools.find(x=>x.name===d.tool);if(!t)return;const props=Object.keys(t.schema.properties||{}),sample={};
  props.forEach(k=>{if((t.schema.required||[]).includes(k))sample[k]=k==='pipeline'?(S.pipelines[0]||{}).id||'':k==='table'?(S.tables[0]||{}).id||'':k==='sql'?'SELECT 1 AS ok':''});
  openDrawer(t.name,`<p>${esc(t.desc)}</p><form data-form="toolRun"><input type="hidden" name="tool" value="${t.name}">
  <label class="field"><span>Arguments (JSON)</span><textarea id="tool-args" name="args" rows="6" spellcheck="false">${esc(JSON.stringify(sample,null,2))}</textarea><small>Accepted: ${props.map(esc).join(', ')||'none'}</small></label>
  ${t.write?`<p class="muted small">This tool changes something. Running it from here is your own action, so it does not wait for approval.</p>`:''}
  <div><button class="btn primary">Call tool</button></div></form><div id="tool-result"></div>`)};
FORMS.toolRun=async f=>{let args;try{args=JSON.parse(f.args||'{}')}catch(e){toast('The arguments are not valid JSON: '+e.message,'warn');return}
  try{const r=await api('POST',`/api/tools/${f.tool}`,{args});$('#tool-result').innerHTML=codeBlock('Result',JSON.stringify(r.result,null,2));LAST='';sync()}catch(e){$('#tool-result').innerHTML=`<div class="banner">${esc(e.message)}</div>`}};
