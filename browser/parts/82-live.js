/* ---------- Only in this copy: the landing areas hold files uploaded in the browser ---------- */
const kb=n=>n<1024?n+' B':n<1048576?(n/1024).toFixed(1)+' KB':(n/1048576).toFixed(1)+' MB';
function filesTable(cid,files){
  if(!files.length)return '<div class="empty">No files yet. Upload a CSV or JSON file above.</div>';
  return `<div class="scroll-x"><table class="tbl"><thead><tr><th>File</th><th class="r">Size</th><th>Uploaded</th><th></th></tr></thead><tbody>${files.map(f=>`<tr><td class="mono" style="overflow-wrap:anywhere">${esc(f.name)}</td><td class="r num">${kb(f.size)}</td><td class="num">${ago(f.modified)}</td><td class="r"><button class="btn sm" data-act="filesRemove" data-id="${esc(cid)}" data-name="${esc(f.name)}">Remove</button></td></tr>`).join('')}</tbody></table></div>`}
async function filesRefresh(cid){const el=$('#files-list');if(!el)return;
  try{const r=await api('GET',`/api/local/files/${cid}`);el.innerHTML=filesTable(cid,r.files)}catch(e){el.innerHTML=`<div class="banner">${esc(e.message)}</div>`}}
const dropFileCache=()=>Object.keys(S.cache).filter(k=>k.startsWith('lfiles:')).forEach(k=>delete S.cache[k]);
function folderHints(b){
  if(b.kind!=='files'||!b.conn||(conn(b.conn)||{}).type!=='folder')return '';
  const c=remote('lfiles:'+b.conn,()=>api('GET',`/api/local/files/${b.conn}`));if(!c.data)return '';
  const count={};c.data.files.forEach(f=>{const i=f.name.lastIndexOf('/'),d=i>0?f.name.slice(0,i):'';if(d)count[d]=(count[d]||0)+1});
  const ks=Object.keys(count).sort();
  return ks.length?' Folders with files: '+ks.map(k=>`<button type="button" class="linkish" data-act="bFolder" data-folder="${esc(k)}">${esc(k)}</button> (${count[k]})`).join(', ')+'.':' Nothing is uploaded here yet.'}
ACT.bFolder=d=>{const b=S.builder;if(!b)return;b.path=d.folder;if(!b.name)b.name=d.folder.split('/').pop().replace(/[^a-z0-9_]/gi,'_').toLowerCase();S.ui.plan=null;render()};
ACT.filesOpen=d=>{const cid=d.id,folder=d.folder||S.ui.upFolder||'';
  openDrawer('Files in '+cid,`<p class="muted" style="margin-bottom:12px">A pipeline loads every CSV or JSON file under the folder named in its Source step. Each file is loaded once, and a file you upload again under the same name is loaded again.</p>
  <form data-form="filesUpload" style="display:flex;flex-direction:column;gap:12px"><input type="hidden" name="conn" value="${esc(cid)}">
    <label class="field"><span>Folder</span><input type="text" id="up-folder" name="folder" value="${esc(folder)}" placeholder="orders" required autocomplete="off"><small>One folder per dataset, for example <span class="mono">orders</span>. The first row of a CSV file must hold the column names.</small></label>
    <label class="field"><span>CSV or JSON files from your device</span><input type="file" id="up-files" multiple accept=".csv,.json,.jsonl,.ndjson,text/csv,application/json,text/plain"></label>
    <details><summary>Or paste the data instead</summary><div style="display:flex;flex-direction:column;gap:10px;margin-top:10px">
      <label class="field"><span>File name</span><input type="text" id="up-name" name="pname" placeholder="2026-10-03.csv" autocomplete="off"></label>
      <label class="field"><span>Content</span><textarea id="up-text" name="ptext" rows="7" spellcheck="false" placeholder="order_id,customer_email,amount,order_date&#10;1001,ava@example.com,49.90,2026-10-01"></textarea></label></div></details>
    <div><button class="btn primary">Upload</button></div></form>
  <h3 style="margin:18px 0 8px">In this landing area</h3><div id="files-list"><div class="empty">Listing files</div></div>`);
  filesRefresh(cid)};
const readText=file=>new Promise((ok,no)=>{const r=new FileReader();r.onload=()=>ok(String(r.result));r.onerror=()=>no(new Error(`${file.name} could not be read`));r.readAsText(file)});
FORMS.filesUpload=async(f,form)=>{
  const cid=f.conn,folder=(f.folder||'').trim(),picked=[...(form.querySelector('#up-files').files||[])],items=[],btn=form.querySelector('button.primary');
  if(!picked.length&&!(f.ptext||'').trim()){toast('Choose a file, or paste the data','warn');return}
  btn.disabled=true;
  try{
    for(const file of picked)items.push({name:file.name,text:await readText(file)});
    if((f.ptext||'').trim()){let name=(f.pname||'').trim()||'pasted-'+dstr(Date.now())+'.csv';if(!/\.(csv|json|jsonl|ndjson)$/i.test(name))name+=/^\s*[\[{]/.test(f.ptext)?'.json':'.csv';items.push({name,text:f.ptext})}
    let done=0;const problems=[];
    for(const it of items){try{await api('POST',`/api/local/files/${cid}`,{folder,name:it.name,text:it.text});done++}catch(e){problems.push(e.message)}}
    S.ui.upFolder=folder;form.querySelector('#up-files').value='';const ta=form.querySelector('#up-text');if(ta&&done)ta.value='';
    if(done&&S.builder&&S.builder.conn===cid&&!S.builder.path){S.builder.path=folder;S.ui.plan=null}
    dropFileCache();await sync(true).catch(()=>{});filesRefresh(cid);if(S.view==='builder')render();
    if(done)toast(`${done} file${done===1?'':'s'} uploaded to ${cid}/${folder}. A pipeline reading that folder loads ${done===1?'it':'them'} on its next run.`);
    problems.forEach(p=>toast(p,'bad'));
  }catch(e){toast(e.message,'bad')}
  btn.disabled=false};
ACT.filesRemove=async d=>{try{await api('DELETE',`/api/local/files/${d.id}`,{name:d.name});dropFileCache();await sync(true).catch(()=>{});filesRefresh(d.id);toast(`${d.name} removed. Rows already loaded from it stay in Bronze.`)}catch(e){toast(e.message,'bad')}};

/* ---------- Only in this copy: what works here, what is kept, and starting over ---------- */
function aboutHtml(){const i=BE.info,kept=i.keeps==='everything'?'Connections, pipelines, uploaded files and table contents':i.keeps==='settings and files'?'Connections, pipelines and uploaded files, but not the table contents':'Nothing yet';
  return `<p>This is the site from your repository with its service running inside the page. The pages, the generated SQL, the checks, the controls and the run engine are the same code paths, answered by SQLite in your browser instead of a server.</p>
  <h3 style="margin:16px 0 6px">Works here</h3>
  <ul class="plain"><li>Upload CSV or JSON files, or read from a table another pipeline built</li><li>Build a pipeline and see the Bronze, Silver and Gold SQL before deploying</li><li>Run it: the SQL executes on your data, with checks, quarantine, the circuit breaker and the Gold publish gate</li><li>Mappings, masking, SCD Type 1 and 2, schema changes, retries, erasure, lineage, the tools</li><li>Schedules, while this page is open</li></ul>
  <h3 style="margin:16px 0 6px">Needs the deployed service</h3>
  <ul class="plain"><li>BigQuery, Cloud Storage, Azure Databricks and ADLS</li><li>The MCP endpoint for outside agents, and the built-in agent</li><li>Alerts by email, chat or pager</li><li>Schedules that run while nobody has the page open</li><li>Downloading the code export as a zip</li></ul>
  <h3 style="margin:16px 0 6px">What is kept</h3>
  <dl class="kv"><dt>Saved in this browser</dt><dd>${kept}${i.saved?', '+ago(i.saved):''}</dd><dt>Leaves your device</dt><dd>Nothing. The files you upload stay in this page.</dd><dt>Size</dt><dd>Meant for files up to a few megabytes</dd></dl>
  ${i.note?`<div class="banner" style="margin-top:10px"><div>${esc(i.note)}</div></div>`:''}
  <h3 style="margin:16px 0 6px">Start over</h3><p class="muted">Deletes every pipeline, table and uploaded file in this copy and puts back the starting connections and the sample file.</p>
  <div style="margin-top:8px"><button class="btn ${S.ui.confirmReset?'danger':''}" data-act="liveReset">${S.ui.confirmReset?'Confirm: delete everything in this copy':'Start over'}</button></div>`}
ACT.liveAbout=()=>{S.ui.confirmReset=false;openDrawer('What works in this copy',aboutHtml())};
ACT.liveReset=()=>{if(!S.ui.confirmReset){S.ui.confirmReset=true;$('#drawer-body').innerHTML=aboutHtml();return}
  S.ui.confirmReset=false;
  try{BE.reset();S.cache={};S.builder=null;S.ui.plan=null;S.ui.orchPid=null;S.ui.layerTable={};S.ui.mapDraft=null;S.ui.mapTable=null;closeDrawer();sync(true).then(()=>{go('overview');toast('This copy is back to its starting state')})}
  catch(e){toast(e.message,'bad')}};
