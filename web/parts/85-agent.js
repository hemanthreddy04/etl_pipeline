/* ---------- Agent ---------- */
const SESSION='s'+Math.random().toString(36).slice(2,10);
function agentHints(){const h=[],e=engines()[0],p=S.pipelines[0];
  if(e)h.push(`Look at my connections and tell me what I can build`);
  if(p){h.push(`Why did the last run of ${p.id} fail?`);h.push(`Which checks does ${p.id} have, and what is missing?`);h.push(`Run ${p.id}`)}
  else if(e)h.push('Create a pipeline for the files in my landing folder');
  return h}
function agentMsg(m){
  if(m.role==='user')return `<div class="msg user"><div class="who">You</div>${esc(m.text)}</div>`;
  if(m.role==='tool')return `<details class="msg tool"><summary>${m.ok?'Called':'Failed'} ${esc(m.tool)}</summary><pre>${esc('args   '+JSON.stringify(m.args)+'\nresult '+JSON.stringify(m.result,null,1).slice(0,1500))}</pre></details>`;
  if(m.role==='approval')return `<div class="msg"><div class="who">Agent asks to change something</div><b class="mono">${esc(m.tool)}</b><pre class="mono small" style="white-space:pre-wrap;margin:6px 0">${esc(JSON.stringify(m.args,null,1))}</pre>${m.done?`<span class="muted small">${m.done}</span>`:`<div style="display:flex;gap:6px"><button class="btn sm primary" data-act="agentApprove" data-ok="1">Approve</button><button class="btn sm" data-act="agentApprove">Reject</button></div>`}</div>`;
  if(m.role==='error')return `<div class="msg" style="border-color:var(--bad)"><div class="who">Problem</div>${esc(m.text)}</div>`;
  const d=document.createElement('div');d.textContent=m.text;return `<div class="msg"><div class="who">Agent</div><div style="white-space:pre-wrap">${d.innerHTML}</div></div>`}
function agentRender(){const el=$('#agentlog');if(!el)return;el.innerHTML=S.agent.msgs.map(agentMsg).join('')+(S.agent.busy?'<div class="msg"><span class="st run">Working</span></div>':'')||'<div class="empty">Ask for something below.</div>';el.scrollTop=el.scrollHeight}
VIEWS.agent={nav:'Agent',render(){
  if(!S.settings.agent)return head('Agent','An agent that designs, runs and explains your pipelines through the same tools the MCP endpoint offers.')+
    `<div class="banner info"><div><b>The agent is switched off.</b> It uses the Claude API, so the service needs an API key. Set <code>ANTHROPIC_API_KEY</code> on the service and restart it. The model can be chosen with <code>AGENT_MODEL</code> (now <span class="mono">${esc(S.settings.agentModel||'')}</span>).</div></div>
    <div class="panel"><header><h2>Without the built-in agent</h2></header><div class="body"><p>Any MCP client can already do the same work. Connect it to this service's MCP endpoint and it gets the same tools, with the same approval step for changes.</p><div style="margin-top:10px"><button class="btn" data-act="go" data-view="mcp">Open MCP and tools</button></div></div></div>`;
  return head('Agent','Say what you want in plain words. The agent looks at your real connections, tables and runs through its tools, and waits for your approval before it changes anything.',`<button class="btn" data-act="go" data-view="mcp">Tools and approvals</button><button class="btn" data-act="agentClear">Clear conversation</button>`)+
  `<div class="panel"><div class="chat" id="agentlog" data-keep="agent" aria-live="polite"></div>
    <div class="body" style="border-top:1px solid var(--line);display:flex;flex-direction:column;gap:10px">
      <div class="suggest">${agentHints().map(h=>`<button data-act="agentHint">${esc(h)}</button>`).join('')}</div>
      <form data-form="agentAsk" style="display:flex;gap:8px"><input type="text" id="agent-input" name="q" placeholder="Ask the agent to build, run, check or explain" autocomplete="off" style="flex:1" aria-label="Message to the agent"><button class="btn primary">Send</button></form>
      <p class="muted small">Model: <span class="mono">${esc(S.settings.agentModel||'')}</span>. What you type and the tool results are sent to the Claude API.</p>
    </div></div>`;
},after(){agentRender()}};
ACT.agentHint=(d,el)=>{const i=$('#agent-input');i.value=el.textContent;i.focus()};
ACT.agentClear=()=>{api('POST','/api/agent/reset',{session:SESSION}).catch(()=>{});S.agent={msgs:[],busy:false,pending:false};render()};
async function agentCall(path,body){S.agent.busy=true;agentRender();
  try{const r=await api('POST',path,{session:SESSION,...body});r.events.forEach(e=>S.agent.msgs.push(e));S.agent.pending=r.events.some(e=>e.role==='approval')}
  catch(e){S.agent.msgs.push({role:'error',text:e.message})}
  S.agent.busy=false;agentRender();LAST='';sync().catch(()=>{})}
FORMS.agentAsk=(f,form)=>{const q=(f.q||'').trim();if(!q||S.agent.busy)return;if(S.agent.pending){toast('Approve or reject the waiting change first','warn');return}form.reset();S.agent.msgs.push({role:'user',text:q});agentCall('/api/agent',{message:q})};
ACT.agentApprove=d=>{if(S.agent.busy)return;const m=[...S.agent.msgs].reverse().find(x=>x.role==='approval'&&!x.done);if(m)m.done=d.ok==='1'?'Approved':'Rejected';S.agent.pending=false;agentCall('/api/agent/approve',{ok:d.ok==='1'})};
