// One scenario, run twice: against the real Python service and against the in-browser backend. The results must match.
const fs=require('fs'),path=require('path');
const SAMPLE=fs.readFileSync(path.join(__dirname,'../../samples/landing/vendor_returns/2026-10-01.csv'),'utf8');
const DAY2=`return_id,order_id,customer_email,reason,channel,refund_amount,return_date,updated_at,carrier
R1001,O501,ava.keller@example.com,damaged,web,55.00,2026-09-30,2026-10-02 08:00:00,dhl
R1012,O512,eli.novak@example.com,wrong size,app,31.40,2026-10-02,2026-10-02 09:00:00,ups
R1013,O513,ana.silva@example.com,"changed mind, twice",phone,12.00,2026-10-02,2026-10-02 09:30:00,
R1014,O514,max.berg@example.com,damaged,web,250.00,2026-10-02,2026-10-02 10:00:00,dhl
`;
const DAY3=`return_id,order_id,customer_email,reason,channel,refund_amount,return_date,updated_at,carrier,notes
R1002,O502,noah.singh@example.com,wrong size,app,125.00,2026-09-30,2026-10-03 08:05:00,ups,"said ""too small"""
R1015,O515,ida.holm@example.com,late delivery,store,44.00,2026-10-03,2026-10-03 09:00:00,dhl,
`;
const EVENTS=`{"event_id": 1, "kind": "click", "value": "3.5", "at": "2026-10-01T10:00:00Z", "meta": {"a": 1}}
{"event_id": 2, "kind": "view", "value": "oops", "at": "2026-10-01T10:05:00", "flag": true}
{"event_id": 3, "kind": "Click ", "value": 7, "at": "2026-10-01 10:06", "flag": false}
{"event_id": 3, "kind": "click", "value": null, "at": "2026-13-01 10:06:00"}
`;
const A={name:'vendor_returns',target:'local_lake',conn:'landing',kind:'files',format:'csv',path:'vendor_returns',pattern:'incremental',
  columns:'return_id:STRING:key\norder_id:STRING\ncustomer_email:STRING:pii\nreason:STRING\nchannel:STRING\nrefund_amount:DECIMAL(18,2)\nreturn_date:DATE\nupdated_at:TIMESTAMP',
  onBad:'quarantine',scd:'1',maskPii:true,cron:'0 * * * *',retries:1,
  gold:{name:'agg_returns_daily',type:'aggregate',dateCol:'return_date',dims:'reason, channel',measures:'COUNT(*) AS returns\nSUM(refund_amount) AS refund_total,'}};
const B={name:'Returns Hist',target:'local_lake',kind:'table',path:'silver.vendor_returns',pattern:'incremental',watermark:'updated_at',
  columns:'return_id:STRING:key\nchannel:STRING\nrefund_amount:DECIMAL(18,2)\nupdated_at:TIMESTAMP\nreason:STRING',scd:'2',onBad:'quarantine',cron:'*/15 * * * *',retries:0,
  gold:{name:'dim_returns',type:'dimension'}};
const C={name:'events',target:'local_lake',conn:'landing',kind:'files',format:'json',path:'events/raw',pattern:'full',evolve:'failOnNewColumns',
  columns:'event_id:BIGINT:key\nkind:STRING\nvalue:DOUBLE\nat:TIMESTAMP\nflag:BOOLEAN\nmeta:STRING',onBad:'stop',scd:'1',maskPii:false,cron:'5 4 * * 1-5',retries:2,
  gold:{name:'events_by_kind',type:'sql',sql:'SELECT e.kind, COUNT(*) AS n, MAX(g.returns) AS most_returns\nFROM {silver} e\nCROSS JOIN {gold.agg_returns_daily} g\nGROUP BY e.kind;'}};
const D={name:'broken gold',target:'local_lake',kind:'table',path:'silver.vendor_returns',pattern:'full',columns:'return_id:STRING:key\nrefund_amount:DECIMAL(18,2)',
  retries:1,gold:{name:'agg_broken',type:'aggregate',measures:'SUM(nope) AS x'}};
const PA='vendor_returns_medallion',PB='returns_hist_medallion',PC='events_medallion',PD='broken_gold_medallion';
const api=(method,p,body)=>({op:'api',method,path:p,body});
const sql=q=>api('POST','/api/tools/run_sql',{args:{sql:q}});
const run=(pid,full)=>[api('POST',`/api/pipelines/${pid}/run`,full?{full:true}:{}),{op:'wait',pid}];
const state={op:'state'};
const SILVER='SELECT return_id, order_id, customer_email, reason, channel, refund_amount, return_date, updated_at FROM silver.vendor_returns ORDER BY return_id';
const steps=[
  {op:'conn',id:'local_lake',type:'local'},{op:'conn',id:'landing',type:'folder'},
  {op:'file',conn:'landing',folder:'vendor_returns',name:'2026-10-01.csv',text:SAMPLE},
  api('POST','/api/files',{connection:'landing',path:'vendor_returns'}),
  api('POST','/api/files',{connection:'landing',path:'nothing_here'}),
  api('POST','/api/files',{connection:'landing',path:'vendor_returns',format:'parquet'}),
  api('POST','/api/columns',{cfg:{...A,columns:''}}),
  api('POST','/api/plan',{cfg:A}),
  api('POST','/api/plan',{cfg:{...A,columns:''}}),
  api('POST','/api/plan',{cfg:{...A,cron:'every hour'}}),
  api('POST','/api/plan',{cfg:{...A,gold:{...A.gold,dims:'reason; drop'}}}),
  api('POST','/api/pipelines',{cfg:A}),api('POST','/api/pipelines',{cfg:A}),
  api('POST','/api/pipelines',{cfg:{...A,name:'other',gold:A.gold}}),
  state,
  ...run(PA),state,                                                   // stops on the missing key
  {op:'rule',match:{table:'silver.vendor_returns',type:'not_null'},body:{severity:'quarantine'}},
  {op:'retry',pid:PA},{op:'wait',pid:PA},state,                        // the circuit breaker stops it now
  ...run(PA),state,                                                   // a fresh run finds no new files
  {op:'retry',pid:PA},                                                // only a failed run can be retried
  {op:'retry',pid:PA,nth:1},{op:'wait',pid:PA,nth:1},state,           // still stopped by the breaker
  api('PUT',`/api/pipelines/${PA}/controls`,{key:'breaker',value:50}),
  api('PUT',`/api/pipelines/${PA}/controls`,{key:'onEmpty',value:'never'}),
  api('PUT',`/api/pipelines/${PA}/controls`,{key:'nonsense',value:1}),
  {op:'retry',pid:PA,nth:1},{op:'wait',pid:PA,nth:1},state,
  sql(SILVER),sql('SELECT return_date, reason, channel, returns, refund_total FROM gold.agg_returns_daily ORDER BY 1, 2, 3'),
  sql('SELECT return_id, refund_amount, _source_file, _batch_id FROM bronze.vendor_returns_raw ORDER BY rowid'),
  {op:'quarantine',table:'silver.vendor_returns'},
  api('GET','/api/tables/silver.vendor_returns/preview'),api('GET','/api/tables/gold.nope/preview'),
  api('POST','/api/rules/run',{}),
  api('POST',`/api/pipelines/${PA}/recommend`,{layer:'bronze'}),api('POST',`/api/pipelines/${PA}/recommend`,{layer:'silver'}),api('POST',`/api/pipelines/${PA}/recommend`,{layer:'gold'}),
  api('POST','/api/rules',{table:'silver.vendor_returns',column:'refund_amount',type:'range',param:'0 to 100',severity:'warn'}),
  api('POST','/api/rules',{table:'silver.vendor_returns',column:'channel',type:'accepted_values',param:"web, 'app', store",severity:'quarantine'}),
  api('POST','/api/rules',{table:'silver.vendor_returns',column:'return_id',type:'regex',param:'^R\\d+$',severity:'fail'}),
  api('POST','/api/rules',{table:'silver.vendor_returns',column:'order_id',type:'referential',param:'must exist in silver.vendor_returns.order_id',severity:'warn'}),
  api('POST','/api/rules',{table:'silver.vendor_returns',type:'custom_sql',param:'refund_amount < 200',severity:'warn'}),
  api('POST','/api/rules',{table:'gold.agg_returns_daily',column:'refund_total',type:'range',param:'at most 500',severity:'warn'}),
  api('POST','/api/rules',{table:'silver.vendor_returns',column:'refund_amount',type:'range',param:'positive',severity:'warn'}),
  api('POST','/api/rules',{table:'silver.vendor_returns',type:'not_null',severity:'warn'}),
  api('POST','/api/rules',{table:'silver.vendor_returns',column:'x',type:'vibes'}),
  api('POST','/api/rules',{table:'silver.nope',column:'x',type:'not_null'}),
  {op:'rule',match:{table:'silver.vendor_returns',type:'range'},body:{min:80}},
  api('POST','/api/rules/run',{table:'silver.vendor_returns'}),state,
  {op:'mapping',table:'silver.vendor_returns',add:[{src:'channel',tgt:'channel_upper',ttype:'STRING',tx:'upper',nullable:true,pii:false},{src:'order_id',tgt:'order_no',ttype:'BIGINT',tx:'digits only',nullable:false,pii:false},
    {src:'reason',tgt:'reason_len',ttype:'INT',tx:'LENGTH(b.reason)'},{src:'reason',tgt:'reason_nice',ttype:'STRING',tx:'initcap'}]},
  {op:'mapping',table:'silver.vendor_returns',add:[{src:'channel',tgt:'channel',ttype:'STRING'}]},
  {op:'mapping',table:'silver.vendor_returns',drop:['return_id']},
  ...run(PA),state,                                                   // nothing new: ends early
  {op:'file',conn:'landing',folder:'vendor_returns',name:'2026-10-02.csv',text:DAY2},
  ...run(PA),state,sql('SELECT * FROM (SELECT return_id, channel, channel_upper, order_no, reason_len, reason_nice, refund_amount FROM silver.vendor_returns) ORDER BY return_id'),
  {op:'quarantine',table:'silver.vendor_returns'},
  api('POST','/api/drift',{table:'bronze.vendor_returns_raw',col:'carrier',action:'map'}),api('POST','/api/drift',{table:'bronze.vendor_returns_raw',col:'nope',action:'map'}),
  ...run(PA,true),state,sql('SELECT return_id, carrier, channel_upper FROM silver.vendor_returns ORDER BY return_id'),
  sql('SELECT return_date, reason, channel, returns, refund_total FROM gold.agg_returns_daily ORDER BY 1, 2, 3'),
  // a table source with a watermark, SCD Type 2 and a dimension
  api('POST','/api/plan',{cfg:B}),api('POST','/api/columns',{cfg:{...B,columns:''}}),api('POST','/api/pipelines',{cfg:B}),
  ...run(PB),state,sql('SELECT return_id, channel, refund_amount, reason, is_current, valid_to IS NULL AS open FROM silver.returns_hist ORDER BY return_id, valid_from, is_current'),
  sql('SELECT returns_hist_sk, return_id, refund_amount, is_current FROM gold.dim_returns ORDER BY return_id, is_current'),
  ...run(PB),state,
  api('PUT',`/api/pipelines/${PA}/controls`,{key:'onNewColumn',value:'rescue'}),
  {op:'file',conn:'landing',folder:'vendor_returns',name:'2026-10-03.csv',text:DAY3},
  ...run(PA),state,sql('SELECT return_id, refund_amount, carrier, _rescued_data, _source_file FROM bronze.vendor_returns_raw WHERE _rescued_data IS NOT NULL ORDER BY return_id'),
  sql(SILVER),
  ...run(PB),state,sql('SELECT return_id, channel, refund_amount, reason, is_current, valid_to IS NULL AS open FROM silver.returns_hist ORDER BY return_id, is_current, refund_amount'),
  // JSON lines, full load, stop on bad values, stop on new columns, custom Gold SQL joining another pipeline
  {op:'file',conn:'landing',folder:'events/raw',name:'a.jsonl',text:EVENTS},
  api('POST','/api/columns',{cfg:{...C,columns:''}}),api('POST','/api/plan',{cfg:C}),api('POST','/api/pipelines',{cfg:C}),
  api('POST','/api/pipelines',{cfg:{...C,name:'events2',gold:{name:'x',type:'sql',sql:'DELETE FROM t'}}}),
  ...run(PC),state,
  {op:'rule',match:{table:'silver.events',type:'castable',column:'value'},body:{severity:'warn'}},
  {op:'rule',match:{table:'silver.events',type:'castable',column:'at'},body:{severity:'quarantine'}},
  api('PUT',`/api/pipelines/${PC}/controls`,{key:'breaker',value:'60'}),
  {op:'retry',pid:PC},{op:'wait',pid:PC},state,sql('SELECT event_id, kind, value, at, flag, meta FROM silver.events ORDER BY event_id'),sql('SELECT kind, n, most_returns FROM gold.events_by_kind ORDER BY kind'),
  api('POST','/api/rules',{table:'gold.events_by_kind',type:'row_count',param:'at least 100 rows',severity:'fail'}),
  ...run(PC),state,sql('SELECT kind, n FROM gold.events_by_kind ORDER BY kind'),
  {op:'rule',match:{table:'gold.events_by_kind',type:'row_count',param:'at least 100 rows'},body:{on:false}},
  {op:'retry',pid:PC},{op:'wait',pid:PC},state,
  {op:'file',conn:'landing',folder:'events/raw',name:'b.json',text:'[{"event_id": 9, "kind": "view", "extra": "x"}]'},
  ...run(PC),state,
  api('PUT',`/api/pipelines/${PC}/controls`,{key:'gate',value:false}),api('PUT',`/api/pipelines/${PC}/controls`,{key:'onNewColumn',value:'accept'}),api('PUT',`/api/pipelines/${PC}/controls`,{key:'dedup',value:false}),
  ...run(PC),state,sql('SELECT event_id, kind, extra FROM bronze.events_raw ORDER BY event_id, kind'),
  // an engine error is retried, then fails
  api('POST','/api/pipelines',{cfg:D}),...run(PD),state,
  // empty-source behaviour
  ...run(PA),api('PUT',`/api/pipelines/${PA}/controls`,{key:'onEmpty',value:'fail'}),...run(PA),api('PUT',`/api/pipelines/${PA}/controls`,{key:'onEmpty',value:'continue'}),...run(PA),state,
  // schedule, pause, tools, MCP and approvals
  api('PUT',`/api/pipelines/${PA}/schedule`,{cron:'*/15 * * * *',retries:9,sla:60}),api('PUT',`/api/pipelines/${PA}/schedule`,{cron:'nonsense'}),
  api('POST',`/api/pipelines/${PA}/toggle`,{}),api('POST',`/api/pipelines/${PA}/toggle`,{}),
  api('GET','/api/tools'),api('POST','/api/tools/run_sql',{args:{sql:'DELETE FROM silver.vendor_returns'}}),api('POST','/api/tools/run_sql',{args:{sql:'SELECT 1; SELECT 2'}}),
  api('POST','/api/tools/list_pipelines',{args:{}}),api('POST','/api/tools/list_tables',{args:{layer:'gold'}}),api('POST','/api/tools/get_table_schema',{args:{table:'silver.vendor_returns'}}),
  api('POST','/api/tools/get_table_schema',{args:{table:'silver.nope'}}),api('POST','/api/tools/get_run_status',{args:{pipeline:PD}}),api('POST','/api/tools/get_run_status',{args:{pipeline:'nope'}}),
  api('POST','/api/tools/list_checks',{args:{table:'gold.agg_returns_daily'}}),api('POST','/api/tools/list_source_files',{args:{connection:'landing',path:'events/raw',format:'json'}}),
  api('POST','/api/tools/preview_table',{args:{table:'gold.dim_returns',limit:2}}),api('POST','/api/tools/nope',{args:{}}),
  {op:'logs',pid:PD},
  api('POST','/mcp',{jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2025-06-18',capabilities:{},clientInfo:{name:'t',version:'1'}}}),
  api('POST','/mcp',{jsonrpc:'2.0',id:2,method:'tools/list'}),api('POST','/mcp',{jsonrpc:'2.0',id:3,method:'nope'}),
  api('POST','/mcp',{jsonrpc:'2.0',id:4,method:'tools/call',params:{name:'list_connections',arguments:{}}}),
  api('POST','/mcp',{jsonrpc:'2.0',id:5,method:'tools/call',params:{name:'set_control',arguments:{pipeline:PA,key:'retries',value:3}}}),
  {op:'approve',ok:true},
  api('POST','/mcp',{jsonrpc:'2.0',id:6,method:'tools/call',params:{name:'pause_pipeline',arguments:{pipeline:PA}}}),{op:'approve',ok:false},
  api('PUT','/api/tools/add_check',{approve:false}),
  api('POST','/mcp',{jsonrpc:'2.0',id:7,method:'tools/call',params:{name:'add_check',arguments:{table:'gold.dim_returns',type:'freshness',param:'newer than 3 days',severity:'warn'}}}),
  api('POST','/mcp',{jsonrpc:'2.0',id:8,method:'tools/call',params:{name:'create_pipeline',arguments:{name:'x',target:'nope',columns:'a:STRING'}}}),{op:'approve',ok:true},
  api('POST','/api/rules/run',{}),
  // governance
  api('POST','/api/governance/erase',{column:'return_id',value:'R1001'}),api('POST','/api/governance/erase',{column:'return_id',value:'R1001',confirm:true}),
  api('POST','/api/governance/erase',{column:'bad name',value:'x'}),sql("SELECT COUNT(*) AS n FROM silver.vendor_returns WHERE return_id = 'R1001'"),
  api('GET','/api/governance/secrets'),
  {op:'quarantine',table:'silver.vendor_returns',discard:true},
  api('POST','/api/tables/refresh',{}),state,
  api('DELETE','/api/connections/landing',{}),
  api('DELETE',`/api/pipelines/${PC}`,{dropTables:true}),api('DELETE',`/api/pipelines/${PD}`,{}),api('DELETE','/api/pipelines/nope',{}),
  sql("SELECT name FROM bronze.sqlite_master WHERE type = 'table' ORDER BY name"),sql("SELECT name FROM silver.sqlite_master WHERE type = 'table' ORDER BY name"),sql("SELECT name FROM gold.sqlite_master WHERE type = 'table' ORDER BY name"),
  state,
];
module.exports=steps;
if(require.main===module)fs.writeFileSync(path.join(__dirname,'steps.json'),JSON.stringify(steps));
