/* =====================================================================================================
   The control plane's backend, running inside the page.
   A port of app/*.py from the repository: the same routes, the same generated SQL, the same run engine,
   on SQLite compiled for the browser (sql.js). Only the local engine exists here: a page in a browser
   cannot reach BigQuery or Databricks, so those need the deployed service.
   ===================================================================================================== */
const BE=(function(){
/* ---------- util.py ---------- */
const IDENT=/^[A-Za-z_][A-Za-z0-9_]*$/;
class ApiError extends Error{constructor(msg,status){super(msg);this.status=status||400}}
class EngineError extends ApiError{}
class NotFound extends ApiError{constructor(msg){super(msg,404)}}
class DataStop extends Error{}
function ident(name,what){what=what||'name';
  if(typeof name!=='string'||!IDENT.test(name))throw new ApiError(`${what} '${name}' may only use letters, digits and underscores, and must not start with a digit`);
  return name}
function slug(text){const s=String(text||'').trim().toLowerCase().replace(/[^a-z0-9_]/g,'_').replace(/^_+|_+$/g,'');
  if(!s)return 'dataset';return /^\d/.test(s)?'c_'+s:s}
const now_ms=()=>Date.now();
const clone=x=>x===undefined?undefined:JSON.parse(JSON.stringify(x));
const commas=n=>Math.round(Number(n)||0).toLocaleString('en-US');
const plural=(n,word)=>word+(n===1?'':'s');
const lines=text=>String(text||'').split(/\r\n|\r|\n/);
const pyfloat=x=>Number.isInteger(x)?x.toFixed(1):String(x);      // how Python prints a float
function pyfixed(x,d){const k=Math.pow(10,d),v=x*k,lo=Math.floor(v);if(v-lo===0.5)return ((lo%2===0?lo:lo+1)/k).toFixed(d);return x.toFixed(d)}   // Python rounds a tie to the even digit
function pyjson(v){                                                 // json.dumps with its default separators
  if(v===null||v===undefined)return 'null';
  if(Array.isArray(v))return '['+v.map(pyjson).join(', ')+']';
  if(typeof v==='object')return '{'+Object.keys(v).map(k=>pyjson(String(k))+': '+pyjson(v[k])).join(', ')+'}';
  if(typeof v==='string')return JSON.stringify(v).replace(/[\u007f-\uffff]/g,c=>'\\u'+c.charCodeAt(0).toString(16).padStart(4,'0'));
  return JSON.stringify(v)}
const pystr=v=>v===true?'True':v===false?'False':typeof v==='number'?pyfloat(v):String(v);
function cronField(expr,value,low){
  if(expr==='*')return true;
  for(const part of expr.split(',')){
    const m=/^(\*|\d+)(?:-(\d+))?(?:\/(\d+))?$/.exec(part);if(!m)return false;
    const a=m[1]==='*'?low:+m[1];let b;
    if(m[1]==='*')b=99;else if(m[2]!=null)b=+m[2];else b=m[3]?99:a;
    const step=m[3]?+m[3]:1;
    if(a<=value&&value<=b&&(value-a)%step===0)return true}
  return false}
const CRON_FIELD=/^(\*|\d+)(-\d+)?(\/\d+)?(,(\*|\d+)(-\d+)?(\/\d+)?)*$/;
function cron_valid(cron){const f=String(cron||'').trim().split(/\s+/);return f.length===5&&f.every(x=>CRON_FIELD.test(x))}
function cron_match(cron,t){const f=String(cron||'').trim().split(/\s+/);if(f.length!==5)return false;
  return cronField(f[0],t.getUTCMinutes(),0)&&cronField(f[1],t.getUTCHours(),0)&&cronField(f[2],t.getUTCDate(),1)&&cronField(f[3],t.getUTCMonth()+1,1)&&cronField(f[4],t.getUTCDay(),0)}

/* ---------- config.py ---------- */
const settings={environment:'this browser',schema_prefix:'',scheduler:true,retry_base_seconds:3,agent_model:'claude-sonnet-5-5',
  channels(){return {email:false,chat:false,pager:false}},
  public(){return {env:this.environment,prefix:this.schema_prefix,channels:this.channels(),agent:false,agentModel:this.agent_model,protected:true,scheduler:this.scheduler,browser:true}}};

/* ---------- store.py: all control-plane state in one document ---------- */
const EMPTY=()=>({seq:1,connections:[],pipelines:[],runs:[],logs:[],alerts:[],rules:[],quarantine:[],mappings:{},drift:[],audit:[],approvals:[],tstats:{},toolApproval:{},roles:[],version:1});
const LIMITS={runs:400,logs:1500,audit:400,alerts:200,quarantine:200,approvals:100};
let FILES={};                 // connection id -> { 'folder/file.csv': {text, size, modified} }
const hooks={change:null,persist:null};
let changeTimer=null,persistTimer=null;
const store={
  data:EMPTY(),
  save(){
    for(const k in LIMITS)if(this.data[k].length>LIMITS[k])this.data[k]=this.data[k].slice(0,LIMITS[k]);
    if(hooks.change&&!changeTimer)changeTimer=setTimeout(()=>{changeTimer=null;try{hooks.change()}catch(e){}},60);
    if(hooks.persist){clearTimeout(persistTimer);persistTimer=setTimeout(()=>{try{hooks.persist()}catch(e){}},700)}},
  next_id(prefix){this.data.seq+=1;return prefix+this.data.seq},
  find(key,id){return this.data[key].find(x=>x.id===id)||null},
  pipeline(pid){return this.find('pipelines',pid)},
  connection(cid){return this.find('connections',cid)},
  audit(action,target,detail,who){this.data.audit.unshift({ts:now_ms(),who:who||'you',action,target,detail:detail||'',env:settings.environment});this.save()},
  log(level,pid,task,msg,run_id){this.data.logs.unshift({ts:now_ms(),level,pid,task,msg,runId:run_id||null});this.save()},
  alert(sev,title,detail,view,pid){const a={id:this.next_id('al'),ts:now_ms(),sev,title,detail,view:view||'orchestration',pid:pid||null,ack:false};this.data.alerts.unshift(a);this.save();return a}};

/* ---------- engines/base.py ---------- */
const LAYERS=['bronze','silver','gold'];
const GENERIC_TYPES=['STRING','INT','BIGINT','DECIMAL(18,2)','DOUBLE','BOOLEAN','DATE','TIMESTAMP'];
const ALIASES={INT64:'BIGINT',INTEGER:'INT',NUMERIC:'DECIMAL(18,2)',BIGNUMERIC:'DECIMAL(18,2)',FLOAT64:'DOUBLE',FLOAT:'DOUBLE',REAL:'DOUBLE',BOOL:'BOOLEAN',VARCHAR:'STRING',TEXT:'STRING',DATETIME:'TIMESTAMP'};
function norm_type(t){t=String(t||'STRING').trim().toUpperCase();
  if(ALIASES[t])return ALIASES[t];
  if(/^DECIMAL\(\d+,\s*\d+\)$/.test(t))return t.replace(/ /g,'');
  if(t==='DECIMAL')return 'DECIMAL(18,2)';
  return GENERIC_TYPES.includes(t)?t:'STRING'}

/* ---------- engines/sqlite_engine.py: the functions SQLite does not have ---------- */
const RE_INT=/^[+-]?\d+$/,RE_NUM=/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
const pad=(n,w)=>String(n).padStart(w||2,'0');
function realDate(y,m,d){if(y<1||m<1||m>12||d<1)return false;const leap=(y%4===0&&y%100!==0)||y%400===0;return d<=[31,leap?29:28,31,30,31,30,31,31,30,31,30,31][m-1]}
const RE_DATE=/^(\d{4})([-/])(\d{1,2})\2(\d{1,2})$/;
const RE_TS=/^(\d{4})-(\d{1,2})-(\d{1,2})(?:([ T])(\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d{1,6}))?)?(Z)?)?$/;
function parseTs(s){const m=RE_TS.exec(s);if(!m)return null;
  const y=+m[1],mo=+m[2],d=+m[3];if(!realDate(y,mo,d))return null;
  if(m[4]===undefined)return m[9]?null:[y,mo,d,0,0,0];
  const h=+m[5],mi=+m[6],hasSec=m[7]!==undefined,sec=hasSec?+m[7]:0;
  if(h>23||mi>59||sec>59)return null;
  if(!hasSec&&(m[4]==='T'||m[9]))return null;        // hours and minutes alone are only accepted with a space
  if(m[9]&&m[4]!=='T')return null;                   // a trailing Z is only accepted in the T form
  return [y,mo,d,h,mi,sec]}
function try_cast(value,target){
  if(value===null||value===undefined)return null;
  const t=norm_type(target),s=String(value).trim();
  if(s==='')return null;
  if(t==='INT'||t==='BIGINT'){if(typeof value==='number'&&Number.isInteger(value))return value;return RE_INT.test(s)?Number(s):null}
  if(t==='DOUBLE'||t.startsWith('DECIMAL'))return RE_NUM.test(s)?Number(s):null;
  if(t==='BOOLEAN'){const low=s.toLowerCase();return ['true','t','1','yes','y'].includes(low)?1:['false','f','0','no','n'].includes(low)?0:null}
  if(t==='DATE'){const m=RE_DATE.exec(s);if(m)return realDate(+m[1],+m[3],+m[4])?`${m[1]}-${pad(m[3])}-${pad(m[4])}`:null;
    const p=parseTs(s);return p?`${pad(p[0],4)}-${pad(p[1])}-${pad(p[2])}`:null}
  if(t==='TIMESTAMP'){const p=parseTs(s);return p?`${pad(p[0],4)}-${pad(p[1])}-${pad(p[2])} ${pad(p[3])}:${pad(p[4])}:${pad(p[5])}`:null}
  return s}
const SHA_K=new Uint32Array([0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
  0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
  0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
  0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2]);
function sha256(text){
  const src=new TextEncoder().encode(String(text)),len=src.length,total=((len+9+63)>>6)<<6,b=new Uint8Array(total);
  b.set(src);b[len]=0x80;const dv=new DataView(b.buffer);dv.setUint32(total-8,Math.floor(len/0x20000000));dv.setUint32(total-4,(len<<3)>>>0);
  const h=new Uint32Array([0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19]),w=new Uint32Array(64);
  const rr=(x,n)=>(x>>>n)|(x<<(32-n));
  for(let o=0;o<total;o+=64){
    for(let i=0;i<16;i++)w[i]=dv.getUint32(o+i*4);
    for(let i=16;i<64;i++){const a=w[i-15],c=w[i-2];w[i]=(w[i-16]+(rr(a,7)^rr(a,18)^(a>>>3))+w[i-7]+(rr(c,17)^rr(c,19)^(c>>>10)))>>>0}
    let [a,bb,c,d,e,f,g,hh]=h;
    for(let i=0;i<64;i++){
      const t1=(hh+(rr(e,6)^rr(e,11)^rr(e,25))+((e&f)^(~e&g))+SHA_K[i]+w[i])>>>0,t2=((rr(a,2)^rr(a,13)^rr(a,22))+((a&bb)^(a&c)^(bb&c)))>>>0;
      hh=g;g=f;f=e;e=(d+t1)>>>0;d=c;c=bb;bb=a;a=(t1+t2)>>>0}
    h[0]+=a;h[1]+=bb;h[2]+=c;h[3]+=d;h[4]+=e;h[5]+=f;h[6]+=g;h[7]+=hh}
  return Array.from(h,x=>x.toString(16).padStart(8,'0')).join('')}
const RX={};
function rx(pattern,flags){const k=(flags||'')+'/'+pattern;return RX[k]||(RX[k]=new RegExp(pattern,flags||''))}
const initcap=v=>String(v).toLowerCase().replace(/(^|[^\p{L}])(\p{L})/gu,(m,a,b)=>a+b.toUpperCase());
/* a CSV reader that follows the usual rules: quoted fields, doubled quotes, line breaks inside quotes */
function parseCsv(text){
  const rows=[];let row=[],f='',i=0,q=false;const n=text.length;
  while(i<n){const c=text[i];
    if(q){if(c==='"'){if(text[i+1]==='"'){f+='"';i+=2;continue}q=false;i++;continue}f+=c;i++;continue}
    if(c==='"'&&f===''){q=true;i++;continue}
    if(c===','){row.push(f);f='';i++;continue}
    if(c==='\r'||c==='\n'){if(c==='\r'&&text[i+1]==='\n')i++;row.push(f);rows.push(row);row=[];f='';i++;continue}
    f+=c;i++}
  if(f!==''||row.length){row.push(f);rows.push(row)}
  return rows}

let SQL=null;                 // the sql.js module
const ENGINES={};             // connection id -> engine, each with its own database
class SQLiteEngine{
  constructor(conn){this.conn=conn;this.opt=conn.options||{};this.prefix=settings.schema_prefix;if(this.prefix)ident(this.prefix,'schema prefix');this.db=null;this.joinArity={}}
  _db(){
    if(!this.db){
      if(!SQL)throw new EngineError('The SQL engine has not loaded');
      const c=new SQL.Database();
      for(const layer of LAYERS)c.run(`ATTACH DATABASE ':memory:' AS ${ident(this.schema(layer))}`);
      c.create_function('try_cast',(v,t)=>try_cast(v,t));
      c.create_function('sha256',v=>v==null?null:sha256(v));
      c.create_function('regexp',(p,v)=>v==null?0:rx(p).test(String(v))?1:0);
      c.create_function('regexp_replace',(v,p,r)=>v==null?null:String(v).replace(rx(p,'g'),r));
      c.create_function('initcap',v=>v==null?null:initcap(v));
      this.db=c;this.joinArity={}}
    return this.db}
  /* names */
  schema(layer){return this.prefix+layer}
  q(name){return `"${ident(name,'column or table name')}"`}
  fq(layer,name){return `${this.schema(layer)}.${this.q(name)}`}
  lit(value){return "'"+String(value).replace(/'/g,"''")+"'"}
  /* running SQL */
  execute(sql){try{const db=this._db();db.run(sql);return {affected:db.getRowsModified()}}catch(e){throw new EngineError(String(e&&e.message||e))}}
  query(sql){let st=null;try{st=this._db().prepare(sql);const out=[];while(st.step())out.push(st.getAsObject());return out}catch(e){throw new EngineError(String(e&&e.message||e))}finally{if(st)try{st.free()}catch(e){}}}
  scalar(sql){const rows=this.query(sql);return rows.length?Object.values(rows[0])[0]:null}
  test(){this.query('SELECT 1 AS ok');return 'Query ran'}
  /* dialect */
  typ(generic){const t=norm_type(generic);return {STRING:'TEXT',INT:'INTEGER',BIGINT:'INTEGER',DOUBLE:'REAL',BOOLEAN:'INTEGER',DATE:'TEXT',TIMESTAMP:'TEXT'}[t]||'REAL'}
  try_cast(expr,generic){return `try_cast(${expr}, ${this.lit(norm_type(generic))})`}
  to_str(expr){return `CAST(${expr} AS ${this.typ('STRING')})`}
  sha256(expr){return `sha256(${expr})`}
  regex_ok(expr,pattern){return `(${expr} REGEXP ${this.lit(pattern)})`}
  regexp_replace(expr,pattern,repl){return `regexp_replace(${expr}, ${this.lit(pattern)}, ${this.lit(repl)})`}
  initcap(expr){return `initcap(${expr})`}
  join_reasons(exprs){const n=exprs.length+1;
    if(!this.joinArity[n]){const f=(sep,...parts)=>parts.filter(p=>p!=null).map(String).join(sep);Object.defineProperty(f,'length',{value:n});this._db().create_function('join_nonnull',f);this.joinArity[n]=true}
    return "join_nonnull('; ', "+exprs.join(', ')+')'}
  row_json(alias,cols){return 'json_object('+cols.map(c=>`${this.lit(c)}, ${alias}.${this.q(c)}`).join(', ')+')'}
  concat(parts){return '('+parts.join(' || ')+')'}
  now(){return "strftime('%Y-%m-%d %H:%M:%S', 'now')"}
  today(){return "date('now')"}
  /* statements */
  ctas(fq,select){return [`DROP TABLE IF EXISTS ${fq}`,`CREATE TABLE ${fq} AS\n${select}`]}
  create_if_missing(fq,cols){return `CREATE TABLE IF NOT EXISTS ${fq} (`+cols.map(([n,t])=>`${this.q(n)} ${this.typ(t)}`).join(', ')+')'}
  drop(fq){return `DROP TABLE IF EXISTS ${fq}`}
  truncate(fq){return `DELETE FROM ${fq}`}
  add_column(fq,col,generic){return `ALTER TABLE ${fq} ADD COLUMN ${this.q(col)} ${this.typ(generic)}`}
  upsert(target,source,keys,cols){
    const on=keys.map(k=>`s.${this.q(k)} = t.${this.q(k)}`).join(' AND '),names=cols.map(c=>this.q(c)).join(', ');
    return [`DELETE FROM ${target} AS t WHERE EXISTS (SELECT 1 FROM ${source} s WHERE ${on})`,`INSERT INTO ${target} (${names})\nSELECT ${names} FROM ${source}`]}
  /* metadata */
  ensure_schemas(){this._db()}
  rawColumns(layer,name){return this.query(`PRAGMA ${this.schema(layer)}.table_info(${this.q(name)})`).map(r=>[r.name,r.type||''])}
  columns(layer,name){return this.rawColumns(layer,name).map(([n,t])=>[n,t||'TEXT'])}
  exists(layer,name){return this.columns(layer,name).length>0}
  stats(layer,name){return {rows:Number(this.scalar(`SELECT COUNT(*) AS n FROM ${this.fq(layer,name)}`)||0),bytes:null}}
  source_table(path){
    const parts=String(path).split('.');
    if(parts.length!==2)throw new EngineError('Name the source table as layer.table, for example bronze.orders_raw');
    const schema=ident(parts[0],'schema'),name=ident(parts[1],'table');
    let rows;try{rows=this.query(`PRAGMA ${schema}.table_info(${this.q(name)})`)}catch(e){rows=[]}
    if(!rows.length)throw new EngineError(`Source table ${path} does not exist`);
    return [`${schema}.${this.q(name)}`,rows.map(r=>[r.name,r.type||'TEXT'])]}
  /* files: the files uploaded to a landing area in this browser */
  _prefix(cfg){return String(cfg.path||'').trim().replace(/^\/+|\/+$/g,'')}
  list_files(src,cfg){
    const fmt=cfg.format||'csv',EXT={csv:['.csv'],json:['.json','.jsonl','.ndjson']};
    if(!EXT[fmt])throw new EngineError(`The local engine reads CSV and JSON files. ${String(fmt).toUpperCase()} needs BigQuery or Databricks.`);
    const all=FILES[src.id]||{},prefix=this._prefix(cfg),under=Object.keys(all).filter(n=>!prefix||n.startsWith(prefix+'/'));
    if(prefix&&!under.length)throw new EngineError(`There is no folder called ${prefix} in ${src.id}. Upload files into it first.`);
    return under.filter(n=>EXT[fmt].some(x=>n.toLowerCase().endsWith(x)))
      .map(n=>({name:prefix?n.slice(prefix.length+1):n,size:all[n].size,modified:all[n].modified})).sort((a,b)=>a.name<b.name?-1:a.name>b.name?1:0)}
  stage_files(src,cfg,files,stage_fq){
    const prefix=this._prefix(cfg),fmt=cfg.format||'csv',all=FILES[src.id]||{},rows=[],cols=[];
    const col=name=>{const t=String(name).trim(),c=IDENT.test(t)?t:slug(name);if(!cols.includes(c))cols.push(c);return c};
    for(const f of files){
      const file=all[(prefix?prefix+'/':'')+f.name];if(!file)throw new EngineError(`File ${f.name} is no longer in ${src.id}`);
      let text=file.text;if(text.charCodeAt(0)===0xFEFF)text=text.slice(1);
      if(fmt==='csv'){
        const recs=parseCsv(text),header=recs.shift();
        if(!header||header.every(h=>!h.trim()))continue;
        const names=header.map(col);
        for(const rec of recs){
          if(!rec.some(x=>x.trim()))continue;
          const r={_source_file:f.name};names.forEach((n,i)=>{r[n]=i<rec.length?rec[i]:null});rows.push(r)}
      }else{
        let items;const body=text.trim();
        try{items=body.startsWith('[')?JSON.parse(body):body.split(/\r\n|\r|\n/).filter(l=>l.trim()).map(l=>JSON.parse(l))}catch(e){throw new EngineError(`File ${f.name} is not valid JSON: ${e.message}`)}
        for(const obj of items){const rec={};
          for(const [k,v] of Object.entries(obj||{}))rec[col(k)]=v===null?null:typeof v==='string'?v:typeof v==='object'?pyjson(v):typeof v==='boolean'?(v?'True':'False'):String(v);
          rec._source_file=f.name;rows.push(rec)}}}
    const db=this._db(),all_cols=cols.concat(['_source_file']);
    try{
      db.run(this.drop(stage_fq));
      db.run(`CREATE TABLE ${stage_fq} (`+all_cols.map(c=>`${this.q(c)} TEXT`).join(', ')+')');
      if(rows.length){
        db.run('BEGIN');const st=db.prepare(`INSERT INTO ${stage_fq} VALUES (`+all_cols.map(()=>'?').join(', ')+')');
        try{for(const r of rows)st.run(all_cols.map(c=>r[c]===undefined?null:r[c]))}finally{st.free()}
        db.run('COMMIT')}
    }catch(e){try{db.run('ROLLBACK')}catch(_){}if(e instanceof ApiError)throw e;throw new EngineError(String(e&&e.message||e))}
    return cols}
  compute(){return [{id:'sqlite',kind:'In-browser SQLite',spec:'Runs in this page. Tables live in memory and are saved in this browser.',state:'running',canToggle:false}]}
}
SQLiteEngine.kind='local';SQLiteEngine.platform='local';SQLiteEngine.label='Local SQLite';SQLiteEngine.table_format='SQLite table';SQLiteEngine.file_connection='folder';
const ENGINE_TYPES={local:SQLiteEngine};
const CLOUD_ENGINES={bq:'BigQuery',adb:'Azure Databricks'};
const NEEDS_SERVICE=name=>`This copy runs inside your browser, and a page in a browser cannot reach ${name}. Use the local engine here, or deploy the service from your repository to work with ${name}.`;
function get_engine(conn){
  if(conn&&CLOUD_ENGINES[conn.type])throw new EngineError(NEEDS_SERVICE(CLOUD_ENGINES[conn.type]));
  if(!conn||!ENGINE_TYPES[conn.type])throw new EngineError('This pipeline has no warehouse connection. Add a BigQuery, Databricks or local connection first.');
  if(!ENGINES[conn.id])ENGINES[conn.id]=new ENGINE_TYPES[conn.type](conn);
  ENGINES[conn.id].conn=conn;
  return ENGINES[conn.id]}
