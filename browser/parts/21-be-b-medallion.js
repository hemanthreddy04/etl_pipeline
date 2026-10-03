/* ---------- medallion.py: turns a pipeline definition into the SQL that builds each layer ---------- */
const LINEAGE=['_ingest_ts','_ingest_date','_source_file','_batch_id'];
const PRESETS=['','trim','lower','upper','initcap','cast','digits only','passthrough'];
const DEFAULT_CTL={onNewColumn:'accept',onEmpty:'skip',dedup:true,mask:true,breaker:5,gate:true,alertFail:true,alertWarn:true,alertSla:true,alertOk:false,email:true,chat:false,pager:false};
const M={};
M.parse_cols=function(text){const out=[];
  for(const line of lines(text)){
    const parts=line.split(':').map(x=>x.trim());
    if(!parts.length||!parts[0])continue;
    const flag=parts.length>2?parts[2].toLowerCase():'';
    out.push({name:IDENT.test(parts[0])?parts[0]:slug(parts[0]),type:norm_type(parts.length>1&&parts[1]?parts[1]:'STRING'),key:flag.includes('key'),pii:flag.includes('pii')})}
  return out};
M.table_names=function(p){const t=p.table,g=p.gold;
  return {bronze:t+'_raw',stg:t+'__stg',silver:t,chk:t+'__chk',new:t+'__new',quar:t+'__quarantine',gold:g,gold_new:g+'__new'}};
M.fqs=function(p,eng){const n=M.table_names(p),layer={bronze:'bronze',stg:'bronze',silver:'silver',chk:'silver',new:'silver',quar:'silver',gold:'gold',gold_new:'gold'},out={};
  for(const k in n)out[k]=eng.fq(layer[k],n[k]);return out};
M.build_pipeline=function(cfg,target_conn,engine_cls){
  const t=ident(slug(cfg.name),'dataset name'),cols=M.parse_cols(cfg.columns);
  if(!cols.length)throw new ApiError('Add at least one column');
  const gold=cfg.gold||{},g=ident(slug(gold.name||`agg_${t}`),'Gold table name');
  const keyed=cols.filter(c=>c.key).map(c=>c.name),keys=keyed.length?keyed:[cols[0].name];
  const kind=['files','table'].includes(cfg.kind)?cfg.kind:'files',b=`bronze_${t}`,s=`silver_${t}`;
  const tasks=[
    {id:b,label:`Bronze ${t}`,layer:'bronze',deps:[]},
    {id:'dq_bronze',label:'Bronze checks',layer:'ops',deps:[b]},
    {id:s,label:`Silver ${t}`,layer:'silver',deps:['dq_bronze']},
    {id:'dq_silver',label:'Silver checks',layer:'ops',deps:[s]},
    {id:`gold_${g}`,label:`Gold ${g}`,layer:'gold',deps:['dq_silver']},
    {id:'dq_gold',label:'Gold checks',layer:'ops',deps:[`gold_${g}`]}];
  const ctl={...DEFAULT_CTL};
  ctl.mask=cfg.maskPii===undefined?true:!!cfg.maskPii;
  ctl.onNewColumn={failOnNewColumns:'stop',rescue:'rescue'}[cfg.evolve]||'accept';
  const retries=Math.max(0,Math.min(5,parseInt(cfg.retries||0,10)||0));
  return {id:`${t}_medallion`,name:`${t}_medallion`,table:t,gold:g,target:target_conn.id,source:cfg.conn||target_conn.id,platform:engine_cls.platform,engine:engine_cls.label,
    kind,format:cfg.format||'csv',path:String(cfg.path||t).trim(),pattern:cfg.pattern==='full'?'full':'incremental',watermark:String(cfg.watermark||'').trim(),
    scd:String(cfg.scd)==='2'?'2':'1',keys,cfg,status:'active',cron:cfg.cron||'0 * * * *',retries,sla:120,owner:'data-eng',ctl,tasks,
    writes:[`bronze.${t}_raw`,`silver.${t}`,`gold.${g}`],orch:'Control plane scheduler',mode:cfg.pattern==='full'?'Batch, full load':'Batch, incremental',
    compute:engine_cls.label,loaded:{},wm:null,pending:[],history:[],lastSched:''}};
M.default_mapping=function(p){const cols=M.parse_cols(p.cfg.columns),mask=p.cfg.maskPii===undefined?true:!!p.cfg.maskPii;
  return {source:`bronze.${p.table}_raw`,rows:cols.map(c=>({src:c.name,stype:'STRING',tgt:c.name,ttype:c.type,tx:c.type==='STRING'?'trim':'cast',nullable:!c.key,pii:!!(c.pii&&mask)}))}};
M.default_rules=function(p){const cols=M.parse_cols(p.cfg.columns),t=p.table,on_bad=p.cfg.onBad;
  const rules=[['bronze.'+t+'_raw','*','row_count','at least 1 row per run','fail']];
  for(const k of p.keys){rules.push([`silver.${t}`,k,'not_null','','fail']);rules.push([`silver.${t}`,k,'unique',p.scd==='2'?'one current row per key':'','fail'])}
  for(const c of cols)if(c.type!=='STRING')rules.push([`silver.${t}`,c.name,'castable',c.type,on_bad==='stop'?'fail':'quarantine']);
  rules.push([`gold.${p.gold}`,'*','row_count','at least 1 row','fail']);
  return rules};

/* Bronze */
M.stage_table_sql=function(p,eng,src_fq,src_cols,since){
  const wm=p.watermark,select=src_cols.map(([c])=>`${eng.to_str(eng.q(c))} AS ${eng.q(c)}`).join(', ');
  const where=wm&&since?`\nWHERE ${eng.to_str(eng.q(wm))} > ${eng.lit(since)}`:'';
  return `SELECT ${select}, ${eng.lit('table:'+p.path)} AS _source_file\nFROM ${src_fq}${where}`};
M.bronze_sql=function(p,eng,stage_cols,existing,batch,full){
  /* existing: data columns already in the Bronze table, or null when the table does not exist yet */
  const f=M.fqs(p,eng),ctl=p.ctl,rescue=ctl.onNewColumn==='rescue';
  const lineage=`${eng.now()} AS _ingest_ts, ${eng.today()} AS _ingest_date, s._source_file AS _source_file, ${eng.lit(batch)} AS _batch_id`;
  if(existing===null){
    const cols=stage_cols.map(c=>`s.${eng.q(c)} AS ${eng.q(c)}`).join(', '),extra=rescue?`, CAST(NULL AS ${eng.typ('STRING')}) AS _rescued_data`:'';
    return eng.ctas(f.bronze,`SELECT ${cols}, ${lineage}${extra}\nFROM ${f.stg} s`)}
  const fresh=stage_cols.filter(c=>!existing.includes(c)),out=[];let data=existing.slice();
  if(fresh.length&&!rescue){for(const c of fresh)out.push(eng.add_column(f.bronze,c,'STRING'));data=data.concat(fresh)}
  out.push(full?eng.truncate(f.bronze):`DELETE FROM ${f.bronze} WHERE _batch_id = ${eng.lit(batch)}`);
  const names=data.map(c=>eng.q(c)).concat(LINEAGE);
  const values=data.map(c=>stage_cols.includes(c)?`s.${eng.q(c)}`:`CAST(NULL AS ${eng.typ('STRING')})`);
  let select=values.join(', ')+`, ${eng.now()}, ${eng.today()}, s._source_file, ${eng.lit(batch)}`;
  if(rescue){names.push('_rescued_data');select+=', '+(fresh.length?eng.row_json('s',fresh):`CAST(NULL AS ${eng.typ('STRING')})`)}
  out.push(`INSERT INTO ${f.bronze} (${names.join(', ')})\nSELECT ${select}\nFROM ${f.stg} s`);
  return out};

/* Silver */
M.map_expr=function(r,eng,bronze_cols,alias){alias=alias||'b';
  /* returns [text expression before the cast, typed expression] */
  const src=r.src||'',tx=String(r.tx||'').trim(),ttype=norm_type(r.ttype),low=tx.toLowerCase();let base;
  if(PRESETS.includes(low)){
    if(!bronze_cols.includes(src))base=`CAST(NULL AS ${eng.typ('STRING')})`;
    else if(src.startsWith('_'))base=`${alias}.${eng.q(src)}`;
    else{const ref=`NULLIF(TRIM(${alias}.${eng.q(src)}), '')`;
      base={lower:`LOWER(${ref})`,upper:`UPPER(${ref})`,initcap:eng.initcap(ref),'digits only':`NULLIF(${eng.regexp_replace(ref,'[^0-9]','')}, '')`}[low]||ref}
  }else base=`(${tx})`;
  if(ttype==='STRING')return [base,base];
  if(src.startsWith('_')&&PRESETS.includes(low))return [base,base];
  return [base,eng.try_cast(base,ttype)]};
M.parse_range=function(param){const text=String(param||''),nums=(text.match(/-?\d+(?:\.\d+)?/g)||[]).map(Number),low=text.toLowerCase();
  if(!nums.length)return [null,null];
  if(nums.length>=2)return [nums[0],nums[1]];
  if(['or less','at most','<=','below','up to'].some(w=>low.includes(w)))return [null,nums[0]];
  return [nums[0],null]};
M.rule_flag=function(rule,eng,ref,mapping_rows){
  /* SQL that is TRUE for a bad row, written against the alias `ref`. null when the rule is not a per-row rule. */
  const col=rule.column,typ=rule.type,param=String(rule.param||'');
  if(typ==='custom_sql')return param.trim()?`NOT COALESCE((${param}), FALSE)`:null;
  if(!col||col==='*'||!IDENT.test(col))return null;
  const c=`${ref}.${eng.q(col)}`;
  if(typ==='not_null')return `${c} IS NULL`;
  if(typ==='castable'){
    if(mapping_rows===undefined||mapping_rows===null)return `(${c} IS NOT NULL AND TRIM(${c}) <> '' AND ${eng.try_cast(c,param||'STRING')} IS NULL)`;
    const row=mapping_rows.find(r=>r.tgt===col);
    if(!row||norm_type(row.ttype)==='STRING')return null;
    return `(${ref}.${eng.q(col+'__raw')} IS NOT NULL AND ${c} IS NULL)`}
  if(typ==='range'){const [lo,hi]=M.parse_range(param),conds=(lo!==null?[`${c} < ${String(lo)}`]:[]).concat(hi!==null?[`${c} > ${String(hi)}`]:[]);
    return conds.length?'('+conds.join(' OR ')+')':null}
  if(typ==='accepted_values'){const vals=param.split(',').filter(v=>v.trim()).map(v=>v.trim().replace(/^['"]+|['"]+$/g,''));
    return vals.length?`(${c} IS NOT NULL AND ${eng.to_str(c)} NOT IN (${vals.map(v=>eng.lit(v)).join(', ')}))`:null}
  if(typ==='regex')return param?`(${c} IS NOT NULL AND NOT ${eng.regex_ok(c,param)})`:null;
  if(typ==='referential'){const m=/(bronze|silver|gold)\.([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)/.exec(param);
    if(!m)return null;
    const other=eng.fq(m[1],m[2]);
    return `(${c} IS NOT NULL AND ${c} NOT IN (SELECT ${eng.q(m[3])} FROM ${other} WHERE ${eng.q(m[3])} IS NOT NULL))`}
  return null};
M.rule_label=function(rule){const name=rule.type.replace(/_/g,' '),extra=rule.param&&rule.type!=='custom_sql'?` (${rule.param})`:'';
  return rule.column!==null&&rule.column!==undefined&&rule.column!=='*'?`${rule.column}: ${name}${extra}`:`${name}${extra}`};
M.silver_check_sql=function(p,eng,mapping_rows,rules,bronze_cols,batches,full){
  /* the checked staging table: every mapped column typed, plus one flag column per row-level rule.
     returns [statements, [[rule, flag column]]] */
  const f=M.fqs(p,eng),data_cols=bronze_cols.filter(c=>!LINEAGE.includes(c)&&c!=='_rescued_data');
  const cast_rules=new Set(rules.filter(r=>r.type==='castable').map(r=>r.column)),inner=[];
  for(const r of mapping_rows){const [base,typed]=M.map_expr(r,eng,bronze_cols);
    inner.push(`${typed} AS ${eng.q(r.tgt)}`);
    if(cast_rules.has(r.tgt)&&norm_type(r.ttype)!=='STRING')inner.push(`${base} AS ${eng.q(r.tgt+'__raw')}`)}
  inner.push('b._ingest_ts AS _ingest_ts');
  inner.push(`${eng.row_json('b',data_cols)} AS _row`);
  const where=full||!batches||!batches.length?'':'\n  WHERE b._batch_id IN ('+batches.map(x=>eng.lit(x)).join(', ')+')';
  const flags=[];
  for(const r of rules){const cond=M.rule_flag(r,eng,'s',mapping_rows);if(cond)flags.push([r,'_f_'+String(r.id).replace(/[^A-Za-z0-9_]/g,'_'),cond])}
  const flag_sql=flags.map(([,name,cond])=>`,\n  COALESCE(${cond}, FALSE) AS ${name}`).join('');
  const select='SELECT s.*'+flag_sql+'\nFROM (\n  SELECT '+inner.join(',\n    ')+`\n  FROM ${f.bronze} b${where}\n) s`;
  return [eng.ctas(f.chk,select),flags.map(([r,name])=>[r,name])]};
M.silver_new_sql=function(p,eng,mapping_rows,exclude_flags){
  /* the rows that will be merged: bad rows left out, duplicates removed, personal data masked */
  const f=M.fqs(p,eng),ctl=p.ctl,keys=p.keys,cols=[];
  for(const r of mapping_rows){const c=`y.${eng.q(r.tgt)}`,t=norm_type(r.ttype);
    if(r.pii&&ctl.mask&&t==='STRING')cols.push(`${eng.sha256(`LOWER(${c})`)} AS ${eng.q(r.tgt)}`);
    else cols.push(`CAST(${c} AS ${eng.typ(t)}) AS ${eng.q(r.tgt)}`)}
  cols.push('y._ingest_ts AS _ingest_ts');
  if(p.scd==='2'){const tracked=mapping_rows.map(r=>r.tgt).filter(c=>!keys.includes(c)),parts=[];
    for(const c of tracked)parts.push(`COALESCE(${eng.to_str('y.'+eng.q(c))}, '')`,"'|'");
    cols.push(`${eng.sha256(eng.concat(parts.length?parts:["''"]))} AS row_hash`)}
  const where=exclude_flags.length?'\n  WHERE NOT ('+exclude_flags.map(n=>`x.${n}`).join(' OR ')+')':'';let src;
  if(ctl.dedup===undefined||ctl.dedup){const part=keys.map(k=>`x.${eng.q(k)}`).join(', ');
    src=`(\n  SELECT x.*, ROW_NUMBER() OVER (PARTITION BY ${part} ORDER BY x._ingest_ts DESC) AS _rn\n  FROM ${f.chk} x${where}\n) y\nWHERE y._rn = 1`}
  else src=`(\n  SELECT x.*\n  FROM ${f.chk} x${where}\n) y`;
  return eng.ctas(f.new,'SELECT '+cols.join(',\n  ')+'\nFROM '+src)};
M.silver_columns=function(p,mapping_rows){let cols=mapping_rows.map(r=>[r.tgt,norm_type(r.ttype)]).concat([['_ingest_ts','TIMESTAMP']]);
  if(p.scd==='2')cols=cols.concat([['row_hash','STRING'],['valid_from','TIMESTAMP'],['valid_to','TIMESTAMP'],['is_current','BOOLEAN']]);
  return cols};
M.silver_write_sql=function(p,eng,mapping_rows,existing,full){
  /* existing: column names already in the Silver table, or null when it does not exist yet */
  const f=M.fqs(p,eng),keys=p.keys,base=mapping_rows.map(r=>r.tgt).concat(['_ingest_ts']),names=base.map(c=>eng.q(c)).join(', '),scd2=p.scd==='2',ts=eng.typ('TIMESTAMP');
  if(existing===null){
    if(scd2)return eng.ctas(f.silver,`SELECT ${names}, row_hash, _ingest_ts AS valid_from, CAST(NULL AS ${ts}) AS valid_to, TRUE AS is_current\nFROM ${f.new}`);
    return eng.ctas(f.silver,`SELECT ${names}\nFROM ${f.new}`)}
  const out=M.silver_columns(p,mapping_rows).filter(([c])=>!existing.includes(c)).map(([c,t])=>eng.add_column(f.silver,c,t));
  if(scd2){const on=keys.map(k=>`s.${eng.q(k)} = t.${eng.q(k)}`).join(' AND ');
    if(full)out.push(eng.truncate(f.silver));
    else out.push(`UPDATE ${f.silver} AS t\nSET is_current = FALSE, valid_to = ${eng.now()}\nWHERE t.is_current\n  AND EXISTS (SELECT 1 FROM ${f.new} s WHERE ${on} AND s.row_hash <> t.row_hash)`);
    out.push(`INSERT INTO ${f.silver} (${names}, row_hash, valid_from, valid_to, is_current)\nSELECT ${base.map(c=>'s.'+eng.q(c)).join(', ')}, s.row_hash, s._ingest_ts, CAST(NULL AS ${ts}), TRUE\nFROM ${f.new} s\n`+
      `WHERE NOT EXISTS (SELECT 1 FROM ${f.silver} t WHERE ${on} AND t.is_current AND t.row_hash = s.row_hash)`);
    return out}
  if(full)return out.concat([eng.truncate(f.silver),`INSERT INTO ${f.silver} (${names})\nSELECT ${names}\nFROM ${f.new}`]);
  return out.concat(eng.upsert(f.silver,f.new,keys,base))};

/* Gold */
M.gold_select=function(p,eng){
  const f=M.fqs(p,eng),g=p.cfg.gold||{},t=p.table,current=p.scd==='2'?'\nWHERE is_current':'',kind=g.type||'aggregate';
  if(kind==='sql'){
    let sql=String(g.sql||'').trim().replace(/;+$/,'');
    if(!/^\s*(select|with)\b/i.test(sql))throw new ApiError('The Gold SQL must be a single SELECT statement');
    sql=sql.split('{silver}').join(f.silver).split('{bronze}').join(f.bronze);
    return sql.replace(/\{(bronze|silver|gold)\.([A-Za-z0-9_]+)\}/g,(m,a,b)=>eng.fq(a,b))}
  if(kind==='dimension'){
    const cols=M.parse_cols(p.cfg.columns),keys=p.keys.concat(p.scd==='2'?['valid_from']:[]),parts=[];
    for(const k of keys)parts.push(`COALESCE(${eng.to_str(eng.q(k))}, '')`,"'|'");
    const attrs=cols.map(c=>eng.q(c.name)).concat(p.scd==='2'?['valid_from','valid_to','is_current']:[]);
    return `SELECT ${eng.sha256(eng.concat(parts))} AS ${eng.q(t+'_sk')},\n  `+attrs.join(',\n  ')+`,\n  ${eng.now()} AS _built_at\nFROM ${f.silver}`}
  const grain=[g.dateCol||''].concat(String(g.dims||'').split(',')).filter(x=>x.trim()).map(x=>ident(x.trim(),'Gold column'));
  const measures=lines(g.measures||'COUNT(*) AS row_count').filter(m=>m.trim()).map(m=>m.trim().replace(/,+$/,''));
  const select=grain.map(x=>eng.q(x)).concat(measures,[`${eng.now()} AS _built_at`]).join(',\n  ');
  const group=grain.length?'\nGROUP BY '+grain.map(x=>eng.q(x)).join(', '):'';
  return `SELECT\n  ${select}\nFROM ${f.silver}${current}${group}`};
M.gold_sql=function(p,eng){const f=M.fqs(p,eng),gate=p.ctl.gate===undefined?true:p.ctl.gate;return eng.ctas(gate?f.gold_new:f.gold,M.gold_select(p,eng))};
M.plan=function(p,eng,mapping_rows,rules){
  /* the statements a run would execute, for display. Column lists assume the declared columns. */
  const declared=M.parse_cols(p.cfg.columns).map(c=>c.name),f=M.fqs(p,eng),gate=p.ctl.gate===undefined?true:p.ctl.gate;
  const silver_rules=rules.filter(r=>r.table===`silver.${p.table}`&&(r.on===undefined||r.on));
  const [chk,flags]=M.silver_check_sql(p,eng,mapping_rows,silver_rules,declared.concat(LINEAGE),['<run id>'],p.pattern==='full');
  const excl=flags.filter(([r])=>['quarantine','fail'].includes(r.severity)).map(([,name])=>name);
  const source=p.kind==='files'?'-- Files are staged into '+f.stg+' as text, one column per source column\n':'';
  return {
    bronze:source+M.bronze_sql(p,eng,declared,null,'<run id>',false).join(';\n\n')+';',
    silver:chk.concat(M.silver_new_sql(p,eng,mapping_rows,excl),M.silver_write_sql(p,eng,mapping_rows,M.silver_columns(p,mapping_rows).map(([c])=>c),p.pattern==='full')).join(';\n\n')+';',
    gold:M.gold_sql(p,eng).concat(gate?['-- after the Gold checks pass\n'+eng.ctas(f.gold,`SELECT * FROM ${f.gold_new}`).slice(-1)[0]]:[]).join(';\n\n')+';'}};

/* ---------- quality.py: each rule becomes SQL that counts what fails ---------- */
const DIMENSION={not_null:'Completeness',unique:'Uniqueness',range:'Validity',accepted_values:'Validity',regex:'Validity',castable:'Validity',referential:'Consistency',freshness:'Timeliness',
  row_count:'Completeness',volume_anomaly:'Completeness',reconciliation:'Accuracy',schema:'Consistency',custom_sql:'Consistency'};
const ROW_RULES=['not_null','castable','range','accepted_values','regex','referential','custom_sql'];
const Q={};
Q.first_number=function(text,dflt){const m=/-?\d+(?:\.\d+)?/.exec(String(text||''));return m?parseFloat(m[0]):dflt};
Q.minutes=function(text,dflt){if(dflt===undefined)dflt=120;const m=/(\d+(?:\.\d+)?)\s*(minute|min|hour|hr|day)/.exec(String(text||'').toLowerCase());
  if(!m)return dflt;const n=parseFloat(m[1]);return n*(m[2].startsWith('min')?1:m[2].startsWith('h')?60:1440)};
Q.pass_rate=function(checked,failed){return !checked?100:Math.round(Math.max(0,100*(1-failed/checked))*1e4)/1e4};
Q.violated=function(rule,checked,failed){return Q.pass_rate(checked,failed)<Number(rule.min!==undefined&&rule.min!==null?rule.min:100)};
Q.record=function(rule,checked,failed){Object.assign(rule,{checked:Math.trunc(checked),failed:Math.trunc(failed),pass:Q.pass_rate(checked,failed),last:now_ms()})};
Q.evaluate=function(rule,eng,p,state,ctx){
  /* count failures for one rule against the table as it stands. Returns [checked, failed, note], or null when it cannot be evaluated here. */
  ctx=ctx||{};
  const dot=rule.table.indexOf('.'),layer=rule.table.slice(0,dot),name=rule.table.slice(dot+1),f=M.fqs(p,eng);
  const fq=(ctx.override||{})[rule.table]||eng.fq(layer,name),typ=rule.type,col=rule.column||'*',param=String(rule.param||'');
  let scope='';
  if(layer==='bronze'&&ctx.batches&&ctx.batches.length)scope=' WHERE x._batch_id IN ('+ctx.batches.map(b=>eng.lit(b)).join(', ')+')';

  if(typ==='schema'){const drift=ctx.drift;
    if(drift===undefined||drift===null){const pending=(state.drift||[]).filter(d=>d.table===rule.table&&d.status==='pending');
      return [1,pending.length?1:0,pending.length?'new columns are waiting for a decision':'']}
    return [1,drift.length?1:0,drift.length?'new columns: '+drift.join(', '):'']}
  if(typ==='freshness'){const fresh=((state.tstats||{})[rule.table]||{}).fresh;
    if(ctx.in_run)return [1,0,'written by this run'];
    if(!fresh)return null;
    const age=(now_ms()-fresh)/60000,limit=Q.minutes(param);
    return [1,age>limit?1:0,`${pyfixed(age,0)} minutes old, limit ${pyfixed(limit,0)}`]}
  if(typ==='volume_anomaly'){const rows=ctx.bronze_rows,hist=(p.history||[]).map(h=>h.rows).slice(-7);
    if(rows===undefined||rows===null||hist.length<3)return null;
    const avg=hist.reduce((a,b)=>a+b,0)/hist.length,tol=Q.first_number(param,30),off=avg?Math.abs(rows-avg)/avg*100:0;
    return [1,off>tol?1:0,`${rows} rows against a recent average of ${pyfixed(avg,0)} (${pyfixed(off,0)}% away, limit ${pyfixed(tol,0)}%)`]}
  if(typ==='row_count'){const need=Math.trunc(Q.first_number(param,1)),n=Number(eng.scalar(`SELECT COUNT(*) AS n FROM ${fq} x${scope}`)||0);
    return [1,n<need?1:0,`${n} rows, at least ${need} expected`]}
  if(typ==='unique'){const cols=col!=='*'?[col]:p.keys,where=layer==='silver'&&p.scd==='2'?' WHERE x.is_current':scope;
    let expr=`x.${eng.q(cols[0])}`;
    if(cols.length>1){const parts=[];for(const c of cols)parts.push(`COALESCE(${eng.to_str('x.'+eng.q(c))}, '')`,"'|'");expr=eng.concat(parts)}
    const r=eng.query(`SELECT COUNT(*) AS n, COUNT(DISTINCT ${expr}) AS d FROM ${fq} x${where}`)[0],n=Number(r.n||0),d=Number(r.d||0);
    return [n,Math.max(0,n-d),'']}
  if(typ==='reconciliation'){const tol=Q.first_number(param,0.1);
    if(layer==='gold'&&col!=='*'){const m=/of\s+(.+?)\s+in\s+silver/i.exec(param),silver_expr=m?m[1]:`SUM(${eng.q(col)})`,where=p.scd==='2'?' WHERE is_current':'';
      const g=Number(eng.scalar(`SELECT SUM(${eng.q(col)}) AS v FROM ${fq}`)||0),s=Number(eng.scalar(`SELECT ${silver_expr} AS v FROM ${f.silver}${where}`)||0);
      const off=s?Math.abs(g-s)/Math.abs(s)*100:(g===0?0:100);
      return [1,off>tol?1:0,`Gold ${pyfixed(g,2)} against Silver ${pyfixed(s,2)} (${pyfixed(off,3)}% apart, tolerance ${pyfloat(tol)}%)`]}
    if(layer==='silver'&&ctx.bronze_rows!==undefined&&ctx.bronze_rows!==null){const b=ctx.bronze_rows,s=ctx.silver_rows||0,qn=ctx.quarantined||0,lost=Math.max(0,b-s-qn),off=b?lost/b*100:0;
      return [1,off>tol?1:0,`${b} rows in, ${s} merged, ${qn} quarantined, ${lost} removed as duplicates (${pyfixed(off,2)}%, tolerance ${pyfloat(tol)}%)`]}
    return null}
  if(ROW_RULES.includes(typ)){
    if(typ==='castable'&&layer==='silver'){
      /* after the merge a bad value is already NULL, so look at the raw text in Bronze */
      const mapping=((state.mappings||{})[rule.table]||{}).rows||[],row=mapping.find(r=>r.tgt===col);
      if(!row||norm_type(row.ttype)==='STRING')return null;
      const bronze_cols=eng.columns('bronze',p.table+'_raw').map(([c])=>c);
      if(!bronze_cols.length)return null;
      const [base,typed]=M.map_expr(row,eng,bronze_cols);
      const r=eng.query(`SELECT COUNT(*) AS n, SUM(CASE WHEN ${base} IS NOT NULL AND ${typed} IS NULL THEN 1 ELSE 0 END) AS f FROM ${f.bronze} b`)[0];
      return [Number(r.n||0),Number(r.f||0),'measured on the raw values in Bronze']}
    const cond=M.rule_flag(rule,eng,'x');
    if(!cond)return null;
    const where=scope||(layer==='silver'&&p.scd==='2'?' WHERE x.is_current':'');
    const r=eng.query(`SELECT COUNT(*) AS n, SUM(CASE WHEN ${cond} THEN 1 ELSE 0 END) AS f FROM ${fq} x${where}`)[0];
    return [Number(r.n||0),Number(r.f||0),'']}
  return null};
Q.recommended=function(p,layer,tables){
  /* the usual starting checks for a layer. Returns [[table, column, type, param, severity]] */
  const out=[],t=p.table;
  if(layer==='bronze'){const tid=`bronze.${t}_raw`;
    out.push([tid,'*','row_count','at least 1 row per run','fail'],[tid,'*','schema','no unannounced columns','warn'],[tid,'*','volume_anomaly','within 30% of the recent average','warn'])}
  if(layer==='silver'){const tid=`silver.${t}`;
    for(const k of p.keys)out.push([tid,k,'not_null','','fail'],[tid,k,'unique',p.scd==='2'?'one current row per key':'','fail']);
    for(const r of tables[tid]||[])if(norm_type(r.ttype)!=='STRING'&&!LINEAGE.includes(r.tgt))out.push([tid,r.tgt,'castable',norm_type(r.ttype),'quarantine']);
    out.push([tid,'*','reconciliation','within 5% of the rows that arrived in Bronze','warn'])}
  if(layer==='gold'){const tid=`gold.${p.gold}`,g=p.cfg.gold||{};
    out.push([tid,'*','row_count','at least 1 row','fail'],[tid,'*','freshness','newer than 2 hours','warn']);
    if((g.type||'aggregate')==='aggregate')for(const line of lines(g.measures)){
      const m=/^\s*SUM\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)/i.exec(line);
      if(m)out.push([tid,m[2],'reconciliation',`within 0.1% of SUM(${m[1]}) in Silver`,'fail'])}}
  return out};
