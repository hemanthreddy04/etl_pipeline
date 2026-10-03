/* ---------- cron helpers ---------- */
function cronField(f,v,lo){if(f==='*')return true;return f.split(',').some(p=>{const m=p.match(/^(\*|\d+)(?:-(\d+))?(?:\/(\d+))?$/);if(!m)return false;const a=m[1]==='*'?lo:+m[1];const b=m[1]==='*'?99:m[2]!=null?+m[2]:(m[3]?99:a);const s=m[3]?+m[3]:1;return v>=a&&v<=b&&(v-a)%s===0})}
function cronMatch(cron,d){const f=cron.trim().split(/\s+/);if(f.length!==5)return false;return cronField(f[0],d.getUTCMinutes(),0)&&cronField(f[1],d.getUTCHours(),0)&&cronField(f[2],d.getUTCDate(),1)&&cronField(f[3],d.getUTCMonth()+1,1)&&cronField(f[4],d.getUTCDay(),0)}
function nextRuns(cron,n=3){if(cron==='continuous')return [];const out=[];let t=Math.ceil(Date.now()/MIN)*MIN;for(let i=0;i<60*24*8&&out.length<n;i++,t+=MIN){if(cronMatch(cron,new Date(t)))out.push(t)}return out}
function cronText(c){if(c==='continuous')return 'Runs continuously';const f=c.trim().split(/\s+/);if(f.length!==5)return 'Not a valid 5 field cron expression';let m;
  if((m=f[0].match(/^\*\/(\d+)$/))&&f.slice(1).every(x=>x==='*'))return `Every ${m[1]} minutes`;
  if(/^\d+$/.test(f[0])&&f[1]==='*'&&f[2]==='*'&&f[3]==='*'&&f[4]==='*')return `Every hour at minute ${f[0]}`;
  if(/^\d+$/.test(f[0])&&(m=f[1].match(/^\*\/(\d+)$/))&&f[2]==='*')return `Every ${m[1]} hours`;
  if(/^\d+$/.test(f[0])&&/^\d+$/.test(f[1])){const at=`${f[1].padStart(2,'0')}:${f[0].padStart(2,'0')} UTC`;
    if(f[2]==='*'&&f[3]==='*'&&f[4]==='*')return `Every day at ${at}`;if(f[2]==='*'&&f[3]==='*'&&f[4]==='1-5')return `Weekdays at ${at}`;if(f[2]==='*'&&f[3]==='*')return `Weekly on day ${f[4]} at ${at}`;if(f[3]==='*'&&f[4]==='*')return `Monthly on day ${f[2]} at ${at}`}
  return 'Custom schedule'}
function quartz(c){const f=c.trim().split(/\s+/);if(f.length!==5)return '0 0 * * * ?';let [mi,h,dom,mon,dow]=f;if(dow==='*')dow='?';else dom='?';return `0 ${mi} ${h} ${dom} ${mon} ${dow}`}

/* ---------- pipeline config helpers ---------- */
function parseCols(text){return String(text||'').split('\n').map(l=>l.trim()).filter(Boolean).map(l=>{const [name,type='STRING',flag='']=l.split(':').map(x=>x.trim());return {name:name.replace(/[^A-Za-z0-9_]/g,'_'),type:type.toUpperCase()||'STRING',key:/key/i.test(flag),pii:/pii/i.test(flag)}}).filter(c=>c.name)}
function names(c){
  const t=(c.name||'dataset').replace(/[^a-z0-9_]/gi,'_').toLowerCase(),g=((c.gold&&c.gold.name)||'agg_'+t).replace(/[^a-z0-9_]/gi,'_').toLowerCase();
  const pre=S.settings.prefix||'',tc=conn(c.target)||{},sc=conn(c.conn)||{},src=(sc.endpoint||'<storage-url>').replace(/\/$/,'');
  if(c.platform==='azure'){const cat=(tc.options||{}).catalog||'main';
    return {t,g,cat,src,acct:'<account>',kv:'<secret-scope>',bronze:`${cat}.${pre}bronze.${t}_raw`,silver:`${cat}.${pre}silver.${t}`,gold:`${cat}.${pre}gold.${g}`}}
  const project=tc.endpoint||'<project-id>',bucketName=src.replace(/^gs:\/\//,'').split('/')[0];
  return {t,g,project,src,bucket:bucketName,bucketName,region:'us-central1',bronze:`${project}.${pre}bronze.${t}_raw`,silver:`${project}.${pre}silver.${t}`,gold:`${project}.${pre}gold.${g}`}}
const pyList=a=>'['+a.map(x=>`"${x}"`).join(', ')+']';

/* ---------- Bronze ---------- */
function genBronze(c){
  const n=names(c),az=c.platform==='azure',csv=c.format==='csv',full=c.pattern==='full',src=c.path||n.t,wm=c.watermark||'updated_at';
  const top=`# pipelines/bronze/bronze_${n.t}.py\n# Bronze lands the source exactly as it arrived: no casting, no business rules, lineage columns only.\n`;
  if(c.kind==='table')return top+`from pyspark.sql import ${az?'':'SparkSession, '}functions as F
${az?'':`
spark = SparkSession.builder.appName("bronze_${n.t}").getOrCreate()
`}
SOURCE = "${src}"
TARGET = "${n.bronze}"

raw = ${az?'spark.table(SOURCE)':'spark.read.format("bigquery").load(SOURCE)'}${full||!c.watermark?'':`
# Incremental: only rows newer than the highest ${wm} already in Bronze
last = ${az?'spark.table(TARGET)':'spark.read.format("bigquery").load(TARGET)'}.agg(F.max("${wm}")).first()[0] or "1900-01-01"
raw = raw.where(F.col("${wm}").cast("string") > F.lit(last))`}

bronze = (
    raw.select([F.col(c).cast("string").alias(c) for c in raw.columns])   # every column stays STRING in Bronze
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_ingest_date", F.current_date())
    .withColumn("_source_file", F.lit("table:${src}"))
)
${az?`bronze.write.mode("${full?'overwrite':'append'}").option("mergeSchema", "true").saveAsTable(TARGET)`:`(
    bronze.write.format("bigquery").option("table", TARGET)
    .option("temporaryGcsBucket", "<temp-bucket>").mode("${full?'overwrite':'append'}").save()
)`}
`;
  if(az&&c.kind==='stream')return top+`from pyspark.sql import functions as F

TARGET     = "${n.bronze}"
CHECKPOINT = "${n.src}/_checkpoints/${n.t}_raw"

raw = (
    spark.readStream.format("kafka")                    # Event Hubs speaks the Kafka protocol
    .option("kafka.bootstrap.servers", "evh-medallion-${S.env}.servicebus.windows.net:9093")
    .option("subscribe", "${src}")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", dbutils.secrets.get("${n.kv}", "evh-jaas-config"))
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 500000)             # back-pressure: cap each micro-batch
    .load()
)

bronze = raw.select(
    F.col("value").cast("string").alias("payload"),     # keep the raw event untouched
    "topic", "partition", "offset",
    F.col("timestamp").alias("_enqueued_ts"),
    F.current_timestamp().alias("_ingest_ts"),
    F.current_date().alias("_ingest_date"),
)

(
    bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)           # offsets live here, so a restart resumes where it stopped
    .partitionBy("_ingest_date")
    .trigger(processingTime="1 minute")
    .toTable(TARGET)
)
`;
  if(az&&c.kind==='jdbc')return top+`from pyspark.sql import functions as F

TARGET   = "${n.bronze}"
JDBC_URL = dbutils.secrets.get("${n.kv}", "${c.conn.replace(/_/g,'-')}-jdbc-url")   # credentials stay in Key Vault
${full?`
query = "(SELECT * FROM ${src}) AS src"                # full load: the whole table every run`:`
# High-water mark: read only rows changed since the last successful load${c.pattern==='cdc'?'\n# CDC: the change table carries _op (I, U, D) and a commit timestamp, and Bronze keeps every change':''}
last  = spark.sql(f"SELECT COALESCE(MAX(${wm}), '1900-01-01') FROM {TARGET}").first()[0]
query = f"(SELECT * FROM ${src} WHERE ${wm} > '{last}') AS src"`}

raw = (
    spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", query)
    .option("fetchsize", 10000)
    .load()
)

bronze = (
    raw.select([F.col(c).cast("string").alias(c) for c in raw.columns])   # every column stays STRING in Bronze
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_ingest_date", F.current_date())
    .withColumn("_source_file", F.lit("jdbc:${src}"))
)

(
    bronze.write.mode("${full?'overwrite':'append'}")
    .option("${full?'overwriteSchema':'mergeSchema'}", "true")            # new source columns are accepted
    .partitionBy("_ingest_date")
    .saveAsTable(TARGET)
)
`;
  if(az)return top+`from pyspark.sql import functions as F

SOURCE     = "${n.src}/${src}/"
TARGET     = "${n.bronze}"
CHECKPOINT = "${n.src}/_checkpoints/${n.t}_raw"
${full?`
raw = (
    spark.read.format("${c.format}")${csv?'\n    .option("header", "true")':''}
    .option("inferSchema", "false")                     # every column stays STRING in Bronze
    .load(SOURCE)
)`:`
raw = (
    spark.readStream.format("cloudFiles")               # Auto Loader picks up only files it has not seen
    .option("cloudFiles.format", "${c.format}")${csv?'\n    .option("header", "true")':''}
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/schema")
    .option("cloudFiles.schemaEvolutionMode", "${c.evolve}")
    .option("cloudFiles.inferColumnTypes", "false")     # every column stays STRING in Bronze
    .load(SOURCE)
)`}

bronze = (
    raw.withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_ingest_date", F.current_date())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)
${full?`
(
    bronze.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("_ingest_date")
    .saveAsTable(TARGET)
)`:`
(
    bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)           # exactly-once: progress survives restarts
    .option("mergeSchema", "true")
    .partitionBy("_ingest_date")
    .trigger(availableNow=True)                         # process what is new as one batch, then stop
    .toTable(TARGET)
)`}
`;
  if(c.kind==='stream')return `# pipelines/bronze/bronze_${n.t}.sh
# On GCP the simplest streaming Bronze is a BigQuery subscription: Pub/Sub writes each message
# straight into the Bronze table, with no Spark job to run or scale.

bq mk --table ${n.project}:bronze.${n.t}_raw \\
  data:STRING,subscription_name:STRING,message_id:STRING,publish_time:TIMESTAMP,attributes:JSON

gcloud pubsub subscriptions create ${n.t}-to-bronze \\
  --topic=${src} \\
  --bigquery-table=${n.project}:bronze.${n.t}_raw \\
  --write-metadata \\
  --dead-letter-topic=${n.t}-dead-letter \\
  --max-delivery-attempts=5
`;
  const gcsOnly=c.bronzeStore==='gcs';
  const writes=`${gcsOnly?'':`
# BigQuery is the working Bronze store, truncated on every run
(
    bronze.write.format("bigquery")
    .option("table", BQ_TABLE)
    .option("temporaryGcsBucket", "<temp-bucket>")
    .mode("overwrite")
    .save()
)
`}
# GCS keeps the full history as date-partitioned Parquet. Overwriting only this run's partition
# makes a rerun safe: the same day can be loaded twice without duplicates.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
bronze.write.mode("overwrite").partitionBy("_ingest_date").parquet(GCS_HISTORY)
`;
  const consts=`${gcsOnly?'':`BQ_TABLE    = "${n.bronze}"\n`}GCS_HISTORY = "${n.src}/_bronze_history/${n.t}_raw/"`;
  if(c.kind==='jdbc')return top+`from google.cloud import secretmanager
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("bronze_${n.t}").getOrCreate()
RUN_DATE = spark.conf.get("spark.medallion.run_date")
${consts}

def secret(name):                                       # credentials stay in Secret Manager
    client = secretmanager.SecretManagerServiceClient()
    path = f"projects/${n.project}/secrets/{name}/versions/latest"
    return client.access_secret_version(name=path).payload.data.decode()
${full?`
query = "(SELECT * FROM ${src}) AS src"                # full load: the whole table every run`:`
# Incremental: only rows changed on the run date, so a rerun reads the same slice again
query = f"(SELECT * FROM ${src} WHERE CAST(${wm} AS DATE) = DATE '{RUN_DATE}') AS src"`}

raw = (
    spark.read.format("jdbc")
    .option("url", secret("${c.conn.replace(/_/g,'-')}-jdbc-url"))
    .option("dbtable", query)
    .option("fetchsize", 10000)
    .load()
)

bronze = (
    raw.select([F.col(c).cast("string").alias(c) for c in raw.columns])   # every column stays STRING in Bronze
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_ingest_date", F.lit(RUN_DATE).cast("date"))
    .withColumn("_source_file", F.lit("jdbc:${src}"))
)
${writes}`;
  return top+`from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("bronze_${n.t}").getOrCreate()
RUN_DATE = spark.conf.get("spark.medallion.run_date")

SOURCE      = f"${n.src}/${src}/${full?'':'dt={RUN_DATE}/'}"
${consts}

raw = (
    spark.read.format("${c.format}")${csv?'\n    .option("header", "true")':''}
    .option("inferSchema", "false")                     # every column stays STRING in Bronze
    .load(SOURCE)                                       # new source columns flow through untouched
)

bronze = (
    raw.withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_ingest_date", F.lit(RUN_DATE).cast("date"))
    .withColumn("_source_file", F.input_file_name())
)
${writes}`;
}

/* ---------- Silver ---------- */
function genSilver(c){
  const n=names(c),az=c.platform==='azure',cols=parseCols(c.columns),keys=cols.filter(x=>x.key).map(x=>x.name);
  const casts=cols.filter(x=>x.type!=='STRING'),pii=c.maskPii?cols.filter(x=>x.pii).map(x=>x.name):[];
  const k=keys.length?keys:[cols[0]?cols[0].name:'id'],scd2=c.scd==='2',stream=c.kind==='stream';
  const order=cols.some(x=>x.name===c.watermark)?c.watermark:'_ingest_ts';
  const errPath=`${n.src}/_errors/silver/${n.t}/`;
  const tracked=cols.filter(x=>!x.key).map(x=>x.name);
  let s=`# pipelines/silver/silver_${n.t}.py
# Silver holds one clean, typed, deduplicated row per business key.
from pyspark.sql import ${az?'':'SparkSession, '}functions as F, Window${az?'\nfrom delta.tables import DeltaTable':''}
${az?'':`
spark = SparkSession.builder.appName("silver_${n.t}").getOrCreate()
RUN_DATE = spark.conf.get("spark.medallion.run_date")
`}
SOURCE  = "${n.bronze}"
TARGET  = "${az?n.silver:`${n.project}.silver_stg.${n.t}`}"${az?'':'        # staging table, merged into Silver by the DAG'}
ERRORS  = ${az?`"${c.onBad==='stop'?errPath:n.silver+'_quarantine'}"`:`f"${errPath}run_date={RUN_DATE}/"`}
KEYS    = ${pyList(k)}
CASTS   = {${casts.map(x=>`"${x.name}": "${x.type}"`).join(', ')}}   # every column that is not a string
COLUMNS = ${pyList(cols.map(x=>x.name))}

${az?`src = spark.table(SOURCE).where(F.col("_ingest_date") >= F.date_sub(F.current_date(), 1))   # only recent Bronze partitions`:`src = spark.read.format("bigquery").load(SOURCE)`}${stream?`
# Bronze holds the raw event as JSON text, so pull the fields out first
src = src.select(*[F.get_json_object("${az?'payload':'data'}", f"$.{name}").alias(name) for name in COLUMNS], ${az?'"_ingest_ts"':'F.col("publish_time").alias("_ingest_ts")'})`:''}

# 1) Cast. try_cast returns NULL instead of raising when a value cannot be converted
checked = src
for name, dtype in CASTS.items():
    checked = checked.withColumn(f"{name}__typed", F.expr(f"try_cast({name} AS {dtype})"))

# 2) Describe what is wrong with each row: a missing key, or a value that was present but not castable
problems = (
    [F.when(F.col(key).isNull(), F.lit(f"{key} is missing")) for key in KEYS]
    + [F.when(F.col(name).isNotNull() & F.col(f"{name}__typed").isNull(),
              F.concat(F.lit(f"{name} not castable to {dtype}: "), F.col(name)))
       for name, dtype in CASTS.items()]
)
checked = checked.withColumn("_error_reason", F.concat_ws("; ", *problems))
bad  = checked.where("_error_reason <> ''")
good = checked.where("_error_reason = ''")

`;
  s+=c.onBad==='stop'?`# 3) Bad rows stop the pipeline. They are saved with the reason first, then the task fails
#    so the orchestrator retries, alerts, and holds everything downstream.
bad_count = bad.count()
if bad_count > 0:
    bad.write.mode("overwrite").parquet(ERRORS)
    raise ValueError(f"{TARGET}: {bad_count} rows could not be cast. Evidence saved to {ERRORS}")
`:`# 3) Bad rows go to quarantine with the reason, and good rows keep moving
${az?'bad.write.mode("append").option("mergeSchema", "true").saveAsTable(ERRORS)':'bad.write.mode("overwrite").parquet(ERRORS)'}
`;
  s+=`
clean = good.select(
    *[F.col(f"{name}__typed").alias(name) if name in CASTS else F.trim(F.col(name)).alias(name) for name in COLUMNS],
    "_ingest_ts",
)
${pii.length?`
# Mask personal data before anyone downstream can read it
for name in ${pyList(pii)}:
    clean = clean.withColumn(name, F.sha2(F.lower(F.trim(F.col(name))), 256))
`:''}
# 4) Deduplicate: keep the newest record for each key
w = Window.partitionBy(*KEYS).orderBy(F.col("${order}").desc())
latest = clean.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
`;
  const on=k.map(x=>`t.${x} = s.${x}`).join(' AND ');
  if(az&&!scd2)s+=`
# 5) Upsert (SCD Type 1). MERGE is idempotent, so running the same batch twice changes nothing
(
    DeltaTable.forName(spark, TARGET).alias("t")
    .merge(latest.alias("s"), "${on}")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
`;
  if(az&&scd2)s+=`
# 5) SCD Type 2: keep history. A changed row closes the old version and opens a new one
staged  = latest.withColumn("row_hash", F.sha2(F.concat_ws("||", *${pyList(tracked)}), 256))
target  = DeltaTable.forName(spark, TARGET)
current = target.toDF().where("is_current")
changes = (
    staged.alias("s").join(current.alias("t"), KEYS, "left")
    .where("t.row_hash IS NULL OR t.row_hash <> s.row_hash")
    .select("s.*")
    .localCheckpoint()                                  # freeze the change set before the table is modified
)

(
    target.alias("t")
    .merge(changes.alias("s"), "${on} AND t.is_current")
    .whenMatchedUpdate(set={"is_current": "false", "valid_to": "s._ingest_ts"})
    .execute()
)

(
    changes.withColumn("valid_from", F.col("_ingest_ts"))
    .withColumn("valid_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
    .write.mode("append").saveAsTable(TARGET)
)
`;
  if(!az){const sx=cols.map(x=>x.name);
    s+=`${scd2?`
latest = latest.withColumn("row_hash", F.sha2(F.concat_ws("||", *${pyList(tracked)}), 256))
`:''}
# 5) Write the staging table. The DAG then merges it into Silver with one BigQuery job
(
    latest.write.format("bigquery")
    .option("table", TARGET)
    .option("temporaryGcsBucket", "<temp-bucket>")
    .mode("overwrite")
    .save()
)

# ---------- sql/merge_silver_${n.t}.sql ----------
${scd2?`-- SCD Type 2: close the versions that changed, then insert the new ones
UPDATE \`${n.silver}\` t
SET is_current = FALSE, valid_to = s._ingest_ts
FROM \`${n.project}.silver_stg.${n.t}\` s
WHERE ${on} AND t.is_current AND t.row_hash <> s.row_hash;

INSERT INTO \`${n.silver}\` (${[...sx,'_ingest_ts','row_hash','valid_from','valid_to','is_current'].join(', ')})
SELECT ${sx.map(x=>'s.'+x).join(', ')}, s._ingest_ts, s.row_hash, s._ingest_ts, CAST(NULL AS TIMESTAMP), TRUE
FROM \`${n.project}.silver_stg.${n.t}\` s
LEFT JOIN \`${n.silver}\` t ON ${on} AND t.is_current
WHERE t.${k[0]} IS NULL;`:`-- SCD Type 1 upsert. MERGE is idempotent, so a rerun changes nothing
MERGE \`${n.silver}\` t
USING \`${n.project}.silver_stg.${n.t}\` s
ON ${on}
WHEN MATCHED THEN UPDATE SET ${[...sx.filter(x=>!k.includes(x)),'_ingest_ts'].map(x=>`${x} = s.${x}`).join(', ')}
WHEN NOT MATCHED THEN INSERT ROW;`}
`}
  return s;
}

/* ---------- Gold ---------- */
function genGold(c){
  const n=names(c),az=c.platform==='azure',g=c.gold,cols=parseCols(c.columns),keys=cols.filter(x=>x.key).map(x=>x.name);
  const k=keys.length?keys:[cols[0]?cols[0].name:'id'],dims=String(g.dims||'').split(',').map(x=>x.trim()).filter(Boolean),ms=String(g.measures||'COUNT(*) AS row_count').split('\n').map(x=>x.trim().replace(/,$/,'')).filter(Boolean);
  const from=az?n.silver:`\`${n.silver}\``,tgt=az?n.gold:`\`${n.gold}\``,cur=c.scd==='2'?'\nWHERE is_current':'';
  if(g.type==='dimension'){const attrs=cols.filter(x=>!x.key).map(x=>x.name);
    return `-- pipelines/gold/${n.g}.sql
-- Gold dimension: one row per ${k.join(', ')}${c.scd==='2'?' version':''}, with a surrogate key that facts join on.
CREATE OR REPLACE TABLE ${tgt}
CLUSTER BY ${az?`(${n.t}_sk)`:`${n.t}_sk`}
AS
SELECT
  ${az?`sha2(concat_ws('||', ${[...k,...(c.scd==='2'?['CAST(valid_from AS STRING)']:[])].join(', ')}), 256)`:`FARM_FINGERPRINT(CONCAT(${[...k,...(c.scd==='2'?['valid_from']:[])].map(x=>`CAST(${x} AS STRING)`).join(", '||', ")}))`} AS ${n.t}_sk,
  ${[...k,...attrs].join(',\n  ')}${c.scd==='2'?',\n  valid_from,\n  valid_to,\n  is_current':''},
  ${az?'current_timestamp()':'CURRENT_TIMESTAMP()'} AS _built_at
FROM ${from};
`}
  const grain=[g.dateCol,...dims].filter(Boolean);
  return `-- pipelines/gold/${n.g}.sql
-- Gold aggregate, ready for dashboards. Grain: one row per ${grain.join(', ')||'table'}.
CREATE OR REPLACE TABLE ${tgt}${g.dateCol&&!az?`\nPARTITION BY ${g.dateCol}`:''}${grain.length?`\nCLUSTER BY ${az?`(${grain.slice(0,2).join(', ')})`:(dims.slice(0,2).join(', ')||g.dateCol)}`:''}
AS
SELECT
  ${[...grain,...ms].join(',\n  ')},
  ${az?'current_timestamp()':'CURRENT_TIMESTAMP()'} AS _built_at
FROM ${from}${cur}
GROUP BY ${grain.length?'ALL':'()'};
`.replace('GROUP BY ();\n',';\n');
}

/* ---------- Orchestration ---------- */
function genOrch(c){
  const n=names(c),az=c.platform==='azure',stream=c.kind==='stream',files=c.kind==='files';
  const b=`bronze_${n.t}`,s=`silver_${n.t}`,g=n.g,retries=+c.retries||0;
  if(c.orch==='dbx'){
    const task=(key,file,deps)=>({task_key:key,...(deps.length?{depends_on:deps.map(d=>({task_key:d}))}:{}),job_cluster_key:'medallion',spark_python_task:{python_file:file,source:'GIT'},max_retries:retries,min_retry_interval_millis:300000,retry_on_timeout:true,timeout_seconds:3600});
    const job={name:`${n.t}_medallion`,...(stream?{continuous:{pause_status:'UNPAUSED'}}:{schedule:{quartz_cron_expression:quartz(c.cron),timezone_id:'UTC',pause_status:'UNPAUSED'}}),max_concurrent_runs:1,
      email_notifications:{on_failure:['data-alerts@example.com']},
      git_source:{git_url:'https://github.com/example/medallion-pipelines',git_provider:'gitHub',git_branch:S.env==='prod'?'main':S.env},
      job_clusters:[{job_cluster_key:'medallion',new_cluster:{spark_version:'15.4.x-scala2.12',node_type_id:'Standard_D4ds_v5',runtime_engine:'PHOTON',autoscale:{min_workers:2,max_workers:8},data_security_mode:'SINGLE_USER'}}],
      tasks:[task(b,`pipelines/bronze/${b}.py`,[]),task(s,`pipelines/silver/${s}.py`,[b]),
        {task_key:g,depends_on:[{task_key:s}],sql_task:{file:{path:`pipelines/gold/${g}.sql`,source:'GIT'},warehouse_id:'<sql-warehouse-id>'},max_retries:retries}]};
    return ['workflows/'+n.t+'_medallion.json',JSON.stringify(job,null,2)+'\n'];
  }
  if(c.orch==='adf'){
    const act=(name,file,dep)=>({name,type:'DatabricksSparkPython',...(dep?{dependsOn:[{activity:dep,dependencyConditions:['Succeeded']}]}:{}),policy:{timeout:'0.01:00:00',retry:retries,retryIntervalInSeconds:300},typeProperties:{pythonFile:`dbfs:/pipelines/${file}`,parameters:['--run_date','@formatDateTime(pipeline().TriggerTime, \'yyyy-MM-dd\')']},linkedServiceName:{referenceName:'ls_databricks_jobs',type:'LinkedServiceReference'}});
    const f=c.cron.trim().split(/\s+/);let rec={frequency:'Day',interval:1,timeZone:'UTC'},m;
    if((m=(f[0]||'').match(/^\*\/(\d+)$/)))rec={frequency:'Minute',interval:+m[1],timeZone:'UTC'};else if(f[1]==='*')rec={frequency:'Hour',interval:1,timeZone:'UTC'};else if(/^\d+$/.test(f[1]||''))rec={frequency:'Day',interval:1,timeZone:'UTC',schedule:{hours:[+f[1]],minutes:[+f[0]||0]}};
    const pl={name:`pl_${n.t}_medallion`,properties:{activities:[act(b,`bronze/${b}.py`),act(s,`silver/${s}.py`,b),
      {name:g,type:'DatabricksNotebook',dependsOn:[{activity:s,dependencyConditions:['Succeeded']}],policy:{timeout:'0.01:00:00',retry:retries,retryIntervalInSeconds:300},typeProperties:{notebookPath:`/Repos/medallion/pipelines/gold/${g}`},linkedServiceName:{referenceName:'ls_databricks_jobs',type:'LinkedServiceReference'}},
      {name:'alert_on_failure',type:'WebActivity',dependsOn:[{activity:g,dependencyConditions:['Failed']},{activity:s,dependencyConditions:['Failed']}],typeProperties:{url:'@pipeline().globalParameters.alertWebhook',method:'POST',body:{pipeline:'@pipeline().Pipeline',runId:'@pipeline().RunId'}}}],
      annotations:['medallion',S.env]},trigger:{name:`tr_${n.t}`,properties:{type:'ScheduleTrigger',typeProperties:{recurrence:rec}}}};
    return ['adf/pl_'+n.t+'_medallion.json',JSON.stringify(pl,null,2)+'\n'];
  }
  const sched=stream?'"*/30 * * * *"':`"${c.cron}"`;
  const headPy=`# dags/${n.t}_medallion.py
from datetime import datetime, timedelta
from airflow import DAG
`;
  const args=`default_args = {
    "owner": "data-eng",
    "retries": ${retries},
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email": ["data-alerts@example.com"],
    "email_on_failure": True,
    "sla": timedelta(hours=1),
}
`;
  const dagOpen=`with DAG(
    dag_id="${n.t}_medallion",
    start_date=datetime(2026, 1, 1),
    schedule=${sched},
    catchup=False,                 # set True and clear a date range to backfill
    max_active_runs=1,             # one run at a time keeps loads in order
    default_args=default_args,
    tags=["medallion", "${S.env}"],
) as dag:
`;
  if(az)return ['dags/'+n.t+'_medallion.py',headPy+`from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

CLUSTER = {
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "Standard_D4ds_v5",
    "runtime_engine": "PHOTON",
    "autoscale": {"min_workers": 2, "max_workers": 8},
}

${args}
def spark_task(task_id, script):
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        new_cluster=CLUSTER,
        spark_python_task={"python_file": f"/Workspace/Repos/medallion/pipelines/{script}"},
    )

${dagOpen}    bronze = spark_task("${b}", "bronze/${b}.py")
    silver = spark_task("${s}", "silver/${s}.py")
    gold = DatabricksSqlOperator(
        task_id="${g}",
        databricks_conn_id="databricks_default",
        http_path="{{ var.value.databricks_sql_http_path }}",
        sql="pipelines/gold/${g}.sql",
    )

    bronze >> silver >> gold
`];
  return ['dags/'+n.t+'_medallion.py',headPy+`from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator${files?'\nfrom airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor':''}

PROJECT = "${n.project}"
REGION  = "${n.region}"
BUCKET  = "${n.bucketName}"

${args}
def spark_batch(task_id, script):
    """Run one PySpark file as a Dataproc Serverless batch."""
    return DataprocCreateBatchOperator(
        task_id=task_id,
        project_id=PROJECT,
        region=REGION,
        batch_id=task_id.replace("_", "-") + "-{{ ts_nodash | lower }}",
        batch={
            "pyspark_batch": {"main_python_file_uri": f"gs://{BUCKET}/_code/pipelines/{script}"},
            "runtime_config": {"version": "2.2", "properties": {"spark.medallion.run_date": "{{ ds }}"}},
        },
    )

def bq_sql(task_id, sql_file):
    return BigQueryInsertJobOperator(
        task_id=task_id,
        location="US",
        configuration={"query": {"query": "{% include '" + sql_file + "' %}", "useLegacySql": False}},
    )

${dagOpen}${files?`    wait = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_files",
        bucket=BUCKET,
        prefix="${c.path||n.t}/dt={{ ds }}/",
        mode="reschedule",         # frees the worker slot between checks
        poke_interval=300,
        timeout=60 * 60 * 3,
    )
`:''}${stream?'    # Bronze is filled continuously by the Pub/Sub BigQuery subscription, so the DAG starts at Silver\n':`    bronze = spark_batch("${b}", "bronze/${b}.py")\n`}    silver = spark_batch("${s}", "silver/${s}.py")
    merge  = bq_sql("merge_${s}", "sql/merge_${s}.sql")
    gold   = bq_sql("${g}", "sql/${g}.sql")

    ${[files?'wait':'',stream?'':'bronze','silver','merge','gold'].filter(Boolean).join(' >> ')}
`];
}

/* ---------- quality and CI ---------- */
function genQuality(c){
  const n=names(c),cols=parseCols(c.columns),keys=cols.filter(x=>x.key).map(x=>x.name),casts=cols.filter(x=>x.type!=='STRING');
  const k=keys.length?keys:[cols[0]?cols[0].name:'id'];
  return `# quality/${n.t}.yml
# Checks run after each layer. The severity decides what a failure does:
#   fail       stops the pipeline and alerts
#   quarantine moves the bad rows aside and continues
#   warn       alerts only

bronze.${n.t}_raw:
  - type: row_count
    min: 1
    severity: fail
  - type: schema
    on_new_column: ${c.evolve==='failOnNewColumns'?'fail':'warn'}

silver.${n.t}:
  - type: not_null
    columns: [${k.join(', ')}]
    severity: fail
  - type: unique
    columns: [${k.join(', ')}]${c.scd==='2'?'\n    where: is_current':''}
    severity: fail
${casts.map(x=>`  - type: castable
    column: ${x.name}
    to: ${x.type}
    severity: ${c.onBad==='stop'?'fail':'quarantine'}
`).join('')}  - type: reconciliation
    against: bronze.${n.t}_raw
    measure: row_count
    tolerance: 2%            # dedup and quarantine explain the gap
    severity: warn

gold.${n.g}:
  - type: freshness
    max_age: 2h
    severity: warn
  - type: row_count
    min: 1
    severity: fail
`}
function genCI(c){
  const n=names(c),az=c.platform==='azure';
  return `# .github/workflows/deploy.yml
name: medallion-pipelines
on:
  pull_request:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements-dev.txt
      - run: ruff check pipelines dags tests        # lint
      - run: pytest tests/unit                      # transformations against small fixtures
      - run: python tools/validate_quality.py quality/   # data contracts parse and reference real columns

  deploy-dev:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: dev
    permissions: { id-token: write, contents: read }   # keyless login, no stored cloud keys
    steps:
      - uses: actions/checkout@v4
${az?`      - uses: azure/login@v2
        with:
          client-id: \${{ vars.AZURE_CLIENT_ID }}
          tenant-id: \${{ vars.AZURE_TENANT_ID }}
          subscription-id: \${{ vars.AZURE_SUBSCRIPTION_ID }}
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate -t dev
      - run: databricks bundle deploy -t dev         # jobs, clusters and files from databricks.yml`:`      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: \${{ vars.GCP_WIF_PROVIDER }}
          service_account: \${{ vars.GCP_DEPLOY_SA }}
      - uses: google-github-actions/setup-gcloud@v2
      - run: gcloud storage rsync pipelines gs://${n.bucketName}/_code/pipelines --recursive
      - run: |
          gcloud composer environments storage dags import \\
            --environment composer-medallion-dev --location ${n.region} \\
            --source dags/${n.t}_medallion.py`}

  # test and prod repeat the deploy job with their own environment,
  # and the prod environment requires a reviewer's approval in GitHub.
`}

