import os, re, json
from groq import Groq
from pyspark.sql import SparkSession

HDFS        = "hdfs://localhost:8020"
CATALOG     = "hadoop_prod"
DATABASE    = "pipeline_db"
ICEBERG_JAR = os.path.expanduser("~/iceberg-spark-runtime-3.2_2.12-1.1.0.jar")
MYSQL_JAR   = os.path.expanduser("~/pipeline_agent/jars/mysql-connector-j-8.0.33.jar")
GROQ_MODEL  = "llama3-8b-8192"

client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))

def create_spark(warehouse):
    """Create Spark session with exact warehouse path"""
    warehouse = warehouse.rstrip("/")
    jars = ICEBERG_JAR
    if os.path.exists(MYSQL_JAR):
        jars += f",{MYSQL_JAR}"
    print(f"[Spark] Starting with warehouse: {warehouse}")
    return (SparkSession.builder
        .appName("DataChat")
        .master("local[*]")
        .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config("spark.hadoop.fs.defaultFS", HDFS)
        .config("spark.jars", jars)
        .config("spark.ui.enabled", "false")
        .getOrCreate())

def set_warehouse(spark, warehouse):
    """Update warehouse on existing spark session"""
    warehouse = warehouse.rstrip("/")
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
    print(f"[Spark] warehouse updated to: {warehouse}")
    return warehouse

def get_schema(spark, table_name, warehouse):
    """Get table schema — always set warehouse first"""
    set_warehouse(spark, warehouse)
    full = f"{CATALOG}.{DATABASE}.{table_name}"
    try:
        rows = spark.sql(f"DESCRIBE TABLE {full}").collect()
        return [{"name": r["col_name"], "type": r["data_type"]}
                for r in rows
                if r["col_name"] and not r["col_name"].startswith("#")]
    except Exception as e:
        print(f"[Schema] error: {e}")
        return []

def ask_ai(user_input, table_name, schema, warehouse, history=None):
    """Send prompt to Groq → get SQL → return structured result"""
    set_warehouse_str = warehouse.rstrip("/")
    full       = f"{CATALOG}.{DATABASE}.{table_name}"
    schema_str = ", ".join([f"{c['name']}({c['type']})" for c in schema])

    system_prompt = f"""You are an expert Apache Spark SQL engineer.

Convert user request into correct Spark SQL for this table.

TABLE    : {full}
WAREHOUSE: {set_warehouse_str}
COLUMNS  : {schema_str}

RULES:
1. ALWAYS use full table name: {full}
2. Fiscal year strings like '2021-22': use SUBSTR(year,1,4) > '2020'
3. String match: use LIKE '%value%'
4. Default LIMIT 50 unless user asks for more
5. Return ONLY valid JSON — no text before or after

RESPOND WITH ONLY THIS JSON:
{{"sql":"FULL SQL HERE","explanation":"one sentence","is_destructive":false,"warning":""}}

EXAMPLES:
User: show top 10
{{"sql":"SELECT * FROM {full} LIMIT 10","explanation":"Show first 10 rows","is_destructive":false,"warning":""}}

User: count rows
{{"sql":"SELECT COUNT(*) as total FROM {full}","explanation":"Count all rows","is_destructive":false,"warning":""}}

User: group by year
{{"sql":"SELECT year, COUNT(*) as count FROM {full} GROUP BY year ORDER BY year","explanation":"Count by year","is_destructive":false,"warning":""}}

User: show snapshots
{{"sql":"SELECT snapshot_id, committed_at, operation FROM {full}.snapshots ORDER BY committed_at DESC","explanation":"Show Iceberg snapshots","is_destructive":false,"warning":""}}

User: delete null rows
{{"sql":"DELETE FROM {full} WHERE id IS NULL","explanation":"Delete rows with null id","is_destructive":true,"warning":"Permanently deletes rows"}}
"""

    messages = [{"role": "system", "content": system_prompt}]
    if history:
        for h in history[-4:]:
            messages.append({"role": "user",      "content": h["user"]})
            messages.append({"role": "assistant",  "content": h["assistant"]})
    messages.append({"role": "user", "content":
        f"Table: {full}\nColumns: {schema_str}\nRequest: {user_input}\nJSON only:"})

    try:
        resp = client.chat.completions.create(
            model=GROQ_MODEL, messages=messages,
            temperature=0.0, max_tokens=400)
        raw    = resp.choices[0].message.content.strip()
        raw    = re.sub(r"```json|```", "", raw).strip()
        result = None
        try:
            result = json.loads(raw)
        except Exception:
            s = raw.find("{"); e = raw.rfind("}") + 1
            if s != -1 and e > s:
                try: result = json.loads(raw[s:e])
                except Exception: pass

        if result and "sql" in result:
            sql = result["sql"]
            # Ensure full table name used
            if table_name in sql and full not in sql:
                sql = sql.replace(table_name, full)
                result["sql"] = sql
            return result

        print(f"[Groq] non-JSON: {raw[:150]}")
        return _fallback(user_input, full, schema)

    except Exception as e:
        print(f"[Groq] error: {e}")
        return _fallback(user_input, full, schema)

def execute_sql(spark, sql, warehouse):
    """Execute SQL — always set correct warehouse first"""
    import io
    from contextlib import redirect_stdout
    set_warehouse(spark, warehouse)
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            df   = spark.sql(sql)
            df.show(50, truncate=False)
        out  = buf.getvalue()
        rows = df.count()
        return out, rows, None
    except Exception as e:
        return None, 0, str(e)

def _fallback(user_input, full_table, schema):
    """Fallback SQL if Groq fails"""
    u = user_input.lower()
    m = re.search(r'(\w+)\s+(?:above|greater than|>)\s*(\d+)', u)
    if m:
        col, val = m.group(1), m.group(2)
        if "year" in col:
            return {"sql": f"SELECT * FROM {full_table} WHERE SUBSTR(year,1,4) > '{val}' LIMIT 50",
                    "explanation": f"Year above {val}",
                    "is_destructive": False, "warning": ""}
        return {"sql": f"SELECT * FROM {full_table} WHERE {col} > {val} LIMIT 50",
                "explanation": f"{col} > {val}",
                "is_destructive": False, "warning": ""}
    if any(w in u for w in ["snapshot","version","history"]):
        return {"sql": f"SELECT snapshot_id,committed_at,operation FROM {full_table}.snapshots ORDER BY committed_at DESC",
                "explanation": "Show snapshots",
                "is_destructive": False, "warning": ""}
    if any(w in u for w in ["count","how many","total"]):
        return {"sql": f"SELECT COUNT(*) as total_rows FROM {full_table}",
                "explanation": "Count rows",
                "is_destructive": False, "warning": ""}
    if any(w in u for w in ["schema","describe","columns"]):
        return {"sql": f"DESCRIBE TABLE {full_table}",
                "explanation": "Show schema",
                "is_destructive": False, "warning": ""}
    n = re.search(r'\d+', user_input)
    n = n.group() if n else "10"
    return {"sql": f"SELECT * FROM {full_table} LIMIT {n}",
            "explanation": f"Show top {n} rows",
            "is_destructive": False, "warning": ""}
