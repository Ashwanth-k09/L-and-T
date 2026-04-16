import os, subprocess, tempfile
import pandas as pd
import mysql.connector

# Fix pandas 2.0
if not hasattr(pd.DataFrame, 'iteritems'):
    pd.DataFrame.iteritems = pd.DataFrame.items
if not hasattr(pd.Series, 'iteritems'):
    pd.Series.iteritems = pd.Series.items

MYSQL_HOST     = "localhost"
MYSQL_PORT     = 3306
MYSQL_USER     = "debian-sys-maint"
MYSQL_PASSWORD = "1zMY11BxPZTX9zM8"
HDFS_BIN       = "/usr/local/hadoop/bin/hdfs"
HDFS           = "hdfs://localhost:8020"
CATALOG        = "hadoop_prod"
DATABASE       = "pipeline_db"
ICEBERG_JAR    = os.path.expanduser("~/iceberg-spark-runtime-3.2_2.12-1.1.0.jar")
MYSQL_JAR      = os.path.expanduser("~/pipeline_agent/jars/mysql-connector-j-8.0.33.jar")

# ── MySQL ─────────────────────────────────────────────────────────

def get_connection(db_name=None):
    cfg = dict(host=MYSQL_HOST, port=MYSQL_PORT,
               user=MYSQL_USER, password=MYSQL_PASSWORD)
    if db_name:
        cfg["database"] = db_name
    return mysql.connector.connect(**cfg)

def list_all_tables():
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN
            ('information_schema','mysql','performance_schema','sys')
            ORDER BY table_schema, table_name
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        result = {}
        for db, tbl in rows:
            result.setdefault(db, []).append(tbl)
        return result
    except Exception as e:
        return {"error": str(e)}

def search_table(table_name):
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN
            ('information_schema','mysql','performance_schema','sys')
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        for db, tbl in rows:
            if tbl.lower() == table_name.lower():
                conn2 = get_connection(db)
                cur2  = conn2.cursor()
                cur2.execute(f"SELECT COUNT(*) FROM `{tbl}`")
                row_count = cur2.fetchone()[0]
                cur2.execute(f"DESCRIBE `{tbl}`")
                columns = [r[0] for r in cur2.fetchall()]
                cur2.close(); conn2.close()
                return {"found": True, "db": db, "table": tbl,
                        "rows": row_count, "columns": columns}
        return {"found": False}
    except Exception as e:
        return {"found": False, "error": str(e)}

# ── HDFS ──────────────────────────────────────────────────────────

def hdfs_exists(path):
    local = path.replace(HDFS, "").rstrip("/") or "/"
    r = subprocess.run([HDFS_BIN, "dfs", "-test", "-d", local],
                       capture_output=True, timeout=15)
    return r.returncode == 0

def hdfs_mkdir(path):
    local = path.replace(HDFS, "").rstrip("/") or "/"
    r = subprocess.run([HDFS_BIN, "dfs", "-mkdir", "-p", local],
                       capture_output=True, timeout=30)
    return r.returncode == 0

# ── Spark ─────────────────────────────────────────────────────────

def _get_spark(warehouse):
    """
    Create Spark session with exact warehouse path.
    Stops existing session if warehouse differs — catalog config is startup-only.
    """
    from pyspark.sql import SparkSession

    warehouse = warehouse.rstrip("/")

    # Stop existing session if warehouse differs
    existing = SparkSession.getActiveSession()
    if existing:
        try:
            current = existing.conf.get(
                f"spark.sql.catalog.{CATALOG}.warehouse", "")
        except Exception:
            current = ""
        if current == warehouse:
            print(f"[Spark] Reusing session warehouse={warehouse}")
            return existing
        print(f"[Spark] Stopping session (was {current})")
        existing.stop()

    # Create HDFS path if missing
    if not hdfs_exists(warehouse):
        print(f"[HDFS] Creating: {warehouse}")
        hdfs_mkdir(warehouse)
    else:
        print(f"[HDFS] Exists  : {warehouse}")

    jars = ICEBERG_JAR
    if os.path.exists(MYSQL_JAR):
        jars += f",{MYSQL_JAR}"

    print(f"[Spark] Starting session warehouse={warehouse}")
    spark = (SparkSession.builder
        .appName("PipelineAgent")
        .master("local[2]")
        .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config("spark.hadoop.fs.defaultFS", HDFS)
        .config("spark.jars", jars)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    actual = spark.conf.get(f"spark.sql.catalog.{CATALOG}.warehouse")
    print(f"[Spark] warehouse confirmed: {actual}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")
    return spark

def _safe_df(spark, pdf):
    """
    Convert pandas to Spark via local temp CSV with file:// prefix.
    NOTE: Do NOT delete temp file until after Spark action completes.
    """
    import tempfile
    from pyspark.sql.types import (StructType, StructField,
        StringType, LongType, DoubleType, BooleanType)

    if not hasattr(pd.DataFrame, 'iteritems'):
        pd.DataFrame.iteritems = pd.DataFrame.items

    pdf = pdf.copy()

    # Clean column names
    pdf.columns = [
        str(c).strip().lower()
         .replace(" ","_").replace("-","_")
         .replace(".","_").replace("/","_")
         .replace("(","").replace(")","")
        for c in pdf.columns
    ]
    pdf = pdf.loc[:, ~pdf.columns.duplicated()]

    # Build schema + clean columns
    fields = []
    for col in pdf.columns:
        dtype = str(pdf[col].dtype)
        if dtype.startswith("int") or dtype.startswith("Int"):
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0).astype("int64")
            fields.append(StructField(col, LongType(), True))
        elif dtype.startswith("float") or dtype.startswith("Float"):
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0.0)
            fields.append(StructField(col, DoubleType(), True))
        elif dtype == "bool":
            pdf[col] = pdf[col].fillna(False)
            fields.append(StructField(col, BooleanType(), True))
        else:
            pdf[col] = pdf[col].fillna("").astype(str)
            pdf[col] = pdf[col].replace({"nan":"","None":"","NaT":"","<NA>":""})
            fields.append(StructField(col, StringType(), True))

    schema   = StructType(fields)
    tmp_path = f"/tmp/spark_upload_{os.getpid()}.csv"
    print(f"[_safe_df] rows={len(pdf)} cols={len(fields)}")

    # Write CSV — do NOT delete here, Spark reads lazily
    pdf.to_csv(tmp_path, index=False)

    # file:// forces local filesystem read, not HDFS
    df = (spark.read
            .option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .schema(schema)
            .csv(f"file://{tmp_path}"))

    print(f"[_safe_df] DataFrame created — temp file will be cleaned after write")
    # Return both df and tmp_path so caller can clean up after writeTo
    df._tmp_path = tmp_path
    return df

def _cleanup_tmp(df):
    """Delete temp CSV after Spark has finished writing to Iceberg"""
    tmp = getattr(df, '_tmp_path', None)
    if tmp and os.path.exists(tmp):
        os.unlink(tmp)
        print(f"[_safe_df] cleaned up {tmp}")

def push_mysql_to_hdfs(spark, table_info, hdfs_warehouse):
    """Pull MySQL table and write to HDFS Iceberg at user-given path"""
    db        = table_info["db"]
    tbl       = table_info["table"]
    warehouse = hdfs_warehouse.rstrip("/")
    spark     = _get_spark(warehouse)
    full_tbl  = f"{CATALOG}.{DATABASE}.{tbl}"

    print(f"[MySQL] Reading {db}.{tbl}...")
    jdbc_url   = (f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{db}"
                  f"?useSSL=false&allowPublicKeyRetrieval=true")
    jdbc_props = {"user": MYSQL_USER, "password": MYSQL_PASSWORD,
                  "driver": "com.mysql.cj.jdbc.Driver"}
    df        = spark.read.jdbc(url=jdbc_url, table=tbl, properties=jdbc_props)
    row_count = df.count()
    print(f"[MySQL] Read {row_count} rows")

    print(f"[Iceberg] Writing {full_tbl} at {warehouse}...")
    df.writeTo(full_tbl) \
      .tableProperty("write.format.default", "parquet") \
      .createOrReplace()

    actual    = spark.sql(f"SELECT COUNT(*) as n FROM {full_tbl}").collect()[0]["n"]
    hdfs_path = f"{warehouse}/{DATABASE}/{tbl}"
    print(f"[Iceberg] Done: {actual} rows at {hdfs_path}")
    return {"full_table": full_tbl, "warehouse": warehouse,
            "hdfs_path": hdfs_path, "row_count": actual,
            "table_name": tbl, "spark": spark}

def push_file_to_hdfs(spark, file_path, table_name, hdfs_warehouse):
    """Read uploaded file and write to HDFS Iceberg at user-given path"""
    warehouse = hdfs_warehouse.rstrip("/")
    spark     = _get_spark(warehouse)
    full_tbl  = f"{CATALOG}.{DATABASE}.{table_name}"

    print(f"[File] Reading: {file_path}")
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".parquet":
        df = spark.read.parquet(f"file://{file_path}")
    else:
        if ext == ".csv":
            pdf = pd.read_csv(file_path)
        elif ext in [".xlsx", ".xls"]:
            pdf = pd.read_excel(file_path)
        elif ext == ".json":
            pdf = pd.read_json(file_path)
        else:
            pdf = pd.read_csv(file_path)
        print(f"[File] Shape: {pdf.shape}")
        df = _safe_df(spark, pdf)

    print(f"[Iceberg] Writing {full_tbl} at {warehouse}...")
    df.writeTo(full_tbl) \
      .tableProperty("write.format.default", "parquet") \
      .createOrReplace()

    # Clean up temp file only AFTER Spark finishes writing
    _cleanup_tmp(df)

    actual    = spark.sql(f"SELECT COUNT(*) as n FROM {full_tbl}").collect()[0]["n"]
    hdfs_path = f"{warehouse}/{DATABASE}/{table_name}"
    print(f"[Iceberg] Done: {actual} rows at {hdfs_path}")
    return {"full_table": full_tbl, "warehouse": warehouse,
            "hdfs_path": hdfs_path, "row_count": actual,
            "table_name": table_name, "spark": spark}
