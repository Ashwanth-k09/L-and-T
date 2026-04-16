# src/cleaner.py
import os, json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

HDFS        = "hdfs://localhost:8020"
WAREHOUSE   = f"{HDFS}/data/iceberg"
ICEBERG_JAR = os.path.expanduser("~/iceberg-spark-runtime-3.2_2.12-1.1.0.jar")

def create_spark(app_name):
    return (SparkSession.builder.appName(app_name).master("local[*]")
        .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hadoop_prod",
            "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hadoop_prod.type",      "hadoop")
        .config("spark.sql.catalog.hadoop_prod.warehouse",  WAREHOUSE)
        # ── DO NOT set fs.defaultFS here ──
        # Setting fs.defaultFS breaks local file reads
        # We manually prefix hdfs:// when writing to HDFS
        .config("spark.jars", ICEBERG_JAR)
        .config("spark.sql.adaptive.enabled",   "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled",             "false")
        .getOrCreate()
    )

def load_file(spark, filepath):
    """
    Read from LOCAL filesystem.
    Always prefix with file:// so Spark never
    tries to resolve it against HDFS.
    """
    ext        = os.path.splitext(filepath)[-1].lower()
    local_path = "file://" + os.path.abspath(filepath)
    print(f"  Reading local file: {local_path}")

    if ext == ".csv":
        return (spark.read
                .option("header",      "true")
                .option("inferSchema", "true")
                .csv(local_path))
    elif ext == ".json":
        return spark.read.option("multiLine","true").json(local_path)
    elif ext == ".parquet":
        return spark.read.parquet(local_path)
    elif ext in (".xlsx", ".xls"):
        import pandas as pd
        pdf = pd.read_excel(filepath)
        tmp = "/tmp/_excel_tmp.csv"
        pdf.to_csv(tmp, index=False)
        return (spark.read
                .option("header",      "true")
                .option("inferSchema", "true")
                .csv("file://" + tmp))
    else:
        raise ValueError(f"Unsupported type: {ext}")

def clean_dataframe(df, progress_callback=None):
    log   = []
    total = 8

    def update(step, msg):
        log.append(msg)
        if progress_callback:
            progress_callback(step / total, f"Step {step}/{total}: {msg}")

    # 1. Standardize column names
    for col in df.columns:
        clean = (col.strip().lower()
                   .replace(" ","_").replace("-","_")
                   .replace(".","_").replace("(","")
                   .replace(")","").replace("/","_"))
        if col != clean:
            df = df.withColumnRenamed(col, clean)
    update(1, "Column names standardized")

    # 2. Drop fully null rows
    before = df.count()
    df     = df.dropna(how="all")
    update(2, f"Dropped {before - df.count()} fully empty rows")

    # 3. Remove duplicates
    before = df.count()
    df     = df.dropDuplicates()
    update(3, f"Dropped {before - df.count()} duplicate rows")

    # 4. Trim strings
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
            df = df.withColumn(col_name,
                F.when(F.col(col_name) == "", None)
                 .otherwise(F.col(col_name)))
    update(4, "Trimmed all string columns")

    # 5. Smart type casting
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            total_rows = df.filter(F.col(col_name).isNotNull()).count()
            if total_rows == 0:
                continue
            numeric = df.filter(
                F.col(col_name).cast("double").isNotNull() &
                F.col(col_name).isNotNull()
            ).count()
            if numeric / total_rows > 0.9:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
                log.append(f"  Cast '{col_name}' → Double")
    update(5, "Smart type casting done")

    # 6. Fill nulls
    for col_name, dtype in df.dtypes:
        nulls = df.filter(F.col(col_name).isNull()).count()
        if nulls == 0:
            continue
        if dtype in ("int","integer","bigint","double","float","long"):
            med  = df.approxQuantile(col_name, [0.5], 0.01)
            fill = float(med[0]) if med else 0.0
            df   = df.fillna({col_name: fill})
            log.append(f"  Filled '{col_name}' nulls with median {fill}")
        elif dtype == "string":
            df = df.fillna({col_name: "UNKNOWN"})
            log.append(f"  Filled '{col_name}' nulls with UNKNOWN")
    update(6, "Null values filled")

    # 7. Drop columns with >70% nulls
    total_rows = df.count()
    dropped    = []
    for col_name, _ in df.dtypes:
        null_pct = df.filter(F.col(col_name).isNull()).count() / max(total_rows, 1)
        if null_pct > 0.70:
            df = df.drop(col_name)
            dropped.append(col_name)
    update(7, f"Dropped high-null columns: {dropped if dropped else 'none'}")

    # 8. Add metadata
    df = df.withColumn("_ingested_at", F.current_timestamp())
    df = df.withColumn("_source",      F.lit("pipeline_agent"))
    update(8, "Metadata columns added")

    return df, log

def run_cleaning_pipeline(input_path, table_name, progress_callback=None):
    spark    = create_spark(f"Clean_{table_name}")

    # ── Read from LOCAL filesystem ────────────────────────────────
    df = load_file(spark, input_path)
    print(f"  Loaded {df.count():,} rows × {len(df.columns)} columns")

    # ── Clean ─────────────────────────────────────────────────────
    clean_df, log = clean_dataframe(df, progress_callback)

    # ── Save cleaned data to HDFS as Parquet ──────────────────────
    hdfs_staging = f"{HDFS}/data/staging/{table_name}_cleaned"
    print(f"\n  Writing cleaned data to HDFS staging: {hdfs_staging}")

    # Configure HDFS for writing only
    spark.conf.set("spark.hadoop.fs.defaultFS", HDFS)
    clean_df.write.mode("overwrite").parquet(hdfs_staging)

    print(f"  ✅ Cleaned data stored in HDFS: {hdfs_staging}")
    return spark, clean_df, log, hdfs_staging
