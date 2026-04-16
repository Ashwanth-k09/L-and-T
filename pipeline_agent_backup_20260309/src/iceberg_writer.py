# src/iceberg_writer.py
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

HDFS        = "hdfs://localhost:8020"
WAREHOUSE   = f"{HDFS}/data/iceberg"
CATALOG     = "hadoop_prod"
DATABASE    = "pipeline_db"
ICEBERG_JAR = os.path.expanduser("~/iceberg-spark-runtime-3.2_2.12-1.1.0.jar")


def create_spark(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # ── Iceberg extensions ────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        # ── Iceberg hadoop catalog → points to your HDFS ─────────
        .config(
            f"spark.sql.catalog.{CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(f"spark.sql.catalog.{CATALOG}.type",      "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse",  WAREHOUSE)
        # ── HDFS connection ───────────────────────────────────────
        .config("spark.hadoop.fs.defaultFS", HDFS)
        # ── Iceberg JAR ───────────────────────────────────────────
        .config("spark.jars", ICEBERG_JAR)
        # ── Performance ───────────────────────────────────────────
        .config("spark.sql.adaptive.enabled",    "true")
        .config("spark.sql.shuffle.partitions",  "4")
        .config("spark.ui.enabled",              "false")
        .getOrCreate()
    )


def detect_partition_col(df):
    """Auto-detect best partition column"""
    # Priority 1: timestamp or date column
    for col, dtype in df.dtypes:
        if "timestamp" in dtype or "date" in dtype:
            print(f"  Auto-partition by timestamp column: '{col}'")
            return col
    # Priority 2: low-cardinality string column
    for col, dtype in df.dtypes:
        if dtype == "string" and col not in ("_source",):
            unique = df.select(col).distinct().count()
            if 2 <= unique <= 100:
                print(f"  Auto-partition by category column: '{col}' ({unique} values)")
                return col
    print("  No suitable partition column found — writing without partition")
    return None


def write_to_iceberg(
    spark,
    df,
    table_name,
    partition_col  = None,
    mode           = "overwrite",
    status_callback= None
):
    """
    Write cleaned DataFrame to HDFS in Apache Iceberg format.

    Args:
        spark          : SparkSession (reuse from cleaner)
        df             : Cleaned DataFrame
        table_name     : Target table name
        partition_col  : Column to partition by (auto-detected if None)
        mode           : overwrite or append
        status_callback: function(msg) to update UI
    """

    def update(msg):
        print(msg)
        if status_callback:
            status_callback(msg)

    full_table = f"{CATALOG}.{DATABASE}.{table_name}"
    hdfs_path  = f"{WAREHOUSE}/{DATABASE}/{table_name}"

    update(f"Target table : {full_table}")
    update(f"HDFS path    : {hdfs_path}")
    update(f"Write mode   : {mode}")
    update(f"Row count    : {df.count():,}")

    # ── Step 1: Set HDFS as default filesystem for writing ────────
    spark.conf.set("spark.hadoop.fs.defaultFS", HDFS)

    # ── Step 2: Create Iceberg namespace/database ─────────────────
    update(f"Creating namespace '{DATABASE}' if not exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DATABASE}")
    update(f"✅ Namespace ready")

    # ── Step 3: Auto-detect partition column if not given ─────────
    if not partition_col:
        partition_col = detect_partition_col(df)

    # ── Step 4: Register DataFrame as temp view ───────────────────
    df.createOrReplaceTempView("staging_data")

    # ── Step 5: Write to Iceberg ──────────────────────────────────
    if mode == "overwrite":
        update("Dropping existing table if any...")
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")

        update("Creating Iceberg table on HDFS...")

        if partition_col:
            # With partitioning
            create_sql = f"""
                CREATE TABLE {full_table}
                USING iceberg
                PARTITIONED BY ({partition_col})
                TBLPROPERTIES (
                    'write.format.default'            = 'parquet',
                    'write.parquet.compression-codec' = 'snappy'
                )
                AS SELECT * FROM staging_data
            """
        else:
            # Without partitioning
            create_sql = f"""
                CREATE TABLE {full_table}
                USING iceberg
                TBLPROPERTIES (
                    'write.format.default'            = 'parquet',
                    'write.parquet.compression-codec' = 'snappy'
                )
                AS SELECT * FROM staging_data
            """

        spark.sql(create_sql)
        update("✅ Iceberg table created successfully")

    else:
        # Append mode
        update("Appending data to existing Iceberg table...")
        df.writeTo(full_table).append()
        update("✅ Data appended successfully")

    # ── Step 6: Verify the write ──────────────────────────────────
    update("Verifying written data...")
    count = spark.sql(
        f"SELECT COUNT(*) as n FROM {full_table}"
    ).collect()[0]["n"]
    update(f"✅ Verified: {count:,} rows in Iceberg table")

    # ── Step 7: Show snapshot info ────────────────────────────────
    try:
        snap = spark.sql(f"""
            SELECT snapshot_id, committed_at, operation
            FROM {full_table}.snapshots
            ORDER BY committed_at DESC
        """).limit(1).collect()
        if snap:
            update(f"✅ Snapshot ID : {snap[0]['snapshot_id']}")
            update(f"   Committed at: {snap[0]['committed_at']}")
    except Exception:
        pass

    update(f"✅ DONE — Table: {full_table}")
    update(f"✅ DONE — HDFS : {hdfs_path}")

    return full_table, hdfs_path, count
