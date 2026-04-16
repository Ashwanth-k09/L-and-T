from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("fifa_players_pipeline") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_prod.type", "hadoop") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020") \
    .config("spark.ui.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

warehouse = spark.conf.get("spark.sql.catalog.hadoop_prod.warehouse")

print("=" * 60)
print(f"DAG       : dag_landt")
print(f"Table     : hadoop_prod.pipeline_db.fifa_players")
print(f"Warehouse : {warehouse}")
print(f"Run time  : {datetime.now()}")
print("=" * 60)

# Step 1: Read
df    = spark.sql("SELECT * FROM hadoop_prod.pipeline_db.fifa_players")
count = df.count()
print(f"Step 1: Read {count} rows from {warehouse}")

# Step 2: Write back to create new snapshot
print("Step 2: Creating new Iceberg snapshot...")
df.writeTo("hadoop_prod.pipeline_db.fifa_players") \
    .tableProperty("write.format.default", "parquet") \
    .overwritePartitions()

# Step 3: Show snapshots
print("Step 3: Snapshots:")
spark.sql("""
    SELECT snapshot_id, committed_at, operation,
           summary[\'added-records\'] as records
    FROM hadoop_prod.pipeline_db.fifa_players.snapshots
    ORDER BY committed_at DESC
""").show(5, truncate=False)

new_count = spark.sql("SELECT COUNT(*) as n FROM hadoop_prod.pipeline_db.fifa_players").collect()[0]["n"]
print(f"Complete: {new_count} rows | snapshot created ✅")
spark.stop()
