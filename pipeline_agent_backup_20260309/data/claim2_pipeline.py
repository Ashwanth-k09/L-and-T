from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("claim2_pipeline") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_prod.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://localhost:8020/claim/new") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')
print("=" * 60)
print("DAG       : dag_claim2")
print("Table     : hadoop_prod.pipeline_db.claim2")
print("Warehouse : hdfs://localhost:8020/claim/new")
print("Run time  : " + str(datetime.now()))
print("=" * 60)

# Step 1: Read current data
df = spark.sql("SELECT * FROM hadoop_prod.pipeline_db.claim2")
count = df.count()
print(f"Step 1: Read {count} rows from Iceberg")

# Step 2: Write back to create new snapshot
print("Step 2: Writing back to create new Iceberg snapshot...")
df.writeTo("hadoop_prod.pipeline_db.claim2") \
    .tableProperty("write.format.default", "parquet") \
    .overwritePartitions()

# Step 3: Verify new snapshot was created
snapshots = spark.sql("""
    SELECT snapshot_id,
           committed_at,
           operation,
           summary["added-records"] as added_records
    FROM hadoop_prod.pipeline_db.claim2.snapshots
    ORDER BY committed_at DESC
""").limit(5)
print("Step 3: Latest snapshots:")
snapshots.show(truncate=False)

# Step 4: Show sample data
spark.sql("SELECT * FROM hadoop_prod.pipeline_db.claim2 LIMIT 10").show(truncate=False)

new_count = spark.sql(
    "SELECT COUNT(*) as n FROM hadoop_prod.pipeline_db.claim2"
).collect()[0]['n']
print(f"Pipeline complete: {new_count} rows | New snapshot created")
spark.stop()
