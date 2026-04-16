import os, subprocess
from datetime import datetime

AIRFLOW_DAGS_DIR = os.path.expanduser("~/airflow/dags")
ICEBERG_JAR      = "/home/vboxuser/iceberg-spark-runtime-3.2_2.12-1.1.0.jar"
MYSQL_JAR        = "/home/vboxuser/pipeline_agent/jars/mysql-connector-j-8.0.33.jar"
SPARK_HOME       = "/usr/local/spark"
HDFS             = "hdfs://localhost:8020"
HDFS_BIN         = "/usr/local/hadoop/bin/hdfs"
CATALOG          = "hadoop_prod"
DATABASE         = "pipeline_db"

def generate_and_save_dag(table_name, warehouse, dag_name=None):
    os.makedirs(AIRFLOW_DAGS_DIR, exist_ok=True)

    warehouse  = warehouse.rstrip("/")
    dag_id     = (dag_name or f"dag_{table_name}").strip().lower() \
                  .replace(" ","_").replace("-","_")
    full_table = f"{CATALOG}.{DATABASE}.{table_name}"
    script_hdfs= f"{HDFS}/scripts/{table_name}_pipeline.py"
    today      = datetime.now()

    print(f"[DAG] dag_id    = {dag_id}")
    print(f"[DAG] table     = {table_name}")
    print(f"[DAG] warehouse = {warehouse}")

    # Pipeline script — lean Spark config for faster startup
    pipeline_script = f'''from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \\
    .appName("{table_name}_pipeline") \\
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.{CATALOG}.type", "hadoop") \\
    .config("spark.hadoop.fs.defaultFS", "{HDFS}") \\
    .config("spark.ui.enabled", "false") \\
    .config("spark.sql.shuffle.partitions", "4") \\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

warehouse = spark.conf.get("spark.sql.catalog.{CATALOG}.warehouse")

print("=" * 60)
print(f"DAG       : {dag_id}")
print(f"Table     : {full_table}")
print(f"Warehouse : {{warehouse}}")
print(f"Run time  : {{datetime.now()}}")
print("=" * 60)

# Step 1: Read
df    = spark.sql("SELECT * FROM {full_table}")
count = df.count()
print(f"Step 1: Read {{count}} rows from {{warehouse}}")

# Step 2: Write back to create new snapshot
print("Step 2: Creating new Iceberg snapshot...")
df.writeTo("{full_table}") \\
    .tableProperty("write.format.default", "parquet") \\
    .overwritePartitions()

# Step 3: Show snapshots
print("Step 3: Snapshots:")
spark.sql("""
    SELECT snapshot_id, committed_at, operation,
           summary[\\'added-records\\'] as records
    FROM {full_table}.snapshots
    ORDER BY committed_at DESC
""").show(5, truncate=False)

new_count = spark.sql("SELECT COUNT(*) as n FROM {full_table}").collect()[0]["n"]
print(f"Complete: {{new_count}} rows | snapshot created ✅")
spark.stop()
'''

    # DAG — reduced memory/cores for faster startup on 4GB VM
    dag_code = f'''from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id="{dag_id}",
        start_date=datetime({today.year}, {today.month}, {today.day}),
        schedule="@once",
        max_active_runs=1,
        catchup=False) as dag:

    {table_name} = BashOperator(
        task_id="task_{table_name}",
        bash_command=(
            "{SPARK_HOME}/bin/spark-submit "
            "--master local[2] "
            "--jars {ICEBERG_JAR},{MYSQL_JAR} "
            "--conf spark.driver.memory=1g "
            "--conf spark.executor.memory=1g "
            "--conf spark.executor.cores=1 "
            "--conf spark.sql.shuffle.partitions=4 "
            "--conf spark.ui.enabled=false "
            "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
            "--conf spark.sql.catalog.{CATALOG}=org.apache.iceberg.spark.SparkCatalog "
            "--conf spark.sql.catalog.{CATALOG}.type=hadoop "
            "--conf spark.sql.catalog.{CATALOG}.warehouse={warehouse} "
            "{script_hdfs}"
        )
    )
'''

    # Save pipeline script locally
    local_dir    = os.path.expanduser("~/pipeline_agent/data")
    os.makedirs(local_dir, exist_ok=True)
    local_script = os.path.join(local_dir, f"{table_name}_pipeline.py")
    with open(local_script, "w") as f:
        f.write(pipeline_script)
    print(f"[DAG] script saved: {local_script}")

    # Upload to HDFS
    try:
        subprocess.run([HDFS_BIN,"dfs","-mkdir","-p","/scripts"],
                       capture_output=True, timeout=15)
        r = subprocess.run(
            [HDFS_BIN,"dfs","-put","-f", local_script, script_hdfs],
            capture_output=True, timeout=30)
        print(f"[DAG] HDFS upload: {'OK' if r.returncode==0 else r.stderr.decode()}")
    except Exception as e:
        print(f"[DAG] HDFS upload error: {e}")

    # Save DAG
    dag_file = os.path.join(AIRFLOW_DAGS_DIR, f"{dag_id}.py")
    with open(dag_file, "w") as f:
        f.write(dag_code)
    print(f"[DAG] saved: {dag_file} | warehouse: {warehouse}")
    return dag_file, dag_code, dag_id
