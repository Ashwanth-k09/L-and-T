from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FIFA_to_Iceberg") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hadoop_catalog",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_catalog.warehouse",
            "hdfs://localhost:8020/warehouse/iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session Created!")

# Read from MySQL
print("📥 Reading from MySQL fifa_db.fifa_players ...")

df = spark.read.jdbc(
    url="jdbc:mysql://127.0.0.1:3306/fifa_db",
    table="fifa_players",
    properties={
        "user": "sparkuser",
        "password": "spark123",
        "driver": "com.mysql.cj.jdbc.Driver",
        "allowPublicKeyRetrieval": "true",
        "useSSL": "false"
    }
)

print(f"✅ Records read: {df.count()}")
df.printSchema()
df.show(5)

# Create namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.fifa")
print("✅ Namespace ready!")

# Create Iceberg table
spark.sql("""
    CREATE TABLE IF NOT EXISTS hadoop_catalog.fifa.fifa_players (
        player_id                INT,
        player_name              STRING,
        age                      INT,
        nationality              STRING,
        club                     STRING,
        position                 STRING,
        overall_rating           INT,
        potential_rating         INT,
        matches_played           INT,
        goals                    INT,
        assists                  INT,
        minutes_played           INT,
        market_value_million_eur DECIMAL(10,2),
        contract_years_left      INT,
        injury_prone             STRING,
        transfer_risk_level      STRING
    )
    USING iceberg
    TBLPROPERTIES (
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
""")
print("✅ Iceberg table created!")

# Write to HDFS Iceberg
print("📤 Writing to HDFS in Iceberg format...")
df.writeTo("hadoop_catalog.fifa.fifa_players").overwritePartitions()
print("✅ Write complete!")

# Verify
print("🔍 Verifying...")
spark.sql("SELECT COUNT(*) AS total FROM hadoop_catalog.fifa.fifa_players").show()
spark.sql("SELECT * FROM hadoop_catalog.fifa.fifa_players LIMIT 5").show()
spark.sql("SELECT * FROM hadoop_catalog.fifa.fifa_players.snapshots").show()

print("🎉 SUCCESS - FIFA data is now in HDFS Iceberg format!")
spark.stop()
