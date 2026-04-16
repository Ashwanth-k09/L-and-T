from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql import Row

# ─── Spark Session ───────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Add_Row_FIFA") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session Created!")

# ─── MySQL Connection ────────────────────────────────────────
mysql_url = "jdbc:mysql://127.0.0.1:3306/fifa_db"
mysql_properties = {
    "user": "sparkuser",
    "password": "spark123",
    "driver": "com.mysql.cj.jdbc.Driver",
    "allowPublicKeyRetrieval": "true",
    "useSSL": "false"
}

# ─── Read existing data ──────────────────────────────────────
existing_df = spark.read.jdbc(url=mysql_url, table="fifa_players", properties=mysql_properties)
print(f"📊 Row count BEFORE insert: {existing_df.count()}")

# ─── Create New Row using existing schema ────────────────────
# This avoids Decimal serialization issue completely
new_row_query = """(
    SELECT 
        2801        AS player_id,
        'Player_2801' AS player_name,
        25          AS age,
        'Spain'     AS nationality,
        'Real Madrid' AS club,
        'CAM'       AS position,
        88          AS overall_rating,
        93          AS potential_rating,
        30          AS matches_played,
        15          AS goals,
        20          AS assists,
        2700        AS minutes_played,
        150.00      AS market_value_million_eur,
        3           AS contract_years_left,
        'No'        AS injury_prone,
        'Low'       AS transfer_risk_level
) AS new_player"""

new_df = spark.read.jdbc(
    url=mysql_url,
    table=new_row_query,
    properties=mysql_properties
)

print("📋 New row to insert:")
new_df.show()

# ─── Insert into MySQL ───────────────────────────────────────
print("📤 Inserting new row into MySQL...")

new_df.write.jdbc(
    url=mysql_url,
    table="fifa_players",
    mode="append",
    properties=mysql_properties
)

print("✅ Row inserted successfully!")

# ─── Verify ──────────────────────────────────────────────────
after_df = spark.read.jdbc(url=mysql_url, table="fifa_players", properties=mysql_properties)
print(f"📊 Row count AFTER insert: {after_df.count()}")

print("🔍 Verifying new row:")
after_df.filter(after_df.player_id == 2801).show()

print("🎉 Done! New player added to MySQL fifa_players!")
spark.stop()
