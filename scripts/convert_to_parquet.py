import sys
import json
import yaml
from pyspark.sql import SparkSession

# Read config path from argument
config_path = sys.argv[1]
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# Start Spark session pointed at local HDFS
spark = SparkSession.builder \
    .appName("HDFSToParquetSync") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

sync_db_path = config["sync_db_path"]
row_counts = {}

print("\n========== Starting HDFS to Parquet Conversion ==========\n")

for db in config["databases"]:
    db_name = db["name"]
    db_path = db["hdfs_path"]

    for table in db["tables"]:
        table_name = table["name"]
        fmt = table["format"]
        source_path = f"{db_path}/{table_name}"
        target_path = f"{sync_db_path}/{table_name}"

        print(f"Processing {db_name}.{table_name} [{fmt}] ...")

        # Read based on format
        if fmt == "csv":
            delimiter = table.get("delimiter", ",")
            df = spark.read \
                .option("header", "true") \
                .option("sep", delimiter) \
                .option("inferSchema", "true") \
                .csv(source_path)
        elif fmt == "parquet":
            df = spark.read.parquet(source_path)
        else:
            print(f"  WARNING: Unknown format {fmt}, skipping.")
            continue

        source_count = df.count()
        print(f"  Source row count : {source_count}")

        # Write as Parquet to sync DB
        df.write.mode("overwrite").parquet(target_path)

        # Verify by reading back
        verify_df = spark.read.parquet(target_path)
        target_count = verify_df.count()
        print(f"  Target row count : {target_count}")

        if source_count != target_count:
            print(f"  WARNING: Row count mismatch for {table_name}!")
        else:
            print(f"  Validation PASSED for {table_name}")

        row_counts[f"{db_name}.{table_name}"] = {
            "source_count": source_count,
            "target_count": target_count,
            "status": "OK" if source_count == target_count else "MISMATCH"
        }

        print()

# Write metadata summary to local file (agent reads this)
metadata_path = "/tmp/row_counts.json"
with open(metadata_path, "w") as f:
    json.dump(row_counts, f, indent=2)

print("========== Conversion Complete ==========")
print(f"Row count summary written to {metadata_path}")
print(json.dumps(row_counts, indent=2))

spark.stop()
