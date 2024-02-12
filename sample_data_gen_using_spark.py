import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

users_data = {
    'id': [1, 2],
    'username': ['kat@email.com', 'mani@email.com'],
    'metadata': [
        '{"secret": "gQTKNMafpw", "provider": "google-oauth2"}',
        '{"secret": "sjmaIS2EmA", "provider": "basic-auth"}'
    ],
    'created_at': ['2023-09-01 08:01:02', '2023-09-01 08:01:03']
}
groups_data = {
    'id': [1, 2],
    'name': ['SUPER_USER', 'DEFAULT_USER'],
    'description': ['Full access to all functions.', 'Initial role assigned to a new user.'],
    'created_at': ['2015-01-01 04:05:06', '2015-01-01 05:06:07']
}
ts_format = '%Y-%m-%d %H:%M:%S'
gcs_bucket = 'nv-interview-chaitanya'
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Process Data") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schema for users_data
users_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("metadata", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Define schema for groups_data
groups_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Create DataFrames from dictionaries
users_df = spark.createDataFrame(
    zip(users_data['id'], users_data['username'], users_data['metadata'],
        [datetime.strptime(ts, ts_format) for ts in users_data['created_at']]),
    schema=users_schema
)

groups_df = spark.createDataFrame(
    zip(groups_data['id'], groups_data['name'], groups_data['description'],
        [datetime.strptime(ts, ts_format) for ts in groups_data['created_at']]),
    schema=groups_schema
)

users_path = f"gs://{gcs_bucket}/interview_data/authentication_users/"
groups_path = f"gs://{gcs_bucket}/interview_data/authentication_groups/"

# Write DataFrames to Parquet format in GCS
users_df.write.format("parquet").mode("append").save(
    os.path.join(users_path, "parquet"))
groups_df.write.format("parquet").mode("append").save(
    os.path.join(groups_path, "parquet"))

# Convert Parquet files to Delta format
# appending for the scope of assignment
users_df.write.format("delta").mode("append").save(os.path.join(users_path, "delta"))
groups_df.write.format("delta").mode("append").save(os.path.join(groups_path, "delta"))

# Stop SparkSession
spark.stop()
