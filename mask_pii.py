import hashlib
from typing import Callable
from functools import reduce
import json
import argparse
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# the functions can sit in utils,
# but for the scope of assessment keeping it simple

# function used for masking or obfuscation of pii data
def hash_value(value: str):
    # Convert the value to bytes (hash functions operate on bytes)
    value_bytes = value.encode('utf-8')

    # Compute the SHA-256 hash of the value
    sha256_hash = hashlib.sha256(value_bytes)

    # Get the hexadecimal representation of the hash
    hashed_value = sha256_hash.hexdigest()

    return hashed_value


def find_and_update_secret_in_json(data: str, key_path: str, fn: Callable):
    # Convert JSON string to dictionary
    data_dict = json.loads(data)

    # Does not work with array paths,
    # only works with plain dict paths
    keys = key_path.split('.')
    current = data_dict
    for key in keys[:-1]:
        if key in current:
            current = current[key]
        else:
            return 'None'  # Key path doesn't exist

    last_key = keys[-1]
    if last_key in current:
        current[last_key] = fn(current[last_key])  # Apply the function to update the value

        # Serialize the updated dictionary back to JSON format
        updated_json = json.dumps(data_dict)
        return updated_json
    else:
        return 'None'  # Last key doesn't exist


@F.udf(StringType())
def find_and_update_secret_in_json_udf(data: str, key_path: str):
    return find_and_update_secret_in_json(data, key_path, hash_value)


@F.udf(StringType())
def hash_value_udf(value: str):
    return hash_value(value)


# pii_definitions = json.loads("""{"pii_data":[
#     {
#         "field": "username",
#         "table": "users",
#         "type": "string"
#     },
#     {
#         "field": "metadata.secret",
#         "table": "users",
#         "type": "json"
#     }
# ]}""")


# ideally should sit in transformations module,
# leaving it here for assessment
def mask_pii(pii_column_definitions: list):
    def inner(df):
        def process_column(_df, col_def):
            field = col_def['field']
            column_type = col_def['type']

            if column_type == 'string':
                return _df.withColumn(field, hash_value_udf(F.col(field)))

            elif column_type == 'json':
                col_name = field.split('.')[0]
                field_path = '.'.join(field.split('.')[1:])
                return _df.withColumn(col_name, find_and_update_secret_in_json_udf(F.col(col_name), F.lit(field_path)))

            return _df

        return reduce(process_column, pii_column_definitions, df)

    return inner


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description="pyspark job arguments")
    parser.add_argument("--pii_map", type=str, required=True, help='required the pii_definitions to mask the data')
    args = parser.parse_args()
    logger.info(f"called with args {args}")
    pii_definitions = args.pii_map
    if pii_definitions:
        pii_definitions = json.loads(pii_definitions)
    else:
        raise Exception("pii_map args is missing for the job")
    spark = SparkSession.builder \
        .appName("Process Data") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # can be passed from job args, depends on case by case, can be read  from config as well
    bucket = "nv-interview-chaitanya"
    users_path = f"gs://{bucket}/interview_data/authentication_users/parquet/"
    groups_path = f"gs://{bucket}/interview_data/authentication_groups/parquet/"
    users_masked = f"gs://{bucket}/interview_data/authentication_users/parquet_masked/"
    groups_masked = f"gs://{bucket}/interview_data/authentication_groups/parquet_masked/"

    users_data = spark.read.format('parquet').load(users_path)
    groups_data = spark.read.format('parquet').load(groups_path)
    users_tbl_defns = []
    group_tbl_defns = []
    for defn in pii_definitions.get('pii-map'):
        if defn.get('table') == 'users':
            users_tbl_defns.append(defn)
        elif defn.get('table') == 'groups':
            group_tbl_defns.append(defn)
        else:
            pass

    users_data.transform(mask_pii(users_tbl_defns)).write.format("parquet").save(users_masked)
    groups_data.transform(mask_pii(group_tbl_defns)).write.format("parquet").save(groups_masked)
