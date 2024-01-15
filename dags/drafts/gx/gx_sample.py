import great_expectations as gx
from pyspark.sql import SparkSession
from great_expectations.core.expectation_configuration import (
    ExpectationConfiguration,
)

context = gx.get_context()

# Connect to a Data Source

datasource_name = "sample_s3_ds"
bucket_name = "kayodatalake-dev-sourcing"
options = {"region": "ap-southeast-2"}

existing_datasources = context.list_datasources()
if any(ds["name"] == datasource_name for ds in existing_datasources):
    context.delete_datasource(datasource_name)

# Dev Config # TODO Separate out.
spark = SparkSession.builder \
    .appName("GX_Trial") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.kafka:kafka-clients:3.4.0,org.apache.hadoop:hadoop-aws:3.3.3,com.amazonaws:aws-java-sdk-bundle:1.12.397") \
    .getOrCreate()

datasource = context.sources.add_spark_s3(
    name=datasource_name, bucket=bucket_name,
    spark_config={
        "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl",
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.profile.ProfileCredentialsProvider",
        "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "spark.sql.sources.partitionOverwriteMode": "dynamic"
    },
    # base_directory="/Users/anup.sethuram/fs/DATA/DEV/gx_data",
)

asset_name = "player_log"
# s3_prefix = "ares/source=vimond/name=player_log_event/version=v1/meta_physical_partition_date=2020-08-18/"
s3_prefix = "ares/vimond/player_log_event/silver/v0_0.0.0_1/meta_physical_partition_valid=valid/meta_physical_partition_date=2020-08-21/"
regex = "meta_physical_partition_hh=\d{2}/"

data_asset = datasource.add_parquet_asset(
    name=asset_name,
    batching_regex=regex,
    s3_prefix=s3_prefix,
    s3_recursive_file_discovery=True
)

exclude_column_names = [
    "client",
    "live"
    ]

# Retrieve Data Set

pl_asset = context.get_datasource("sample_s3_ds").get_asset("player_log")
print(pl_asset.batch_request_options)

pl_batch_request = pl_asset.build_batch_request()
batches = pl_asset.get_batch_list_from_batch_request(pl_batch_request)

for batch in batches:
    print(batch.batch_spec)

suite_name = "pl_suite"
if suite_name in [suite.expectation_suite_name for suite in context.list_expectation_suites()]:
    context.delete_expectation_suite(suite_name)
suite = context.add_expectation_suite(suite_name)

# Create an Expectation
expectation_configuration_2 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "tenant",
        "value_set": ["aresi"],
    },
)
suite.add_expectation(expectation_configuration=expectation_configuration_2)

expectation_configuration_3 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
        "column": "event_name",
    },
    meta={
        "notes": {
            "format": "markdown",
            "content": "Event name to be player_log_event. **Markdown** `Supported`",
        }
    },
)
suite.add_expectation(expectation_configuration=expectation_configuration_3)


context.update_expectation_suite(expectation_suite=suite)


# CHECKPOINT

checkpoint = context.add_or_update_checkpoint(
    name="pl_checkpoint",
    validations=[
        {
            "batch_request": pl_batch_request,
            "expectation_suite_name": "pl_suite",
        },
    ],
)

checkpoint_result = checkpoint.run()
print(f"CHECKPOINT Result: \n {checkpoint_result}")

context.build_data_docs()

retrieved_checkpoint = context.get_checkpoint(name="pl_checkpoint")
print(f"Retrieved CHECKPOINT: \n {retrieved_checkpoint}")




