import great_expectations as gx
from pyspark.sql import SparkSession

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
    }
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

# Retrieve Data Set

pl_asset = context.get_datasource("sample_s3_ds").get_asset("player_log")
print(pl_asset.batch_request_options)

pl_batch_request = pl_asset.build_batch_request()
batches = pl_asset.get_batch_list_from_batch_request(pl_batch_request)

for batch in batches:
    print(batch.batch_spec)



