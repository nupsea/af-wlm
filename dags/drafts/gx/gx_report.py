import great_expectations as gx
from pyspark.sql import SparkSession


class GxManager:
    def __init__(self, params, engine="pandas"):
        self.params = params or {}
        self.engine = engine

        self.context = gx.get_context()
        self.datasource = self._setup_datasource()

    def _setup_datasource(self):
        datasource_name = self.params["datasource_name"]

        if self.engine == "spark":
            self._setup_spark()

            datasource = self.context.sources.add_spark_s3(
                name=datasource_name,
                bucket=self.params["bucket_name"],
                spark_config=self.spark_config
            )
        else:
            datasource = self.context.sources.add_or_update_pandas_s3(
                name=datasource_name,
                bucket=self.params["bucket_name"]
            )

        return datasource

    def _setup_spark(self):
        # TODO Refactor
        self.spark = SparkSession.builder \
            .appName("GX_Trial") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.kafka:kafka-clients:3.4.0,org.apache.hadoop:hadoop-aws:3.3.3,com.amazonaws:aws-java-sdk-bundle:1.12.397") \
            .getOrCreate()


    @property
    def spark_config(self):
        return {
            "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl",
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.profile.ProfileCredentialsProvider",
            "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            "spark.sql.sources.partitionOverwriteMode": "dynamic"
        }

    def add_parquet_asset(self, asset_name, s3_prefix, regex):
        return self.datasource.add_parquet_asset(
            name=asset_name,
            batching_regex=regex,
            s3_prefix=s3_prefix,
            s3_recursive_file_discovery=True
        )

    def get_asset_batches(self, asset):
        batch_request = asset.build_batch_request()
        return asset.get_batch_list_from_batch_request(batch_request)

    def create_or_update_expectation_suite(self, suite_name, expectations):
        suite = self.context.add_or_update_expectation_suite(suite_name)
        for expectation in expectations:
            suite.add_expectation(expectation)
        self.context.update_expectation_suite(expectation_suite=suite)

    def run_checkpoint(self, checkpoint_name):
        checkpoint = self.get_checkpoint(checkpoint_name)
        return checkpoint.run()

    def build_data_docs(self):
        self.context.build_data_docs()

    def get_checkpoint(self, checkpoint_name):
        return self.context.get_checkpoint(name=checkpoint_name)
