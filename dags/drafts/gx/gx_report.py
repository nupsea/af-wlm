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
        if any(ds["name"] == datasource_name for ds in self.context.list_datasources()):
            self.context.delete_datasource(datasource_name)

        datasource = None
        if self.engine == "spark":
            self._setup_spark()

            datasource = self.context.sources.add_spark_s3(
                name=datasource_name,
                bucket=self.params["bucket_name"],
                spark_config=self.spark_config
            )
        else:
            datasource = self.context.sources.add_pandas_s3(
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
        if suite_name in [suite.expectation_suite_name for suite in self.context.list_expectation_suites()]:
            self.context.delete_expectation_suite(suite_name)
        suite = self.context.add_expectation_suite(suite_name)
        for expectation in expectations:
            suite.add_expectation(expectation)
        self.context.update_expectation_suite(expectation_suite=suite)

    def run_checkpoint(self, checkpoint_name, batch_request, suite_name):
        checkpoint = self.context.add_or_update_checkpoint(
            name=checkpoint_name,
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": suite_name,
                },
            ],
        )
        return checkpoint.run()

    def build_data_docs(self):
        self.context.build_data_docs()

    def get_checkpoint(self, checkpoint_name):
        return self.context.get_checkpoint(name=checkpoint_name)
