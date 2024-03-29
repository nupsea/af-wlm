import great_expectations as gx
from great_expectations.datasource.fluent import SQLDatasource
from pyspark.sql import SparkSession
import boto3
import os
import json
from great_expectations.data_context import EphemeralDataContext


class GxManager:
    def __init__(self, env, source, engine="athena"):
        self.env = env
        self.source = source
        self.engine = engine
        self.context = gx.get_context(
                                      # context_root_dir="/Users/anup.sethuram/DEV/LM/NSea/af-271/gx/"
        )
        # print(f" $$$ Context: {self.context} ")
        # self.context = self.context.convert_to_file_context()

        self.datasource = self._setup_datasource()

        if isinstance(self.context, EphemeralDataContext):
            print(" ----- It's Ephemeral! -------- ")

    # def _get_creds(self):
    #     profile_name = '<AWS_PROFILE>'
    #     session = boto3.Session(profile_name=profile_name)
    #     sts_client = session.client('sts')
    #     assumed_role = sts_client.assume_role(
    #         RoleArn="<YOUR-AWS-RESOURCE-ARN>",
    #         RoleSessionName="GxSession"
    #     )
    #     return assumed_role['Credentials']

    @property
    def params(self):
        with open(
                os.path.abspath(
                    f"gx/uncommitted/validation_sources/{self.engine}/{self.env}/{self.source}.json"
                )
        ) as f:
            return json.load(f)

    @property
    def conf(self):
        with open(
                os.path.abspath(
                    f"gx/uncommitted/validation_sources/{self.engine}/{self.env}/conf.json"
                )
        ) as f:
            return json.load(f)

    def _setup_datasource(self):
        print(f" *** Params: {self.params}")
        print(f" *** Conf: {self.conf}")

        datasource_name = self.params["datasource_name"]

        if self.engine == "spark":
            self._setup_spark()

            datasource = self.context.sources.add_or_update_spark_s3(
                name=datasource_name,
                bucket=self.params["bucket_name"],
                spark_config=self.spark_config,
            )
        elif self.engine == "athena":

            # Extract the temp credentials
            # credentials = self._get_creds()

            athena_connection_string = (
                "awsathena+rest://@athena."
                f"{self.conf['region_name']}.amazonaws.com:443/"
                f"{self.params['athena_database']}?s3_staging_dir={self.conf['s3_staging_dir']}"
            )

            # athena_connection_string = (
            #     f"awsathena+rest://{credentials['AccessKeyId']}:{credentials['SecretAccessKey']}@"
            #     f"athena.{self.params['region_name']}.amazonaws.com:443/"
            #     f"{self.params['athena_database']}?s3_staging_dir=s3://{self.params['s3_staging_dir']}/path/&"
            #     f"aws_session_token={credentials['SessionToken']}"
            # )

            datasource: SQLDatasource = self.context.sources.add_or_update_sql(
                datasource_name, connection_string=athena_connection_string
            )
        else:
            datasource = self.context.sources.add_or_update_pandas_s3(
                name=datasource_name, bucket=self.params["bucket_name"]
            )

        return datasource

    def _setup_spark(self):
        # TODO Refactor
        self.spark = (
            SparkSession.builder.appName("GX_Trial")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.kafka:kafka-clients:3.4.0,org.apache.hadoop:hadoop-aws:3.3.3,com.amazonaws:aws-java-sdk-bundle:1.12.397",
            )
            .getOrCreate()
        )

    @property
    def spark_config(self):
        return {
            "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl",
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.profile.ProfileCredentialsProvider",
            "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        }

    def add_table_asset(self, asset_name):
        return self.datasource.add_table_asset(asset_name, table_name=asset_name)

    def add_query_asset(self, asset_name, query):
        return self.datasource.add_query_asset(asset_name, query=query)

    def add_parquet_asset(self, asset_name, s3_prefix, regex):
        return self.datasource.add_parquet_asset(
            name=asset_name,
            batching_regex=regex,
            s3_prefix=s3_prefix,
            s3_recursive_file_discovery=True,
        )

    def add_asset(self):
        if self.engine == "athena":
            return self.add_query_asset(self.params["asset_name"], self.params["query"])
        else:
            raise Exception(f"Op not supported for engine: {self.engine}")

    def get_asset_batches(self, asset, options=None):
        batch_request = asset.build_batch_request(options=options)
        return asset.get_batch_list_from_batch_request(batch_request)

    def create_or_update_expectation_suite(self, suite_name, expectations):
        print(f" *** Suite Name: {suite_name}")
        suite = self.context.get_expectation_suite(suite_name)
        for expectation in expectations:
            suite.add_expectation(expectation)
        self.context.add_or_update_expectation_suite(expectation_suite=suite)

    # def run_checkpoint(self, checkpoint_name):
    #     checkpoint = self.get_checkpoint(checkpoint_name)
    #     return checkpoint.run()

    def run_checkpoint(self, checkpoint_name, validations, suite_name):
        checkpoint = self.context.add_or_update_checkpoint(
            name=checkpoint_name,
            validations=validations,
            # expectation_suite_name=suite_name,
            run_name_template="%Y%m%dT%H%M%S.%fZ",
        )
        return checkpoint.run()

    def build_data_docs(self):
        self.context.build_data_docs()
        self.context.open_data_docs()

    def get_checkpoint(self, checkpoint_name):
        return self.context.get_checkpoint(name=checkpoint_name)

    def exec(self):

        # my_asset = self.context.get_datasource(self.params["datasource_name"]).get_asset(self.params["asset_name"])

        data_asset = self.add_asset()

        batches = self.get_asset_batches(data_asset, options=None)

        # self.context.add_or_update_expectation_suite(self.params["suite"])

        for batch in batches:
            print(batch.batch_spec)

        batch_request_list = [batch.batch_request for batch in batches]
        print(batch_request_list)

        validations = [
            {"batch_request": br, "expectation_suite_name": self.params["suite"]}
            for br in batch_request_list
        ]

        print(f"Validations: {validations}")

        checkpoint_result = self.run_checkpoint(self.params["checkpoint"], validations, self.params["suite"])
        print(f"CHECKPOINT Result: \n {checkpoint_result}")


