from datetime import datetime

import great_expectations as gx
from great_expectations.core import ExpectationSuiteValidationResult, RunIdentifier
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import DataContextConfig, InMemoryStoreBackendDefaults, \
    CheckpointValidationConfig
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier
from great_expectations.datasource.fluent import SQLDatasource

import boto3
import os
import json
from great_expectations.data_context import EphemeralDataContext
from contextlib import contextmanager


@contextmanager
def temp_env_variable(key, value):
    """
    Temporarily set an environment variable and restore it to its original state after exiting the context.
    """
    # Remember the original value (if any)
    original_value = os.environ.get(key)
    # Set the new value
    os.environ[key] = value
    try:
        # Yield control back to the context block
        yield
    finally:
        # Revert the environment variable to its original state
        if original_value is None:
            # If the variable was not set originally, delete it
            del os.environ[key]
        else:
            # Otherwise, restore the original value
            os.environ[key] = original_value


class GxManager:
    def __init__(self, env, source, engine="athena", conf=None, params=None):
        if params is None:
            params = {}
        self.env = env
        self.source = source
        self.engine = engine
        # self.context = gx.get_context(
        #                               # context_root_dir="/Users/anup.sethuram/DEV/LM/NSea/af-271/gx/"
        # )

        self.data_context_config = DataContextConfig(
            datasources={},
            store_backend_defaults=InMemoryStoreBackendDefaults(),
            data_docs_sites={
                "default_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "/Users/anup.sethuram/tmp_gx_data_docs/",
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder",
                        "show_cta_footer": True,
                    },
                }
            },
            validation_operators={
                "default": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [
                        {
                            "name": "store_validation_result",
                            "action": {"class_name": "StoreValidationResultAction"},
                        },
                        {
                            "name": "update_data_docs",
                            "action": {"class_name": "UpdateDataDocsAction"},
                        },
                    ],
                },
            },
            # anonymous_usage_statistics={
            #     "enabled": False
            # }
        )
        self.context = EphemeralDataContext(
            project_config=self.data_context_config
        )
        # print(f" $$$ Context: {self.context} ")
        # self.context = self.context.convert_to_file_context()
        if params:
            self.params = params
        else:
            self.params = self._read_params()

        if conf:
            self.conf = conf
        else:
            self.conf = self._read_conf()

        self.datasource = self._setup_datasource()

        if isinstance(self.context, EphemeralDataContext):
            print(" ----- It's Ephemeral! -------- ")

    def _get_creds(self):
        # martian_role = "arn:aws:iam::711866164579:role/group-datalake-powerdev"
        martian_role = "arn:aws:iam::711866164579:role/nodes.cluster.platform-data-lake-nonprod.com.au"
        # martian_role = "arn:aws:iam::711866164579:role/athena-dev-access-role"

        profile_name = 'martian-data-lake-nonprod'
        session = boto3.Session()
        sts_client = session.client('sts')
        assumed_role = sts_client.assume_role(
            RoleArn=martian_role,
            RoleSessionName="GxSession"
        )
        return assumed_role['Credentials']

    def _read_params(self):
        with open(
                os.path.abspath(
                    f"gx/uncommitted/validation_sources/{self.engine}/{self.env}/{self.source}.json"
                )
        ) as f:
            return json.load(f)

    def _read_conf(self):
        with open(
                os.path.abspath(
                    f"gx/uncommitted/validation_sources/{self.engine}/{self.env}/conf.json"
                )
        ) as f:
            return json.load(f)

    def _read_suite(self):
        with open(
                os.path.abspath(
                    f"gx/expectations/{self.params.get('suite')}.json"
                )
        ) as f:
            return json.load(f)

    def _setup_datasource(self):
        print(f" *** Params: {self.params}")
        print(f" *** Conf: {self.conf}")

        datasource_name = self.params["datasource_name"]

        # if self.engine == "spark":
        #     self._setup_spark()
        #
        #     datasource = self.context.sources.add_or_update_spark_s3(
        #         name=datasource_name,
        #         bucket=self.params["bucket_name"],
        #         spark_config=self.spark_config,
        #     )
        if self.engine == "athena":

            # Extract the temp credentials
            # credentials = self._get_creds()

            athena_connection_string = (
                "awsathena+rest://@athena."
                f"{self.conf['region_name']}.amazonaws.com:443/"
                f"{self.params['athena_database']}?s3_staging_dir={self.conf['s3_staging_dir']}"
            )

            # athena_connection_string = (
            #     f"awsathena+rest://{credentials['AccessKeyId']}:{credentials['SecretAccessKey']}@"
            #     f"athena.{self.conf['region_name']}.amazonaws.com:443/"
            #     f"{self.params['athena_database']}?s3_staging_dir=s3://{self.conf['s3_staging_dir']}/path/&"
            #     f"aws_session_token={credentials['SessionToken']}"
            # )

            datasource: SQLDatasource = self.context.sources.add_or_update_sql(
                datasource_name, connection_string=athena_connection_string
            )
            # datasource.get_execution_engine()
        else:
            datasource = self.context.sources.add_or_update_pandas_s3(
                name=datasource_name, bucket=self.params["bucket_name"]
            )

        return datasource

    # def _setup_spark(self):
    #     # TODO Refactor
    #     self.spark = (
    #         SparkSession.builder.appName("GX_Trial")
    #         .config(
    #             "spark.jars.packages",
    #             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.kafka:kafka-clients:3.4.0,org.apache.hadoop:hadoop-aws:3.3.3,com.amazonaws:aws-java-sdk-bundle:1.12.397",
    #         )
    #         .getOrCreate()
    #     )

    # @property
    # def spark_config(self):
    #     return {
    #         "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl",
    #         "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    #         "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.profile.ProfileCredentialsProvider",
    #         "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    #         "spark.sql.sources.partitionOverwriteMode": "dynamic",
    #     }

    def add_table_asset(self, asset_name):
        return self.datasource.add_table_asset(asset_name, table_name=asset_name)

    def add_query_asset(self, asset_name, query):
        print(r" ######## Query to execute: ", query)
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
        elif self.engine == "pandas":
            return self.add_parquet_asset(self.params["asset_name"], self.params["s3_prefix"],
                                          self.params.get("regex", ""))
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

    # def update_and_run_checkpoint(self, checkpoint_name):
    #     checkpoint = self.get_checkpoint(checkpoint_name)
    #     return checkpoint.run()

    def update_and_run_checkpoint(self, checkpoint_name, validations, suite_name):
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

    # def exec(self):
    #
    #     self.context = self.context.convert_to_file_context()
    #     data_asset = self.add_asset()
    #     suite_dict = self._read_suite()
    #
    #     # Add an Expectation Suite
    #     self.context.add_or_update_expectation_suite(
    #         expectation_suite_name=self.params["suite"],
    #         expectations=suite_dict['expectations']
    #     )
    #
    #     batches = self.get_asset_batches(data_asset, options=None)
    #
    #     for batch in batches:
    #         print(f"Batch spec: {batch.batch_spec}")
    #
    #     batch_request_list = [batch.batch_request for batch in batches]
    #     print(batch_request_list)
    #
    #     validations = []
    #     for (i, br) in enumerate(batch_request_list):
    #         validations.append(
    #             CheckpointValidationConfig(
    #                 id=i,
    #                 expectation_suite_name=self.params["suite"],
    #                 batch_request=br
    #             ))
    #     print(f"Validations: {validations}")
    #
    #     checkpoint_result = self.update_and_run_checkpoint(self.params["checkpoint"], validations, self.params["suite"])
    #     print(f"CHECKPOINT Result: \n {checkpoint_result}")

    def exec(self):
        self.context = self.context.convert_to_file_context()
        data_asset = self.add_asset()
        suite_dict = self._read_suite()

        # Add an Expectation Suite
        self.context.add_or_update_expectation_suite(
            expectation_suite_name=self.params["suite"],
            expectations=suite_dict['expectations']
        )

        # Get batches
        batches = self.get_asset_batches(data_asset, options=None)

        # Iterate over each batch and run the checkpoint separately
        for i, batch in enumerate(batches):
            batch_request = batch.batch_request
            validations = [
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": self.params["suite"],
                }
            ]

            # Create a checkpoint config
            checkpoint_config = {
                "name": f"{self.params['checkpoint']}_{batch.id}_{i}",
                "config_version": 1.0,
                "class_name": "Checkpoint",
                "run_name_template": f"%Y%m%d-%H%M%S",
                "validations": validations,
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction"
                        }
                    },
                    {
                        "name": "store_evaluation_params",
                        "action": {
                            "class_name": "StoreEvaluationParametersAction"
                        }
                    },
                    {
                        "name": "update_data_docs",
                        "action": {
                            "class_name": "UpdateDataDocsAction"
                        }
                    }
                ]
            }

            # Create or update the checkpoint
            self.context.add_or_update_checkpoint(**checkpoint_config)

            if self.engine == "pandas":
                run_name = f'File_{i+1}_{os.path.basename(batch.batch_spec["path"])}'
            else:
                run_name = f'{self.engine}_Run_{i+1}'

            # Run checkpoint with the current batch validation
            checkpoint_result = self.context.run_checkpoint(
                checkpoint_name=f"{self.params['checkpoint']}_{batch.id}_{i}",
                run_name=run_name
            )
            print(f"CHECKPOINT Result for batch {batch.id}: \n {checkpoint_result}")

            # results = json.loads(checkpoint_result["run_results"]["validation_result"]["results"])
            # for result in results:
            #     if not result.get("success", False):
            #         raise RuntimeError(f" GX Error:\n {result['exception_info']['exception_message']}")

            first_key = next(iter(checkpoint_result['run_results']))
            results = checkpoint_result['run_results'][first_key]['validation_result']['results']
            for result in results:
                if result['exception_info'].get("raised_exception"):
                    raise RuntimeError(f" GX Error =>\n\t{result['exception_info']['exception_message']}")

        # Build and open data docs
        self.context.build_data_docs()
        self.context.open_data_docs()

