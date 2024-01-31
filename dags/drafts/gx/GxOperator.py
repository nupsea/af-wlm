from airflow.models import BaseOperator
from dags.drafts.gx.gx_report import GxManager
from great_expectations.core.expectation_configuration import ExpectationConfiguration


class GxOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):

        gx_manager = GxManager(
            params=self.params
        )

        data_asset = gx_manager.add_parquet_asset(
            self.params["asset_name"],
            self.params["s3_prefix"],
            self.params["regex"]
        )

        batches = gx_manager.get_asset_batches(data_asset)
        batch_request_list = [batch.batch_request for batch in batches]
        print(batch_request_list)

        validations = [
            {"batch_request": br, "expectation_suite_name": self.params["suite"]}
            for br in batch_request_list
        ]

        for batch in batches:
            print(batch.batch_spec)

        checkpoint_result = gx_manager.run_checkpoint("zuo_checkpoint", validations)
        print(f"CHECKPOINT Result: \n {checkpoint_result}")

        gx_manager.build_data_docs()
        retrieved_checkpoint = gx_manager.get_checkpoint("zuo_checkpoint")
        print(f"Retrieved CHECKPOINT: \n {retrieved_checkpoint}")
