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

        for batch in batches:
            print(batch.batch_spec)

        expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={"column": "tenant", "value_set": ["ares"]},
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "event_name"},
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": "Event name to be player_log_event. **Markdown** `Supported`",
                    }
                },
            )
        ]

        gx_manager.create_or_update_expectation_suite("pl_suite", expectations)

        checkpoint_result = gx_manager.run_checkpoint("pl_checkpoint")
        print(f"CHECKPOINT Result: \n {checkpoint_result}")

        gx_manager.build_data_docs()
        retrieved_checkpoint = gx_manager.get_checkpoint("pl_checkpoint")
        print(f"Retrieved CHECKPOINT: \n {retrieved_checkpoint}")
