from dags.drafts.gx.gx_report import GxManager
from great_expectations.core.expectation_configuration import ExpectationConfiguration


def main():

    args = {
        "datasource_name": "sample_s3_ds",
        "bucket_name": "kayodatalake-dev-sourcing",
        "options": {"region": "ap-southeast-2"}
    }

    gx_manager = GxManager(
        args
    )

    asset_name = "player_log"
    s3_prefix = "ares/vimond/player_log_event/silver/v0_0.0.0_1/meta_physical_partition_valid=valid/meta_physical_partition_date=2020-08-21/"
    regex = "meta_physical_partition_hh=\\d{2}/"

    data_asset = gx_manager.add_parquet_asset(asset_name, s3_prefix, regex)

    batches = gx_manager.get_asset_batches(data_asset)

    for batch in batches:
        print(batch.batch_spec)

    expectations = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "tenant", "value_set": ["aresi"]},
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

    pl_batch_request = data_asset.build_batch_request()
    checkpoint_result = gx_manager.run_checkpoint("pl_checkpoint", pl_batch_request, "pl_suite")
    print(f"CHECKPOINT Result: \n {checkpoint_result}")

    gx_manager.build_data_docs()
    retrieved_checkpoint = gx_manager.get_checkpoint("pl_checkpoint")
    print(f"Retrieved CHECKPOINT: \n {retrieved_checkpoint}")


if __name__ == '__main__':
    main()
