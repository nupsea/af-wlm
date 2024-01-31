from dags.drafts.gx.gx_report import GxManager


def main():

    args = {
        "datasource_name": "s3_sourcing_zuora_delta",
        "bucket_name": "kayodatalake-dev-sourcing",
        "options": {"region": "ap-southeast-2"},
        "asset_name": "account",
        "s3_prefix": "zuora_aqua_obj_delta/account/silver/v1_0.0.8283_1/meta_physical_partition_valid=valid/",
        "regex": "meta_physical_partition_date=\\d{4}-\\d{2}-\\d{2}/meta_physical_partition_hh=\\d{2}/",
        "checkpoint": "zuo_checkpoint",
        "suite": "zuo_suite"
    }

    # args = {
    #     "datasource_name": "s3_sourcing_binge_vimond",
    #     "bucket_name": "kayodatalake-dev-sourcing",
    #     "options": {"region": "ap-southeast-2"},
    #     "asset_name": "player_log",
    #     "s3_prefix": "ares/vimond/player_log_event/silver/v0_0.0.0_1/meta_physical_partition_valid=valid/meta_physical_partition_date=2020-08-21/",
    #     "regex": "meta_physical_partition_hh=\\d{2}/",
    #     "checkpoint": "pl_checkpoint",
    #     "suite": "pl_suite"
    # }
    # test # "s3_prefix": "ares/vimond/player_log_event/silver/v0_0.0.0_1/meta_physical_partition_valid=valid/meta_physical_partition_date=2020-08-18/meta_physical_partition_hh=05/part-00000-6054130e-8aad-42d6-b27c-82f0d909acbb.c000.snappy.parquet",

    gx_manager = GxManager(
        args,
        engine="spark"
    )

    data_asset = gx_manager.add_parquet_asset(
        args["asset_name"],
        args["s3_prefix"],
        args["regex"]
    )

    batches = gx_manager.get_asset_batches(data_asset)

    for batch in batches:
        print(batch.batch_spec)

    batch_request_list = [batch.batch_request for batch in batches]
    print(batch_request_list)

    validations = [
        {"batch_request": br, "expectation_suite_name": args["suite"]}
        for br in batch_request_list
    ]

    checkpoint_result = gx_manager.run_checkpoint(args["checkpoint"], validations)
    print(f"CHECKPOINT Result: \n {checkpoint_result}")

    gx_manager.build_data_docs()


if __name__ == '__main__':
    main()
