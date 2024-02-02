from dags.drafts.gx.gx_manager import GxManager


def main():
    # #  define AWS_PROFILE='m-d-l-dev' in ENV

    args = {
        "datasource_name": "athena_sourcing",
        "asset_name": "zuora_aqua_obj_delta_account_v1",
        "checkpoint": "zuo_checkpoint",
        "suite": "zuo_suite",
        "region_name": "ap-southeast-2",
        "athena_database": "kayo_temp",
        "s3_staging_dir": "s3://aws-athena-query-results-294530054210-dev/"
    }

    # args = {
    #     "datasource_name": "athena_sourcing",
    #     "asset_name": "ares_vimond_player_log",
    #     "checkpoint": "pl_checkpoint",
    #     "suite": "pl_suite",
    #     "region_name": "ap-southeast-2",
    #     "athena_database": "kayo_temp",
    #     "s3_staging_dir": "s3://aws-athena-query-results-294530054210-dev/",
    #     "engine": "athena"
    # }

    gx_manager = GxManager(
        args
    )

    data_asset = gx_manager.add_table_asset(
        args["asset_name"]
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
