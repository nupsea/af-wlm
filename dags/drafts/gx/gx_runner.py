from dags.drafts.gx.gx_manager import GxManager
import os
import json


def main():

    env = "nonprod"
    source = "s3_sourcing_binge_vimond"
    engine = "spark"

    with open(os.path.abspath(f'../../../gx/uncommitted/validation_sources/{engine}/{env}/{source}.json')) as f:
        args = json.load(f)

    gx_manager = GxManager(
        args
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
