from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from airflow.decorators import dag
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

from dags.drafts.gx.GxOperator import GxOperator

MY_GX_DATA_CONTEXT = "include/great_expectations"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def gx_pl_exec():

    start = EmptyOperator(task_id="start")

    # gx_validate = GreatExpectationsOperator(
    #     task_id="gx_validate",
    #     data_context_root_dir="include/great_expectations",
    #     dataframe_to_validate=df,
    #     execution_engine="SparkDFExecutionEngine",
    #     expectation_suite_name="strawberry_suite",
    #     return_json_dict=True,
    # )

    # Custom Operator call
    gx_validate = GxOperator(
        task_id="gx_validate",
        params={
            "datasource_name": "s3_sourcing_binge_vimond",
            "bucket_name": "kayodatalake-dev-sourcing",
            "options": {"region": "ap-southeast-2"},
            "asset_name": "player_log",
            "s3_prefix": "ares/vimond/player_log_event/silver/v0_0.0.0_1/meta_physical_partition_valid=valid/meta_physical_partition_date=2020-08-21/",
            "regex": "meta_physical_partition_hh=\\d{2}/",
            # "regex": "meta_physical_partition_hh=\\d{2}/.*.parquet",
            "checkpoint": "pl_checkpoint",
            "suite": "pl_suite"
        }
    )

    complete = EmptyOperator(task_id="end")
    start >> gx_validate >> complete


gx_pl_exec()
