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
def gx_zuo_exec():

    start = EmptyOperator(task_id="start")

    # Custom Operator call
    gx_validate = GxOperator(
        task_id="zuo_validate",
        params={
            "datasource_name": "s3_sourcing_zuora_delta",
            "bucket_name": "kayodatalake-dev-sourcing",
            "options": {"region": "ap-southeast-2"},
            "asset_name": "account",
            "s3_prefix": "zuora_aqua_obj_delta/account/silver/v1_0.0.8283_1/meta_physical_partition_valid=valid/",
            "regex": "meta_physical_partition_date=\\d{4}-\\d{2}-\\d{2}/meta_physical_partition_hh=\\d{2}/",
            "checkpoint": "zuo_checkpoint",
            "suite": "zuo_suite"
        }
    )

    complete = EmptyOperator(task_id="end")
    start >> gx_validate >> complete


gx_zuo_exec()
