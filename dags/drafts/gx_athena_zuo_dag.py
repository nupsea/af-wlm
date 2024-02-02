from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from airflow.decorators import dag

from dags.drafts.gx.gx_operator import GxOperator

MY_GX_DATA_CONTEXT = "include/great_expectations"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def gx_athena_zuo_exec():

    start = EmptyOperator(task_id="start")

    # Custom Operator call
    gx_validate = GxOperator(
        task_id="zuo_validate",
        params={
            "datasource_name": "athena_sourcing",
            "asset_name": "zuora_aqua_obj_delta_account_v1",
            "checkpoint": "zuo_checkpoint",
            "suite": "zuo_suite",
            "region_name": "ap-southeast-2",
            "athena_database": "kayo_temp",
            "s3_staging_dir": "s3://aws-athena-query-results-294530054210-dev/",
            "engine": "athena"
        }
    )

    complete = EmptyOperator(task_id="end")
    start >> gx_validate >> complete


gx_athena_zuo_exec()
