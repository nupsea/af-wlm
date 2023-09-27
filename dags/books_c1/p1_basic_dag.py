from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def _fetch():
    """
    Function invoked by the PythonOperator.
    Reads a variable (JSON) defined in the Airflow Admin Console and prints it.
    """
    novels = Variable.get("novels_data_source", deserialize_json=True)
    novels_id = novels["id"]
    novels_file_path = novels["file_path"]
    print(f"Novels ID: {novels_id} Path: {novels_file_path}")


with DAG(
        dag_id="book_c1_p1",
        description="Basic dag params with scheduling.",
        schedule=timedelta(minutes=240),  # or schedule="@hourly" : Frequency of dag execution
        start_date=datetime(2023, 9, 20),   # starting date time of dag.
        dagrun_timeout=timedelta(minutes=10),  # expected to complete the dag in that time.
        tags=[
            "concepts",
            "dag_params",
            "variables"
        ],
        max_active_runs=2,  # Determine how many back-fill dags could be run simultaneously
        catchup=False
) as dag:

    # Defines a Python Operator task
    fetch_novels = PythonOperator(
        task_id="novels",
        python_callable=_fetch
    )


