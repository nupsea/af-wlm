from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable

"""
 Dag and Task definitions using decorators.
"""


@dag(
    dag_id="book_c2_p2",
    description=" DAG and Task definitions using decorators. Shows Xcom passage ",
    schedule="@daily",
    start_date=datetime(2023, 9, 21),
    dagrun_timeout=timedelta(minutes=10),
    tags=[
        "xcom",
        "decorator",
        "concepts",
    ],
    max_active_runs=1,
    catchup=False
)
def books_dag():
    @task(
        task_id="get_novels"
    )
    def _fetch():
        novels = Variable.get("novels_data_source", deserialize_json=True)
        novels_id = novels["id"]
        novels_file_path = novels["file_path"]
        print(f"Novels ID: {novels_id} Path: {novels_file_path}")
        return novels  # return avoids explicit xcom_push through ti context

    @task(
        task_id="process_novels"
    )
    def _process(**context):  # Pass the context to access TaskInstance object to pull xcoms
        novels_json = context["ti"].xcom_pull(task_ids="get_novels")
        print(f".. Processing for Novels: {novels_json}")

    # Task relationship
    _fetch() >> _process()


# Invoke the dag
books_dag()
