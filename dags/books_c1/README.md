# Authoring DAGs
Airflow is an orchestration tool for data pipelines. 

## Chapter 1
Introduces how to define and schedule dags with a basic structure. Provides constructs for a 
basic DAG authoring.

### Page 1: DAG Definition and Schedule

- Airflow looks for `dag` or `airflow` to parse dag files.
- Use ` .airflowignore ` similar to ` .gitignore ` to skip parsing certain extensions, files, folders.

- The airflow dag with `dag_id` starts its first run **after** the `start_date` + `schedule_interval`
- cron vs timedelta
    - cron expression is stateless
        - "@daily" -> "0 0 * * *"  => At every day it triggers at the same time of the day.
    - timedelta is stateful
        - `timedelta(days=1)`
        - **Use case**: Relative runs and cases where it is difficult to specify it in cron. _Ex: Every 4 days_ .
      

### Page 2: Task Idempotence & Determinism

- Ensure you make your tasks
  - produce the same result if run at different times with the same input.
  - has the same side effect when run multiple times.
  ```python
  pg = PostresOperator("table_create", "CREATE TABLE IF NOT EXISTS ...")
  md = BashOperator("new_dir", "mkdir -p /usr/tmp/dummy")
  ```

### Page 3: Backfill 
- Run to import historical data or re-run erroneous ones.
  ```
  airflow dags backfill <dag_id> -s [start_date] -e [end_date]	
  ```
- Airflow Web:
    - *Browse -> Dag Runs -> Filter DAG (id, start_date, end_date) => Clear Runs*
- Ensure `catchup=False` to prevent accidental over-runs.
- Leverage `max_active_runs=<N> # N=[1,2 ..]` to determine how many backfill dags you want to be running simultaneously
- Pre-req: Tasks need to be idempotent and deterministic
