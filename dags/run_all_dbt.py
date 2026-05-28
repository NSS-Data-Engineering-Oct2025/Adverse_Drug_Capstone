import pendulum
from airflow.sdk import dag, task


@task
def run_all_dbt():
    import run_dbt_airflow

    run_dbt_airflow.run_dbt(model_selected="all")

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)

def dbt_run_all():
    run_all_dbt()

dbt_run_all()
