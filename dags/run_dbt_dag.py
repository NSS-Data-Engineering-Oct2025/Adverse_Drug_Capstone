import pendulum
from airflow.sdk import dag, task


@task
def run_fct_choose_mart():
    import run_dbt_airflow

    run_dbt_airflow.run_dbt(model_selected="PICK_A_MODEL")

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)

def fct_choose_mart():
    run_fct_choose_mart()

fct_choose_mart()
