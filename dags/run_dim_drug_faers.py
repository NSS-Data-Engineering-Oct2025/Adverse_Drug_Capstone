import pendulum
from airflow.sdk import dag, task


@task
def run_dim_drug_faers():
    import run_dbt_airflow

    run_dbt_airflow.run_dbt(model_selected="dim_drug_faers")

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)

def dim_drug_faers():
    run_dim_drug_faers()

dim_drug_faers()
