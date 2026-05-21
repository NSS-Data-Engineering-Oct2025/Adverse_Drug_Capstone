import pendulum
from airflow.sdk import dag, task


@task
def run_dim_fips_census():
    import run_dbt_airflow

    run_dbt_airflow.run_dbt(model_selected="dim_fips_census")

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)

def dim_fips_census():
    run_dim_fips_census()

dim_fips_census()
