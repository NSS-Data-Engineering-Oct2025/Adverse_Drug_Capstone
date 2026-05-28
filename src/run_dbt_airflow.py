import subprocess

from loguru import logger


def run_dbt(model_selected: str):
    dbt_dir = "/opt/airflow/dags/adverse_drugs_dbt"
    dbt_run_cmd_list = ["dbt", "run"] if model_selected == "all" else ["dbt", "run", "--select", model_selected]

    # We chain 'clean' and 'deps' before 'run' to ensure the manifest is fresh
    commands = [
        ["dbt", "clean"],
        ["dbt", "deps"],
        dbt_run_cmd_list
    ]

    for cmd in commands:
        logger.info(f"Executing: {' '.join(cmd)}")
        # Using cwd ensures we are in the right spot
        subprocess.run(cmd, cwd=dbt_dir, check=True)

    logger.info("Pipeline updated successfully!")