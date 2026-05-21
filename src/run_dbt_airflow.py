import subprocess

from loguru import logger


def run_dbt(model_selected: str):
    dbt_dir = "/opt/airflow/dags/adverse_drugs_dbt"

    # We chain 'clean' and 'deps' before 'run' to ensure the manifest is fresh
    commands = [
        ["dbt", "clean"],
        ["dbt", "deps"],
        ["dbt", "run", "--select", model_selected]
    ]

    for cmd in commands:
        logger.info(f"Executing: {' '.join(cmd)}")
        # Using cwd ensures we are in the right spot
        subprocess.run(cmd, cwd=dbt_dir, check=True)

    logger.info("Pipeline updated successfully!")