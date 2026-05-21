FROM apache/airflow:2.10.5-python3.12

# 1. Set the correct user immediately to avoid permission friction
USER airflow

WORKDIR /opt/airflow/workspace

# 2. Copy dependency files first (optimizes layer caching)
# Using :root is fine, but :airflow is safer for Mac/Windows volume syncing
COPY --chown=airflow:airflow pyproject.toml uv.lock README.md ./
COPY --chown=airflow:airflow src ./src

# 3. Use 'pip install' as the airflow user. 
# Avoid switching to ROOT unless you're installing system-level 'apt' packages.
# The '--user' flag is implied in the official Airflow image for the airflow user.
RUN uv pip install --no-cache -e .

# 4. Ensure the entrypoint stays as the airflow user
USER airflow