FROM apache/airflow:2.10.5

# Copy requirements and install
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# Create a non-root user (Airflow already runs as non-root)
# Just ensure permissions are correct
USER airflow