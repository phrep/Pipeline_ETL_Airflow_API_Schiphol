FROM apache/airflow:3.1.0 AS airflow

FROM airflow

USER ROOT
# Requirements are installed here to ensure they will be cached.
COPY ./requirements/ /tmp/requirements/
COPY --chmod=+x ./scripts/airflow-init.sh /


USER airflow
# Install Python dependencies.
RUN bash -c "pip install -v --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements/requirements.txt --log /home/airflow/pip.log" 