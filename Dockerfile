# Imagen de Airflow + dependencias del pipeline.
# Para actualizar Airflow cambiá la versión en las DOS líneas (FROM y ARG).
FROM apache/airflow:3.1.0-python3.12

ARG AIRFLOW_VERSION=3.1.0

COPY requirements.txt /tmp/requirements.txt
# Fijamos apache-airflow para que pip no lo actualice/rompa al instalar dependencias.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/airflow/src
