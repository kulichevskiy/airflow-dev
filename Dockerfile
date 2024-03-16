# Use the official Airflow image as a parent image
FROM apache/airflow:2.8.2

ENV AIRFLOW_HOME=/airflow

# Set the working directory in the container
WORKDIR $AIRFLOW_HOME
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./dags $AIRFLOW_HOME/dags
