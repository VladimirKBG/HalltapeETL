FROM apache/airflow:3.1.3-python3.12

COPY requirements.txt /requirements.txt

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt update && \
    apt install -y openjdk-17-jdk

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"