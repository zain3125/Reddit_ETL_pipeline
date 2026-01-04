FROM apache/airflow:2.7.1-python3.9

COPY requirements.txt /requirements.txt

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir --user -r /requirements.txt