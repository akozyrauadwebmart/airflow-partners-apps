# FROM apache/airflow:3.0.1

# ENV AIRFLOW_VERSION=3.0.1
# ENV PYTHON_VERSION=3.12
# ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# COPY requirements.txt .

# RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" && \
#     pip install -r requirements.txt --constraint "${CONSTRAINT_URL}"

FROM apache/airflow:3.0.1

COPY requirements.txt .

ENV AIRFLOW_VERSION=3.0.1
ENV PYTHON_VERSION=3.12
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install -r requirements.txt --constraint "${CONSTRAINT_URL}" || pip install -r requirements.txt