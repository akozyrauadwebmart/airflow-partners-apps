import os
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()


DEFAULT_TZ = "UTC"

class LiftoffApi:
    API_KEY = os.getenv("LIFTOFF_API_KEY")
    API_SECRET = os.getenv("LIFTOFF_API_SECRET")

    class PostReports:
        DATE_REQUEST_FORMAT = "%Y-%m-%d"
        DEFAULT_START_TIME = "2025-06-01"
    
    class GetReportsIdData:
        DATE_RESPONSE_FORMAT = "%Y-%m-%d"


class ClickHouseProd:
    HOST = os.getenv("CLICKHOUSE_PROD_HOST")
    PORT = os.getenv("CLICKHOUSE_PROD_PORT")
    USER = os.getenv("CLICKHOUSE_PROD_USER")
    PASSWORD = os.getenv("CLICKHOUSE_PROD_PASSWORD")
    DB_NAME = os.getenv("CLICKHOUSE_PROD_DB")
    AIRFLOW_CONN_ID = os.getenv("CONN_ID_CLICKHOUSE_PROD")


def main() -> None:
    print(ClickHouseProd.AIRFLOW_CONN_ID)


if __name__ == "__main__":
    main()