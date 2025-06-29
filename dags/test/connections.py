from datetime import datetime

from airflow.decorators import dag, task

from include.hooks.clickhouse import ClickHouseHook
from src import config


@dag(
    dag_id="test_clickhouse_hook",
    schedule=None,
    start_date=datetime(2025, 1, 1, 1, 1),
    catchup=False
)
def test_clickhouse_hook() -> None:

    @task
    def test_ping() -> None:
        clickhouse_hook = ClickHouseHook()
        ch_client = clickhouse_hook.get_conn()
        print("Connection to ClickHouse is ", ch_client.ping())
    test_ping()
test_clickhouse_hook()


@dag(
    dag_id="test_local_connection",
    schedule=None,
    start_date=datetime(2025, 1, 1, 1, 1),
    catchup=False
)
def test_local_connection() -> None:
    
    @task
    def test_unit() -> None:
        pass
    test_unit()
test_local_connection()