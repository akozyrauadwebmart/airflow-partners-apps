import os

from airflow.hooks.base import BaseHook
from clickhouse_connect.driver.client import Client
import clickhouse_connect

from src import config


class ClickHouseHook(BaseHook):

    def __init__(
            self,
            logger_name = None,
            conn_id: str = config.ClickHouseProd.AIRFLOW_CONN_ID
        ):
        super().__init__(logger_name)
        self.conn_id = conn_id
        self.connection = self.get_connection(self.conn_id)

    def get_conn(self) -> Client:
        host = self.connection.host
        port = self.connection.port
        user = self.connection.login
        password = self.connection.password
        database = self.connection.schema
        return clickhouse_connect.get_client(
            host=host,
            user=user,
            password=password,
            port=port
        )
    

def main() -> None:
    clickhouse_hook = ClickHouseHook()
    ch_client = clickhouse_hook.get_conn()
    print(ch_client.ping())


if __name__ == "__main__":
    main()