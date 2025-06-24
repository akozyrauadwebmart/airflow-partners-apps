from typing import Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from src import config


def create_client(
        host: Optional[str] = config.ClickHouseProd.HOST,
        port: Optional[str] = config.ClickHouseProd.PORT,
        user: Optional[str] = config.ClickHouseProd.USER,
        password: Optional[str] = config.ClickHouseProd.PASSWORD,
        db_name: Optional[str] = config.ClickHouseProd.DB_NAME
) -> Client:
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password
    )
    return client


def main() -> None:
    ch_client = create_client()
    print(ch_client.ping())


if __name__ == "__main__":
    main()