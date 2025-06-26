from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Union
import json
import os
from datetime import datetime

from clickhouse_connect.driver.client import Client
import pandas

from src.db.clickhouse import models
from src.db.clickhouse import client_init


class LoaderFactory(ABC):

    def __init__(
            self,
            data: Optional[Union[List, Dict]] = None,
            path_to_data: Optional[str] = None,
            model: models.ModelFactory = None,
            ch_client: Optional[Client] = None
    ) -> None:
        self.data = self.get_json_data_from_local_storage(path_to_data) if data is None else data
        self.ch_client = self.get_default_ch_client() if ch_client is None else ch_client
        self.model = model

    def get_json_data_from_local_storage(
            self,
            path: str,
            datetime_columns: Optional[list[str]] = None
    ) -> None:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)
        if datetime_columns is not None:
            data = self.transform_datetime_to_obj(data, datetime_columns)
        return data
    
    def transform_datetime_to_obj(self, json_data: list[Dict], columns: list[str]) -> list[Dict]:
        for record in json_data:
            for column in columns:
                record[column] = datetime.fromisoformat(record[column])
        return json_data
    
    def get_default_ch_client(self) -> Client:
        return client_init.create_client()
    
    def create_st_liftoff_db_name(self, api_key) -> str:
        return f"st_liftoff_{api_key}"
    
    def load_data_to_clickhouse(
            self,
            data: Optional[pandas.DataFrame] = None,
            db_name: Optional[str] = None,
            table_name: Optional[str] = None
    ) -> None:
        db_name = self.model.DB_NAME if db_name is None else db_name
        table_name = self.model.TABLE_NAME if table_name is None else table_name
        df = pandas.DataFrame(self.data) if data is None else data
        self.ch_client.insert_df(
            database=db_name,
            table=table_name,
            df=df
        )
        print("The data is loaded successfully")
    
    def remove_file(self, path: str) -> None:
        os.remove(path)
    

class GetReportsIdDataStagingLoader(LoaderFactory):

    def __init__(
            self,
            data: Optional[Union[List, Dict]] = None,
            path_to_data: Optional[str] = None,
            model: models.ModelFactory = models.LiftoffStagingReportModel,
            ch_client: Optional[Client] = None
    ) -> None:
        super().__init__(data, path_to_data, model, ch_client)


class GetAppsStagingLoader(LoaderFactory):

    def __init__(
            self,
            data: Optional[Union[List, Dict]] = None,
            path_to_data: Optional[str] = None,
            model: models.ModelFactory = models.LiftoffStagingAppModel,
            ch_client: Optional[Client] = None
    ) -> None:
        super().__init__(data, path_to_data, model, ch_client)


class GetCreativesStagingLoader(LoaderFactory):

    def __init__(
            self,
            data: Optional[Union[List, Dict]] = None,
            path_to_data: Optional[str] = None,
            model: models.ModelFactory = models.LiftoffStagingCreativeModel,
            ch_client: Optional[Client] = None
    ) -> None:
        super().__init__(data, path_to_data, model, ch_client)


class GetCampaignsStagingLoader(LoaderFactory):

    def __init__(
            self,
            data: Optional[Union[List, Dict]] = None,
            path_to_data: Optional[str] = None,
            model: models.ModelFactory = models.LiftoffStagingCampaignModel,
            ch_client: Optional[Client] = None
    ) -> None:
        super().__init__(data, path_to_data, model, ch_client)


def main() -> None:
    loader = GetCampaignsStagingLoader(path_to_data="src/app/data/response_eniched.json")
    data = loader.get_json_data_from_local_storage(
        "src/app/data/response_eniched.json"
    )
    df = pandas.DataFrame(data)
    print(df['state_last_changed_at'].dtypes)
    db_name = loader.create_st_liftoff_db_name("60d83c8c0e")
    loader.load_data_to_clickhouse(df, db_name)


if __name__ == "__main__":
    main()
        