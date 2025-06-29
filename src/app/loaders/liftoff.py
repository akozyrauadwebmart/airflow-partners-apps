from abc import ABC
from typing import Optional, List, Dict, Union, Any
from datetime import datetime

from clickhouse_connect.driver.client import Client
import pandas

from src.db.clickhouse import models
from src.db.clickhouse import client_init
from src.app import utils


class LoaderFactory(ABC):

    str_to_datetime_obj_columns = []
    
    def __init__(
            self,
            api_key: str,
            data: Union[dict, list],
            model: Optional[models.ModelFactory] = None,
            ch_client: Optional[Client] = None
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.data = data
        self.ch_client = self.get_default_ch_client() if ch_client is None else ch_client
    
    def get_default_ch_client(self) -> Client:
        return client_init.create_client()
    
    def transform_datetime_to_obj(
            self,
            columns: list[str] | None = None,
            data: list[Dict] | None = None
    ) -> list[Dict]:
        data = self.data if data is None else data
        columns = self.str_to_datetime_obj_columns if columns is None else columns
        for record in data:
            for column in columns:
                record[column] = datetime.fromisoformat(record[column])
        return data
    
    def load_data_to_clickhouse(
            self,
            table_name: Optional[str] = None,
            db_name: Optional[str] = None,
            data: Optional[pandas.DataFrame] = None,
            delete_data_before_insert: bool = False
    ) -> None:
        db_name = self.create_st_liftoff_db_name() if db_name is None else db_name
        table_name = self.model.TABLE_NAME if table_name is None else table_name
        data = self.data if data is None else data
        df = pandas.DataFrame(data)

        if not df.empty and delete_data_before_insert is True:
            self.delete_full_data(table_name, db_name)

        self.ch_client.insert_df(
            database=db_name,
            table=table_name,
            df=df
        )
        print("Data is loaded successfully")
    
    def delete_full_data(
            self,
            table_name: Optional[str] = None,
            db_name: Optional[str] = None
    ) -> None:
        table_name = self.model.TABLE_NAME if table_name is None else table_name
        db_name = self.create_st_liftoff_db_name() if db_name is None else db_name
        query = f"""
            DELETE FROM {db_name}.{table_name}
            WHERE TRUE
        """
        self.ch_client.query(query=query)
        print("Data was deleted successfully")

    def create_st_liftoff_db_name(self, api_key: Optional[str] = None) -> str:
        api_key = self.api_key if api_key is None else api_key
        return f"st_liftoff_{api_key}"
    

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
            api_key: str,
            data: list[dict[Any]],
            model: models.ModelFactory = models.LiftoffStagingAppModel,
            ch_client = None
    ) -> None:
        super().__init__(api_key, data, model, ch_client)


class GetCreativesStagingLoader(LoaderFactory):

    def __init__(
            self,
            api_key: str,
            data: Union[dict, list],
            model: Optional[models.ModelFactory] = models.LiftoffStagingCreativeModel,
            ch_client: Optional[Client] = None
    ) -> None:
        super().__init__(api_key, data, model, ch_client)


class GetCampaignsStagingLoader(LoaderFactory):

    str_to_datetime_obj_columns = [
        "created_at"
    ]

    def __init__(
            self,
            api_key: str,
            data: Union[dict, list],
            model: Optional[models.ModelFactory] = models.LiftoffStagingCampaignModel,
            ch_client: Optional[Client] = None
    ) -> None:
        super().__init__(api_key, data, model, ch_client)


def main() -> None:
    api_key = "3aa24b5688"
    path_before = "src/app/data/campaign_enriched_data_3aa24b5688_2025_06_29_15_10_51_166504.json"

    local_connector = utils.LocalConnector()
    data = local_connector.extract_json_data(path_before)

    loader = GetCreativesStagingLoader(api_key, data)
    data = loader.transform_datetime_to_obj()
    loader.load_data_to_clickhouse(data=data, delete_data_before_insert=True)


if __name__ == "__main__":
    main()