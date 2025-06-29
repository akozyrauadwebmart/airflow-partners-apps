from abc import ABC
from typing import Union, Optional, Any
from src.app import utils

import pandas


class APIResponseCleanerFactory(ABC):

    single_quote_columns: list[str] = None
    
    def __init__(
            self,
            api_key: str,
            data: Union[dict, list]
    ) -> None:
        super().__init__()
        self.api_key = api_key
        self.data = data

    def replace_single_quote_in_data(self) -> list[dict[str]]:
        for item in self.data:
            for column in self.single_quote_columns:
                item[column] = self.replace_single_quote(item.get(column))
        return self.data
    
    def replace_single_quote(
            self,
            input_str: str,
            new: str = " "
    ) -> str:
        if input_str is None:
            return input_str
        return input_str.replace("'", new)
    
    def replace_single_quote_in_df(
            self,
            df: Optional[pandas.DataFrame] = None,
            columns: Optional[list[str]] = None
    ) -> pandas.DataFrame:
        df = self.transform_response_to_df(self.response)
        columns = self.single_quote_columns if columns is None else columns
        for column in columns:
            df[column] = df[column].str.replace("'", " ")
        return df
    
    def transform_response_to_df(self, data: list[dict[Any]] | None = None) -> pandas.DataFrame:
        data = self.data if data is None else data
        return pandas.DataFrame(data)


class APIGetAppsCleaner(APIResponseCleanerFactory):

    single_quote_columns = [
        'id',
        'name',
        'app_store_id',
        'bundle_id',
        'title',
        'platform',
        'optimization_event_id',
        'optimization_event_name',
        'state',
    ]


class APIPostReportsCleaner(APIResponseCleanerFactory):
    pass


class APIGetReportsIdStatusCleaner(APIResponseCleanerFactory):

    def check_state(self) -> bool:
        state = self.get_status()
        return self.is_ready_to_download(state)
    
    def get_status(self) -> str:
        return self.data.get("state")
    
    def is_ready_to_download(self, state: str) -> bool:
        return state == "completed"
    

class APIGetReportsIdDataCleaner(APIResponseCleanerFactory):

    single_quote_columns = (
        'app_id',
        'campaign_id',
        'creative_id',
        'country_code',
        'publisher_app_store_id',
        'publisher_name',
        'ad_format',
        'ad_format'
    )

    def transform_response_to_df(self, data: list[dict[Any]] | None = None) -> pandas.DataFrame:
        data = self.data if data is None else data
        df = pandas.DataFrame(
            columns=data["columns"],
            data=data["rows"]
        )
        return df


class APIGetCreativesCleaner(APIResponseCleanerFactory):

    single_quote_columns = (
        'id',
        'name',
        'preview_url',
        'full_html_preview_url',
        'creative_type',
        'state',
        'video_url'
    )

    t_replace_columns = [
        "created_at"
    ]

    def clean(self) -> Union[dict, list]:
        self.data = self.replace_single_quote_in_data()
        return self.replace_t_in_str_datatimes()

    def replace_t_in_str_datatimes(
            self,
            new: Optional[str] = " ",
            columns: Optional[list] = None
    ) -> list:
        columns = self.t_replace_columns if columns is None else columns
        for item in self.data:
            for column in columns:
                item[column] = item[column].replace("T", new)
        return self.data


class APIGetCampaignsCleaner(APIResponseCleanerFactory):

    single_quote_columns = (
        'id',
        'app_id',
        'name',
        'campaign_type',
        'tracker_type',
        'min_os_version',
        'max_os_version',
        'state',
        'demand_product'
    )


def main() -> None:
    api_key = "3aa24b5688"
    path_before = "src/app/data/creative_raw_data_3aa24b5688_2025_06_29_15_05_17_186848.json"

    local_connector = utils.LocalConnector()
    data = local_connector.extract_json_data(path_before)
    
    cleaner = APIGetCreativesCleaner(api_key, data)
    cleaned_data = cleaner.clean()

    path_after = local_connector.create_path(api_key, "creative", "cleaned")
    local_connector.save_json_data(path_after, cleaned_data)


if __name__ == "__main__":
    main()