from abc import ABC, abstractmethod
from typing import Union, List, Dict, Optional, Any
import json
from datetime import datetime

import pandas


class APIResponseCleanerFactory(ABC):

    single_quote_columns: list[str] = None
    
    def __init__(
            self,
            api_key: str,
            response: Union[List, Dict, None] = None,
            path_to_response: Optional[str] = None
    ) -> None:
        super().__init__()
        self.response = response if path_to_response is None else self.get_response_from_file(path_to_response)
        self.api_key = api_key

    def get_response_from_file(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)

    def replace_single_quote(
            self,
            input_str: str,
            new: str = " "
    ) -> str:
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
        data = self.response if data is None else data
        return pandas.DataFrame(data)
    
    def save_df_to_json(
            self,
            df: pandas.DataFrame,
            path: Optional[str] = None
    ) -> None:
        path = self.create_local_path() if path is None else path
        df.to_json(path, indent=4)
        print(f"The cleaned response was saved in: {path}")

    def create_local_path(self) -> str:
        now = str(datetime.now()).replace(" ", "_").replace(":", "_").replace(".", "_")
        return f"src/app/data/cleaned_response_{self.api_key}_{now}.json"


class APIGetAppsCleaner(APIResponseCleanerFactory):

    single_quote_columns = [
        'id',
        'name',
        'app_store_id',
        'bundle_id',
        'title',
        'platform',
        'optimization_event',
        'state',
    ]

    def create_local_path(self) -> str:
        now = str(datetime.now()).replace(" ", "_").replace(":", "_").replace(".", "_")
        return f"src/app/data/app_cleaned_response_{self.api_key}_{now}.json"


class APIPostReportsCleaner(APIResponseCleanerFactory):

    def get_id_from_response(self) -> str:
        return self.response.get("id")


class APIGetReportsIdStatusCleaner(APIResponseCleanerFactory):

    def check_state(self) -> bool:
        state = self.get_status()
        return self.is_ready_to_download(state)
    
    def get_status(self) -> str:
        return self.response.get("state")
    
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
        data = self.response if data is None else data
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
    cleaner = APIGetAppsCleaner(
        api_key=api_key,
        path_to_response="src/app/data/3aa24b5688-2025-06-26_21_29_38_348589.json"
    )

    print(cleaner.response)

    df = cleaner.transform_response_to_df()

    print(df.head())

    df = cleaner.replace_single_quote_in_df(df)
    path = cleaner.create_local_path()
    cleaner.save_df_to_json(df, path)


if __name__ == "__main__":
    main()