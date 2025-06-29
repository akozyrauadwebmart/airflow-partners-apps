from abc import ABC, abstractmethod
from typing import Optional, List, Union
from requests import Response
import requests
from datetime import datetime

from src.app import utils


class APIExtractorFactory(ABC):

    def __init__(
            self,
            api_key: str,
            api_secret: str,
            url: str = None
    ) -> None:
        super().__init__()
        self.url = url
        self.api_key = api_key
        self.api_secret = api_secret
        self.auth = None
        self.response: Union[Response, None] = None

    def set_auth(
            self, 
            api_key: Optional[str] = None,
            api_secret: Optional[str] = None
    ) -> None:
        api_key = self.api_key if api_key is None else api_key
        api_secret = self.api_secret if api_secret is None else api_secret
        self.auth = (api_key, api_secret)

    @abstractmethod
    def get_response(self):
        pass


class APIGetAppsExtractor(APIExtractorFactory):
    """
    Docs:  
    https://docs.liftoff.io/advertiser/reporting_api#get-apps
    """

    def __init__(
            self,
            api_key: str,
            api_secret: str,
            url: Optional[str] = "https://data.liftoff.io/api/v1/apps"
    ) -> None:
        super().__init__(api_key, api_secret, url)

    def get_response(self) -> Response:
        if self.auth is None:
            self.set_auth()
        self.response = requests.get(url=self.url, auth=self.auth)
        return self.response
    
    def create_local_storage_path_for_response(self) -> str:
        now = str(datetime.now()).replace(" ", "_").replace(":", "_").replace(".", "_")
        return f"src/app/data/app_raw_response_{self.api_key}_{now}.json"
    

class APIPostReportsExtractor(APIExtractorFactory):
    """
    Docs:  
    https://docs.liftoff.io/advertiser/reporting_api#post-reports
    """

    def __init__(
            self,
            api_key: str,
            api_secret: str,
            url: Optional[str] = "https://data.liftoff.io/api/v1/reports"
    ) -> None:
        super().__init__(api_key, api_secret, url)
        self.url = url
        self.json_body = None
        self.headers = None

    def get_response(
            self,
            start_time: str,
            end_time: str
    ) -> Response:
        self.set_auth()
        self.set_json(start_time, end_time)
        self.set_headers()
        self.response = requests.post(
            url=self.urßl,
            json=self.json_body,
            auth=self.auth,
            headers=self.headers
        )
        return self.response
    
    def set_json(
            self,
            start_time: str, # example: "2020-10-01"
            end_time: str, # example: "2020-11-01"
            group_by: Optional[List[str]] = None,
            format: Optional[str] = "json"
    ) -> None:
        group_by = ["apps", "campaigns", "creatives", "country"] if group_by is None else group_by
        self.json_body = {
            "group_by": group_by,
            "start_time": start_time,
            "end_time": end_time,
            "format": format
        }

    def set_headers(
            self,
            content_type: Optional[str] = "application/json"
    ) -> None:
        self.headers = {"Content-Type": content_type}

    def get_report_id_from_response(self, response: Optional[Response] = None) -> str:
        response = self.response if response is None else response
        return response.json()["id"]


class APIGetReportsIdStatusExtractor(APIExtractorFactory):
    """
    Docs:  
    https://docs.liftoff.io/advertiser/reporting_api#get-reportsidstatus
    """

    def set_url(self, id: str) -> None:
        self.url = f"https://data.liftoff.io/api/v1/reports/{id}/status"

    def get_response(self, id: str):
        self.set_auth()
        self.set_url(id)
        self.response = requests.get(url=self.url, auth=self.auth)
        return self.response


class APIGetReportsIdDataExtractor(APIExtractorFactory):
    """
    Docs:  
    https://docs.liftoff.io/advertiser/reporting_api#get-reportsiddata
    """
    
    def set_url(self, id: str) -> None:
        self.url = f"https://data.liftoff.io/api/v1/reports/{id}/data"

    def get_response(self, id: str):
        self.set_auth()
        self.set_url(id)
        self.response = requests.get(url=self.url, auth=self.auth)
        return self.response


class APIGetCreativesExtractor(APIExtractorFactory):
    """
    Docs:  
    https://docs.liftoff.io/advertiser/reporting_api#get-creatives
    """

    def __init__(
            self,
            api_key: str,
            api_secret: str,
            url = "https://data.liftoff.io/api/v1/creatives"
    ) -> None:
        super().__init__(api_key, api_secret, url)

    def get_response(self) -> Response:
        self.set_auth()
        self.response = requests.get(url=self.url, auth=self.auth)
        return self.response
    

class APIGetCampaignsExtractor(APIExtractorFactory):
    """
    API Docs:  
    https://docs.liftoff.io/advertiser/reporting_api#get-campaigns
    """

    def __init__(
            self,
            api_key: str,
            api_secret: str,
            url: Optional[str] = "https://data.liftoff.io/api/v1/campaigns"
    ) -> None:
        super().__init__(api_key, api_secret, url)

    def get_response(self) -> Response:
        self.set_auth()
        self.response = requests.get(url=self.url, auth=self.auth)
        return self.response


def main() -> None:
    start_time = "2025-06-18"
    end_time = "2025-06-20"

    id = "3a78cc9136e4db3ca9ed"

    api_key = "3aa24b5688"
    api_secret = "9XZmSsbXAun9GCruQnweHQ=="

    extractor = APIGetCreativesExtractor(api_key, api_secret)
    response = extractor.get_response()

    local_connector = utils.LocalConnector()
    path = local_connector.create_path(api_key, "creative", "raw")
    local_connector.save_json_data(path, response.json())


if __name__ == "__main__":
    main()