from abc import ABC, abstractmethod
from typing import Optional, List
from requests import Response
import requests
import json

from src import config


class ExtractorFactory(ABC):

    def __init__(
            self,
            url = None,
            api_key: Optional[str] = None,
            api_secret: Optional[str] = None
    ) -> None:
        super().__init__()
        self.url = url
        self.api_key = config.LiftoffApi.API_KEY if api_key is None else api_key
        self.api_secret = config.LiftoffApi.API_SECRET if api_secret is None else api_secret
        self.auth = None

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


class GetAppsExtractor(ExtractorFactory):
    """
    Docs:  
    https://docs.liftoff.io/advertiser/reporting_api#get-apps
    """

    def __init__(
            self,
            url: Optional[str] = "https://data.liftoff.io/api/v1/apps",
            api_key: Optional[str] = None,
            api_secret: Optional[str] = None
    ) -> None:
        super().__init__(url, api_key, api_secret)

    def get_response(self) -> Response:
        if self.auth is None:
            self.set_auth()
        response = requests.get(url=self.url, auth=self.auth)
        return response
    

class PostReportsExtractor(ExtractorFactory):
    """
    Docs:  
    https://docs.liftoff.io/advertiser/reporting_api#post-reports
    """

    def __init__(
            self,
            url: Optional[str] = "https://data.liftoff.io/api/v1/reports",
            api_key: Optional[str] = None,
            api_secret: Optional[str] = None
    ) -> None:
        super().__init__(url, api_key, api_secret)
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
        print(self.auth)
        response = requests.post(
            url=self.url,
            json=self.json_body,
            auth=self.auth,
            headers=self.headers
        )
        return response
    
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


class GetReportsIdStatusExtractor(ExtractorFactory):
    """
    Docs:  
    https://docs.liftoff.io/advertiser/reporting_api#get-reportsidstatus
    """

    def set_url(self, id: str) -> None:
        self.url = f"https://data.liftoff.io/api/v1/reports/{id}/status"

    def get_response(self, id: str):
        self.set_auth()
        self.set_url(id)
        response = requests.get(url=self.url, auth=self.auth)
        return response


class GetReportsIdDataExtractor(ExtractorFactory):
    """
    Docs:  
    https://docs.liftoff.io/advertiser/reporting_api#get-reportsiddata
    """
    
    def set_url(self, id: str) -> None:
        self.url = f"https://data.liftoff.io/api/v1/reports/{id}/data"

    def get_response(self, id: str):
        self.set_auth()
        self.set_url(id)
        response = requests.get(url=self.url, auth=self.auth)
        return response

    
def main() -> None:
    start_time = "2025-06-18"
    end_time = "2025-06-20"
    id = "3a78cc9136e4db3ca9ed"
    extractor = GetReportsIdDataExtractor()
    response = extractor.get_response(id)
    print(response.status_code)
    print(response.json())
    with open("src\\app\\data\\status_response.json", "w", encoding="utf-8") as file:
        json.dump(response.json(), file, indent=4, ensure_ascii=False, default=str)


if __name__ == "__main__":
    main()