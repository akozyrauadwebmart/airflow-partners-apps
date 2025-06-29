from abc import ABC, abstractmethod
from typing import Union, List, Dict, Any, Optional
from uuid import uuid4
from datetime import datetime
from src.app import utils

import pytz

from src import config


class EnricherFactory(ABC):

    def __init__(
            self,
            data: Union[List, Dict]
    ) -> None:
        super().__init__()
        self.data = data

    @abstractmethod
    def enrich_api_response(self):
        pass


class GetReportsIdDataEnricher(EnricherFactory):

    def enrich_api_response(
            self,
            start_time: str, # example: "2020-10-01"
            end_time: str, # example: "2020-11-01"
            date_format: Optional[str] = config.LiftoffApi.PostReports.DATE_REQUEST_FORMAT
    ) -> tuple[List, List]:
        columns = self.data.get("columns")
        start_time_obj = datetime.strptime(start_time, date_format).date()
        end_time_obj = datetime.strptime(end_time, date_format).date()
        now = datetime.now(tz=pytz.timezone(config.DEFAULT_TZ))
        records = []
        for row in self.data["rows"]:
            record = {
                "id": uuid4(),
                "start_time": start_time_obj,
                "end_time": end_time_obj,
                "date": datetime.strptime(row[columns.index("date")], config.LiftoffApi.GetReportsIdData.DATE_RESPONSE_FORMAT).date() if "date" in columns else None,
                "app_id": row[columns.index("app_id")] if "app_id" in columns else None,
                "campaign_id": row[columns.index("campaign_id")] if "campaign_id" in columns else None,
                "creative_id": row[columns.index("creative_id")] if "creative_id" in columns else None,
                "country_code": row[columns.index("country_code")] if "country_code" in columns else None,
                "publisher_app_store_id": row[columns.index("publisher_app_store_id")] if "publisher_app_store_id" in columns else None,
                "publisher_name": row[columns.index("publisher_name")] if "publisher_name" in columns else None,
                "ad_format": row[columns.index("ad_format")] if "ad_format" in columns else None,
                "is_interstitial": row[columns.index("is_interstitial")] if "is_interstitial" in columns else None,
                "video_starts": row[columns.index("video_starts")] if "video_starts" in columns else None,
                "video_plays_at_25_percent": row[columns.index("video_plays_at_25_percent")] if "video_plays_at_25_percent" in columns else None,
                "video_plays_at_50_percent": row[columns.index("video_plays_at_50_percent")] if "video_plays_at_50_percent" in columns else None,
                "video_plays_at_75_percent": row[columns.index("video_plays_at_75_percent")] if "video_plays_at_75_percent" in columns else None,
                "video_completes": row[columns.index("video_completes")] if "video_completes" in columns else None,
                "spend": row[columns.index("spend")] if "spend" in columns else None,
                "impressions": row[columns.index("impressions")] if "impressions" in columns else None,
                "clicks": row[columns.index("clicks")] if "clicks" in columns else None,
                "installs": row[columns.index("installs")] if "installs" in columns else None,
                "event_name": row[columns.index("event_name")] if "event_name" in columns else None,
                "cpm": row[columns.index("cpm")] if "cpm" in columns else None,
                "cpc": row[columns.index("cpc")] if "cpc" in columns else None,
                "ctr": row[columns.index("ctr")] if "ctr" in columns else None,
                "cpi": row[columns.index("cpi")] if "cpi" in columns else None,
                "cpa": row[columns.index("cpa")] if "cpa" in columns else None,
                "skan_installs_with_no_conversion_value": row[columns.index("skan-installs-with-no-conversion-value")] if "skan-installs-with-no-conversion-value" in columns else None,
                "skan_installs_with_conversion_value": row[columns.index("skan-installs-with-conversion-value")] if "skan-installs-with-conversion-value" in columns else None,
                "created": now,
                "updated": now
            }
            records.append(record)
        return records


class GetAppsEnricher(EnricherFactory):

    def enrich_api_response(self):
        now = datetime.now(tz=pytz.timezone(config.DEFAULT_TZ))
        for item in self.data:
            item["app_id"] = item["id"]
            item["id"] = uuid4()
            item["created"] = now
            item["updated"] = now
        return self.data
    

class GetCreativesEnricher(EnricherFactory):

    def enrich_api_response(self):
        now = datetime.now(tz=pytz.timezone(config.DEFAULT_TZ))
        for row in self.data:
            row["creative_id"] = row["id"]
            row["id"] = uuid4()
            row["name"] = row.get("name")
            row["preview_url"] = row.get("preview_url")
            row["full_html_preview_url"] = row.get("full_html_preview_url")
            row["width"] = row.get("width")
            row["height"] = row.get("height")
            row["video_duration"] = row.get("video_duration")
            row["video_url"] = row.get("video_url")
            row["created"] = now
            row["updated"] = now
        return self.data
    

class GetCampaignsEnricher(EnricherFactory):

    def enrich_api_response(self) -> list[Dict]:
        now = datetime.now(tz=pytz.timezone(config.DEFAULT_TZ))
        for row in self.data:
            row["campaign_id"] = row["id"]
            row["id"] = uuid4()
            row["created"] = now
            row["updated"] = now
        return self.data


def main() -> None:
    api_key = "3aa24b5688"
    path_before = "src/app/data/creative_cleaned_data_3aa24b5688_2025_06_29_15_07_43_268974.json"

    local_connector = utils.LocalConnector()
    data = local_connector.extract_json_data(path_before)

    enricher = GetCreativesEnricher(data)
    enrihced_data = enricher.enrich_api_response()

    path_after = local_connector.create_path(api_key, "campaign", "enriched")
    local_connector.save_json_data(path_after, enrihced_data)

if __name__ == "__main__":
    main()
