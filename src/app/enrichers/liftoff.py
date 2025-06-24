from abc import ABC, abstractmethod
from typing import Union, List, Dict, Any, Optional
from uuid import uuid4
from datetime import datetime
import json

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

    def save_data_to_local_storage(self, data: Union[List, Dict], path: str) -> None:
        with open(path, "r", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False, default=str)


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


def main() -> None:
    with open("status_response.json", "r", encoding="utf-8") as file:
        data = json.load(file)
    start_time = "2020-10-01"
    end_time = "2020-11-01"

    enricher = GetReportsIdDataEnricher(data)
    enriched_data = enricher.enrich_api_response(start_time, end_time)

    with open("enriched_response.json", "w", encoding="utf-8") as file:
        json.dump(enriched_data, file, indent=4, ensure_ascii=False, default=str)


if __name__ == "__main__":
    main()
