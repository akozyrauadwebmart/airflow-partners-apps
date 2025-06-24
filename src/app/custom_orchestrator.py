from time import sleep
import json

from src.app.extractors import liftoff as ex_lift
from src.app.cleaners import liftoff as cl_lift
from src.app.enrichers import liftoff as en_lift
from src.app.loaders import liftoff as ld_lift


class ELTReport:

    def test(self) -> None:
        start_time = "2025-06-01"
        end_time = "2025-06-23"

        extractor_gen = ex_lift.PostReportsExtractor()
        response_gen = extractor_gen.get_response(start_time, end_time)

        cleaner_gen = cl_lift.PostReportsCleaner(response_gen.json())
        report_id = cleaner_gen.get_id_from_response()
        report_id = "9dfc9c4663d33c42da4c"
        print("Report ID: ", report_id)

        self.check_report_status(report_id)

        extractor_get = ex_lift.GetReportsIdDataExtractor()
        response_get = extractor_get.get_response(report_id)

        with open("src/app/data/raw_data.json", "w", encoding="utf-8") as file:
            json.dump(response_get, file, indent=4, ensure_ascii=False, default=str)

        enricher_get = en_lift.GetReportsIdDataEnricher(response_get.json())
        staging_data = enricher_get.enrich_api_response(start_time, end_time)

        with open("src/app/data/staging_data.json", "w", encoding="utf-8") as file:
            json.dump(staging_data, file, indent=4, ensure_ascii=False, default=str)

        loader_get = ld_lift.GetReportsIdDataStagingLoader(staging_data)
        loader_get.load_data_to_clickhouse()


    def check_report_status(self, id: str) -> bool:
        extractor_check = ex_lift.GetReportsIdStatusExtractor()
        response_check = extractor_check.get_response(id)

        cleaner_check = cl_lift.GetReportsIdStatusCleaner(response_check.json())
        is_completed = cleaner_check.check_state()

        if not is_completed:
            print("The report isn't ready")
            sleep(5)
            return self.check_report_status(id)
        else:
            print("The report is ready")
            return True
        

class ELTApp:

    def elt(self) -> None:
        extractor = ex_lift.GetAppsExtractor()
        response_raw = extractor.get_response()

        enricher = en_lift.GetAppsEnricher(response_raw.json())
        response_enriched = enricher.enrich_api_response()

        loader = ld_lift.GetAppsStagingLoader(response_enriched)
        loader.load_data_to_clickhouse()
    

def main() -> None:
    elt = ELTApp()
    elt.elt()


if __name__ == "__main__":
    main()