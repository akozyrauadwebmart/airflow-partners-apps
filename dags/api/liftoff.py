from datetime import datetime, timedelta
import traceback

from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin

from include.hooks import clickhouse
from src.app.extractors import secret
from src.app.extractors import liftoff as extractors
from src.app.transformers import liftoff as transformers
from src.app.cleaners import liftoff as cleaners
from src.app.enrichers import liftoff as enrichers
from src.app.loaders import liftoff as loaders
from src.app import utils
from src import config


logger = LoggingMixin().log

default_args = {
    "owner": "a.kozyrev",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["a.kozyrau@adwebmart.com"],
}

@dag(
    dag_id="api_liftoff_app_etl_all",
    schedule=None,
    start_date=datetime(2025, 1, 1, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["liftoff", "etl", "app", "api", "all"]
)
def api_liftoff_app_etl_all() -> None:

    @task
    def get_auth_data() -> list[dict]:
        clickhouse_hook = clickhouse.ClickHouseHook()
        ch_client = clickhouse_hook.get_conn()

        exreacror_secret = secret.LiftoffSecretExtractor(ch_client)
        auth_data = exreacror_secret.get_full_secret_data()
        return auth_data
    
    @task
    def extract(auth_data: dict[str]) -> dict:
        api_extractor = extractors.APIGetAppsExtractor(
            api_key=auth_data["api_key"],
            api_secret=auth_data["api_secret"]
        )
        raw_data = api_extractor.get_response()

        local_connector = utils.LocalConnector()
        raw_data_path = local_connector.create_path(auth_data["api_key"], "app", "raw")
        local_connector.save_json_data(raw_data_path, raw_data.json())
        context = {
            "auth_data": {
                "api_key": auth_data["api_key"],
                "api_secret": auth_data["api_secret"]
            },
            "paths": {
                "raw_data": raw_data_path
            }
        }
        return context
    
    @task
    def transform(context: dict) -> dict:
        local_connector = utils.LocalConnector()
        raw_data = local_connector.extract_json_data(context["paths"]["raw_data"])

        transformer = transformers.APIGetAppsTransformer(context["auth_data"]["api_key"], raw_data)
        trans_data = transformer.transform_to_one_level_of_nesting()

        trans_data_path = local_connector.create_path(context["auth_data"]["api_key"], "app", "trans")
        local_connector.save_json_data(trans_data_path, trans_data)

        context["paths"]["trans_data"] = trans_data_path
        return context
    
    @task
    def clean(context: dict) -> dict:
        local_connector = utils.LocalConnector()
        trans_data = local_connector.extract_json_data(context["paths"]["trans_data"])

        cleaner = cleaners.APIGetAppsCleaner(context["auth_data"]["api_key"], trans_data)
        cleaned_data = cleaner.replace_single_quote_in_data()

        cleaned_data_path = local_connector.create_path(context["auth_data"]["api_key"], "app", "cleaned")
        local_connector.save_json_data(cleaned_data_path, cleaned_data)

        context["paths"]["cleaned_data"] = cleaned_data_path
        return context
    
    @task
    def enrich(context: dict) -> dict:
        local_connector = utils.LocalConnector()
        cleaned_data = local_connector.extract_json_data(context["paths"]["cleaned_data"])

        enricher = enrichers.GetAppsEnricher(cleaned_data)
        enriched_data = enricher.enrich_api_response()

        enriched_data_path = local_connector.create_path(context["auth_data"]["api_key"], "app", "enriched")
        local_connector.save_json_data(enriched_data_path, enriched_data)

        context["paths"]["enriched_data"] = enriched_data_path
        return context
    
    @task
    def load(context: dict) -> dict:
        local_connector = utils.LocalConnector()
        enriched_data = local_connector.extract_json_data(context["paths"]["enriched_data"])

        ch_hook = clickhouse.ClickHouseHook()
        ch_client = ch_hook.get_conn()

        loader = loaders.GetAppsStagingLoader(
            api_key=context["auth_data"]["api_key"],
            data=enriched_data,
            ch_client=ch_client
        )
        loader.load_data_to_clickhouse(delete_data_before_insert=True)
        return context

    @task
    def remove_local_data(context: dict) -> None:
        local_connector = utils.LocalConnector()
        for label, path in context["paths"].items():
            local_connector.remove_file(path)

    auth_data = get_auth_data()
    raw_data = extract.expand(auth_data=auth_data)
    trans_data = transform.expand(context=raw_data)
    cleaned_data = clean.expand(context=trans_data)
    enriched_data = enrich.expand(context=cleaned_data)
    loaded_data = load.expand(context=enriched_data)
    remove_local_data.expand(context=loaded_data)
api_liftoff_app_etl_all()


@dag(
    dag_id="api_liftoff_campaign_etl_all",
    schedule=None,
    start_date=datetime(2025, 1, 1, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["api", "liftoff", "campaign", "etl", "all"]
)
def api_liftoff_campaign_etl_all() -> None:

    @task
    def get_auth_data() -> list[dict]:
        clickhouse_hook = clickhouse.ClickHouseHook()
        ch_client = clickhouse_hook.get_conn()

        exreacror_secret = secret.LiftoffSecretExtractor(ch_client)
        auth_data = exreacror_secret.get_full_secret_data()
        return auth_data
    
    @task
    def extract(auth_data: dict[str]) -> dict:
        api_extractor = extractors.APIGetCampaignsExtractor(
            api_key=auth_data["api_key"],
            api_secret=auth_data["api_secret"]
        )
        raw_data = api_extractor.get_response()

        local_connector = utils.LocalConnector()
        raw_data_path = local_connector.create_path(auth_data["api_key"], "campaign", "raw")
        local_connector.save_json_data(raw_data_path, raw_data.json())
        context = {
            "auth_data": {
                "api_key": auth_data["api_key"],
                "api_secret": auth_data["api_secret"]
            },
            "paths": {
                "raw_data": raw_data_path
            }
        }
        return context
    
    @task
    def clean(context: dict) -> dict:
        local_connector = utils.LocalConnector()
        trans_data = local_connector.extract_json_data(context["paths"]["raw_data"])

        cleaner = cleaners.APIGetCampaignsCleaner(context["auth_data"]["api_key"], trans_data)
        cleaned_data = cleaner.replace_single_quote_in_data()

        cleaned_data_path = local_connector.create_path(context["auth_data"]["api_key"], "campaign", "cleaned")
        local_connector.save_json_data(cleaned_data_path, cleaned_data)

        context["paths"]["cleaned_data"] = cleaned_data_path
        return context
    
    @task
    def enrich(context: dict) -> dict:
        local_connector = utils.LocalConnector()
        cleaned_data = local_connector.extract_json_data(context["paths"]["cleaned_data"])

        enricher = enrichers.GetCampaignsEnricher(cleaned_data)
        enriched_data = enricher.enrich_api_response()

        enriched_data_path = local_connector.create_path(context["auth_data"]["api_key"], "campaign", "enriched")
        local_connector.save_json_data(enriched_data_path, enriched_data)

        context["paths"]["enriched_data"] = enriched_data_path
        return context
    
    @task
    def load(context: dict) -> dict:
        local_connector = utils.LocalConnector()
        enriched_data = local_connector.extract_json_data(context["paths"]["enriched_data"])

        ch_hook = clickhouse.ClickHouseHook()
        ch_client = ch_hook.get_conn()

        loader = loaders.GetCampaignsStagingLoader(
            api_key=context["auth_data"]["api_key"],
            data=enriched_data,
            ch_client=ch_client
        )
        loader.load_data_to_clickhouse(delete_data_before_insert=True)
        return context

    @task
    def remove_local_data(context: dict) -> None:
        local_connector = utils.LocalConnector()
        for label, path in context["paths"].items():
            local_connector.remove_file(path)

    auth_data = get_auth_data()
    raw_data = extract.expand(auth_data=auth_data)
    cleaned_data = clean.expand(context=raw_data)
    enriched_data = enrich.expand(context=cleaned_data)
    loaded_data = load.expand(context=enriched_data)
    remove_local_data.expand(context=loaded_data)
api_liftoff_campaign_etl_all()


@dag(
    dag_id="api_liftoff_creative_etl_all",
    schedule=None,
    start_date=datetime(2025, 1, 1, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["api", "liftoff", "creative", "etl", "all"]
)
def api_liftoff_creative_etl_all() -> None:

    @task
    def get_auth_data() -> list[dict]:
        clickhouse_hook = clickhouse.ClickHouseHook()
        ch_client = clickhouse_hook.get_conn()

        exreacror_secret = secret.LiftoffSecretExtractor(ch_client)
        auth_data = exreacror_secret.get_full_secret_data()
        return auth_data
    
    @task
    def extract(auth_data: dict[str]) -> dict:
        api_extractor = extractors.APIGetCreativesExtractor(
            api_key=auth_data["api_key"],
            api_secret=auth_data["api_secret"]
        )
        raw_data = api_extractor.get_response()

        local_connector = utils.LocalConnector()
        raw_data_path = local_connector.create_path(auth_data["api_key"], "creative", "raw")
        local_connector.save_json_data(raw_data_path, raw_data.json())
        context = {
            "auth_data": {
                "api_key": auth_data["api_key"],
                "api_secret": auth_data["api_secret"]
            },
            "paths": {
                "raw_data": raw_data_path
            }
        }
        return context    
    
    @task
    def clean(context: dict) -> dict:
        local_connector = utils.LocalConnector()
        raw_data = local_connector.extract_json_data(context["paths"]["raw_data"])

        cleaner = cleaners.APIGetCreativesCleaner(context["auth_data"]["api_key"], raw_data)
        cleaner.clean()

        cleaned_data_path = local_connector.create_path(context["auth_data"]["api_key"], "creative", "cleaned")
        local_connector.save_json_data(cleaned_data_path, cleaner.data)

        context["paths"]["cleaned_data"] = cleaned_data_path
        return context
    
    @task
    def enrich(context: dict) -> dict:
        local_connector = utils.LocalConnector()
        cleaned_data = local_connector.extract_json_data(context["paths"]["cleaned_data"])

        enricher = enrichers.GetCreativesEnricher(cleaned_data)
        enriched_data = enricher.enrich_api_response()

        enriched_data_path = local_connector.create_path(context["auth_data"]["api_key"], "creative", "enriched")
        local_connector.save_json_data(enriched_data_path, enriched_data)

        context["paths"]["enriched_data"] = enriched_data_path
        return context
    
    @task
    def load(context: dict) -> dict:
        local_connector = utils.LocalConnector()
        enriched_data = local_connector.extract_json_data(context["paths"]["enriched_data"])

        ch_hook = clickhouse.ClickHouseHook()
        ch_client = ch_hook.get_conn()

        loader = loaders.GetCreativesStagingLoader(
            api_key=context["auth_data"]["api_key"],
            data=enriched_data,
            ch_client=ch_client
        )
        loader.load_data_to_clickhouse(delete_data_before_insert=True)
        return context

    @task
    def remove_local_data(context: dict) -> None:
        local_connector = utils.LocalConnector()
        for label, path in context["paths"].items():
            local_connector.remove_file(path)

    auth_data = get_auth_data()
    raw_data = extract.expand(auth_data=auth_data)
    cleaned_data = clean.expand(context=raw_data)
    enriched_data = enrich.expand(context=cleaned_data)
    loaded_data = load.expand(context=enriched_data)
    remove_local_data.expand(context=loaded_data)
api_liftoff_creative_etl_all()


@dag(
    dag_id="api_liftoff_report_etl_all_v10",
    schedule=None,
    start_date=datetime(2025, 1, 1, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["api", "liftoff", "report", "etl", "all"]
)
def api_liftoff_report_etl_all() -> None:

    @task
    def get_auth_data() -> list[dict]:
        clickhouse_hook = clickhouse.ClickHouseHook()
        ch_client = clickhouse_hook.get_conn()

        exreacror_secret = secret.LiftoffSecretExtractor(ch_client)
        auth_data = exreacror_secret.get_full_secret_data()
        return auth_data
    
    # TODO: create request params
    
    @task
    def create_report(auth_data: dict[str]) -> dict:

        # temp
        start_time = (datetime.now() - timedelta(7)).strftime(config.LiftoffApi.PostReports.DATE_REQUEST_FORMAT)
        end_time = datetime.now().strftime(config.LiftoffApi.PostReports.DATE_REQUEST_FORMAT)
        # temp

        api_extractor = extractors.APIPostReportsExtractor(
            api_key=auth_data["api_key"],
            api_secret=auth_data["api_secret"]
        )
        api_extractor.get_response(start_time, end_time)
        report_id = api_extractor.get_report_id_from_response()

        # TODO: save report | new LocalConnector
        context = {
            "auth_data": {
                "api_key": auth_data["api_key"],
                "api_secret": auth_data["api_secret"]
            },
            "report": {
                "start_time": start_time,
                "end_time": end_time,
                "id": report_id
            }
        }
        return context 
    
    @task.sensor(poke_interval=5, timeout=3600, mode="reschedule")
    def wait_sensor(context: dict) -> bool:
        api_extractor = extractors.APIGetReportsIdStatusExtractor(
            api_key=context["auth_data"]["api_key"],
            api_secret=context["auth_data"]["api_secret"]
        )
        api_extractor.get_response(context["report"]["id"])
        status = api_extractor.get_status_from_response()
        return status == "completed"
    
    @task
    def download_report(context: dict) -> dict:
        api_extractor = extractors.APIGetReportsIdDataExtractor(
            api_key=context["auth_data"]["api_key"],
            api_secret=context["auth_data"]["api_secret"]
        )
        api_extractor.get_response(context["report"]["id"])

        local_connector = utils.LocalConnector()
        raw_data_path = local_connector.create_path(context["auth_data"]["api_key"], "report", "raw")

        local_connector.save_json_data(raw_data_path, api_extractor.response.json())
        context["paths"] = {
            "raw_data": raw_data_path
        }
        return context
    auth_data = get_auth_data()
    created_reports = create_report.expand(auth_data=auth_data)
    waited_reports = wait_sensor.expand(context=created_reports)
    dawnloaded_reports = download_report.expand(context=created_reports)
    auth_data >> created_reports >> waited_reports >> dawnloaded_reports
api_liftoff_report_etl_all()