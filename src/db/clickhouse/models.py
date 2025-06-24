from abc import ABC
import os

from dotenv import load_dotenv

load_dotenv()


class ModelFactory(ABC):
    DB_NAME = None
    TABLE_NAME = None


class LiftoffStagingReportModel(ModelFactory):
    DB_NAME = os.getenv("DB_NAME_LIFTOFF_STAGING")
    TABLE_NAME = os.getenv("TABLE_NAME_LIFTOFF_STAGING_REPORT")