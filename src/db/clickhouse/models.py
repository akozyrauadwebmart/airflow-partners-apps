from abc import ABC
import os

from dotenv import load_dotenv

load_dotenv()


class ModelFactory(ABC):
    DB_NAME = None
    TABLE_NAME = None


class LiftoffStagingReportModel(ModelFactory):
    TABLE_NAME = os.getenv("TABLE_NAME_LIFTOFF_STAGING_REPORT")


class LiftoffStagingAppModel(ModelFactory):
    TABLE_NAME = os.getenv("TABLE_NAME_LIFTOFF_STAGING_APP")


class LiftoffStagingCreativeModel(ModelFactory):
    TABLE_NAME = os.getenv("TABLE_NAME_LIFTOFF_STAGING_CREATIVE")


class LiftoffStagingCampaignModel(ModelFactory):
    TABLE_NAME = os.getenv("TABLE_NAME_LIFTOFF_STAGING_CAMPAIGN")


class LiftoffSecretAccountModel(ModelFactory):
    DB_NAME = os.getenv("DB_NAME_SECRET")
    TABLE_NAME = os.getenv("TABLE_NAME_SECRET_ACCOUNT")
    FULL_NAME = DB_NAME + "." + TABLE_NAME