import os

from dotenv import load_dotenv

load_dotenv()


class LiftoffApi:
    API_KEY = os.getenv("LIFTOFF_API_KEY")
    API_SECRET = os.getenv("LIFTOFF_API_SECRET")