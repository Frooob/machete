import os

from dotenv import load_dotenv
from dataclasses import dataclass


@dataclass
class Connector_env:
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION_NAME: str
    MOTHERDUCK_TOKEN: str

    def __init__(self):
        load_dotenv()
        self.AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
        self.AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
        self.AWS_REGION_NAME = os.environ["AWS_DEFAULT_REGION"]
        self.MOTHERDUCK_TOKEN = os.environ["MOTHERDUCK_TOKEN"]


connector_env = Connector_env()
