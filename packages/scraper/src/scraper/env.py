import os

from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class Scraper_env:
    EODHD_API_KEY: str

    def __init__(self):
        load_dotenv()
        self.EODHD_API_KEY = os.environ["EODHD_API_KEY"]


scraper_env = Scraper_env()
