from dataclasses import dataclass
from scraper import scrape


@dataclass
class Connection:
    name: str
    speed: int

    def get_connection(self):
        print(f"Got connection from {self.name}")

    def scrape_data(self):
        scrape.scrape()
