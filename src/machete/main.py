import pandas as pd
from scraper.scrape import scrape


def main():
    print("Hello from machete!")


if __name__ == "__main__":
    main()
    df = pd.DataFrame({1: [2]})
    scrape()
