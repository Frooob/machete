from prefect import flow, task, runtime
from prefect.artifacts import create_markdown_artifact
from prefect_aws.s3 import S3Bucket
from prefect.blocks.system import Secret
from datetime import datetime
import statsapi
import json
import pandas as pd
import duckdb
from datetime import timedelta
import requests

import country_converter as coco

from prefect.cache_policies import TASK_SOURCE, INPUTS
from prefect_aws import S3Bucket

from connector.setup_prefect import get_aws_bucket_block
from scraper.env import scraper_env


cache_policy_s3_no_delete = TASK_SOURCE + INPUTS


@task(
    result_storage_key="all_exchanges.json",
    result_serializer="json",
    cache_policy=cache_policy_s3_no_delete,
    result_storage=get_aws_bucket_block(),
)
def get_all_exchanges_json():
    print("Fetching list of all exchanges")
    url = f"https://eodhd.com/api/exchanges-list/?api_token={scraper_env.EODHD_API_KEY}&fmt=json"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.text


@task(
    result_storage_key="exchange-{parameters[code]}-symbol-list.json",
    result_serializer="json",
    cache_policy=cache_policy_s3_no_delete,
    result_storage=get_aws_bucket_block(),
)
def get_common_stocks_from_exchange(code: str):
    print(f"Fetching list of common stocks of exchange {code}.")
    url = f"https://eodhd.com/api/exchange-symbol-list/{code}?api_token={scraper_env.EODHD_API_KEY}&type=common_stock&fmt=json"
    response = requests.get(url)
    response.raise_for_status()
    return response.text


@task
def filter_relevant_exchange_codes(exchanges: list[dict]):
    relevant_codes = [
        exchange["Code"] for exchange in exchanges if exchange["CountryISO3"]
    ]
    return relevant_codes


@flow(log_prints=True)
def get_all_exchanges():
    exchanges_json = get_all_exchanges_json()
    exchanges = json.loads(exchanges_json)
    exchange_codes = filter_relevant_exchange_codes(exchanges)

    return exchanges


if __name__ == "__main__":
    exchanges = get_all_exchanges()
    print(exchanges)
    ...
