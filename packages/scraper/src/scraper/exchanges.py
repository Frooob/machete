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
    # https://eodhd.com/api/exchanges-list/?api_token=YOUR_API_KEY&fmt=json
    print("Fetching list of all exchanges")
    url = f"https://eodhd.com/api/exchanges-list/?api_token={scraper_env.EODHD_API_KEY}&fmt=json"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.text


@flow(log_prints=True)
def get_all_exchanges():
    exchanges_json = get_all_exchanges_json()
    exchanges = json.loads(exchanges_json)

    return exchanges


if __name__ == "__main__":
    exchanges = get_all_exchanges()
    print(exchanges)
    ...
