import os
import pathlib
from pathlib import Path

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
import botocore.exceptions

import country_converter as coco
from prefect.cache_policies import TASK_SOURCE, INPUTS
from prefect_aws import S3Bucket

from connector.setup_prefect import get_aws_bucket_block
from scraper.env import scraper_env
from scraper.metadata import data_root
import awswrangler as wr

cache_policy_s3_no_delete = TASK_SOURCE + INPUTS

raw_stocks_dataframe_path = os.path.join(data_root, "df_stocks.csv")


@task
def save_df_as_csv_to_local_folder(df: pd.DataFrame, path: str):
    print(f"Saving df to path {path}")
    df.to_csv(path)


@task
def upload_raw_data_to_s3(file_path: str, s3_key: str = None) -> str:
    """
    Upload the local file to S3 only if it doesn't already exist.
    Returns the S3 object path.
    """
    bucket = get_aws_bucket_block()
    aws_creds = bucket.credentials.get_boto3_session()

    s3_client = aws_creds.client("s3")
    bucket_name = bucket.bucket_name
    s3_key = s3_key or Path(file_path).name
    full_s3_path = f"{bucket.basepath}/{s3_key}".strip("/")

    # Check if the object already exists
    try:
        s3_client.head_object(Bucket=bucket_name, Key=full_s3_path)
        print(f"Skipping upload: S3 object already exists at {full_s3_path}")
        return f"s3://{bucket_name}/{full_s3_path}"
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise  # Unexpected error
        # File does not exist → proceed to upload

    # Upload file
    s3_uri = bucket.upload_from_path(from_path=file_path, to_path=full_s3_path)
    print(f"Uploaded {file_path} to S3: {s3_uri}")
    return s3_uri


@task
def load_dataframe_from_csv(path: str):
    if not os.path.exists(path):
        s3_fname = pathlib.Path(path).name
        bucket = get_aws_bucket_block()
        bucket.download_object_to_path(s3_fname, path)
        print(f"Downloaded dataframe from s3 path {s3_fname} to local path {path}.")

    df = pd.read_csv(path)
    print(f"Loaded dataframe from local path: {path}")
    return df


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


@task
def create_dataframe_from_common_stocks(common_stocks_all_exchanges: dict[list]):
    # format of the input is: {Exchange_code: list[stock_info_dict]}
    big_list = []

    for exchange_code, stock_list in common_stocks_all_exchanges.items():
        for stock in stock_list:
            stock_copy = stock.copy()
            stock_copy["ex_code"] = exchange_code
            big_list.append(stock_copy)
    df_stocks = pd.DataFrame(big_list)
    return df_stocks


@flow(log_prints=True)
def get_exchanges_dataframe():
    exchanges_json = get_all_exchanges_json()
    exchanges = json.loads(exchanges_json)
    exchange_codes = filter_relevant_exchange_codes(exchanges)
    exchange_codes = [
        "US",
        "F",
    ]  # In order not to get poor during the process lol. Financial data is expensive as shit.

    common_stocks_all_exchanges = {}
    for exchange_code in (
        exchange_codes
    ):  # Could be done in parallel. But I'm limited heavily anyways. So why bother.
        common_stocks = json.loads(get_common_stocks_from_exchange(exchange_code))
        common_stocks_all_exchanges[exchange_code] = common_stocks

    df_stocks = create_dataframe_from_common_stocks(common_stocks_all_exchanges)

    local_path_df_stocks = raw_stocks_dataframe_path
    save_df_as_csv_to_local_folder(df_stocks, local_path_df_stocks)
    s3_path_df_stocks = upload_raw_data_to_s3(local_path_df_stocks)

    return s3_path_df_stocks


@flow(log_prints=True)
def filtered_exchanges_to_warehouse():
    df_stocks = load_dataframe_from_csv(raw_stocks_dataframe_path)
    return df_stocks


if __name__ == "__main__":
    # s3_path_df_stocks = get_exchanges_dataframe()
    # print(s3_path_df_stocks)

    print(filtered_exchanges_to_warehouse())
    ...
