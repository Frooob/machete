from connector.setup_prefect import (
    get_motherduck_block,
    get_aws_bucket_block,
    get_aws_credentials_block,
)


def scrape():
    motherduck_block = get_motherduck_block()
    s3_block = get_aws_bucket_block()

    print("Scraped data")
    print(motherduck_block)
    print(s3_block)


if __name__ == "__main__":
    scrape()
