from dotenv import load_dotenv

from prefect_aws import AwsCredentials, S3Bucket
from prefect.blocks.system import Secret

from connector.env import connector_env

load_dotenv()

s3_cred_block_name = "machete-s3"
s3_buck_block_name = "machete-bucket-s3"
motherduck_token_name = "machete-motherduck-token"
bucket_name = "machetebucketfroobington"


def get_aws_credentials_block():
    return AwsCredentials.load(s3_cred_block_name)


def get_aws_bucket_block():
    return S3Bucket.load(s3_buck_block_name)


def get_motherduck_block():
    return Secret.load(motherduck_token_name)


def main():
    try:
        AwsCredentials(
            aws_access_key_id=connector_env.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=connector_env.AWS_SECRET_ACCESS_KEY,
            region_name=connector_env.AWS_REGION_NAME,
        ).save(s3_cred_block_name)
    except ValueError:
        print("Saving AWS credentials block failed (it might exist already).")

    aws_credentials = get_aws_bucket_block()
    try:
        S3Bucket(
            bucket_name=bucket_name,
            credentials=aws_credentials,
        ).save(s3_buck_block_name)
    except ValueError:
        print("Saving S3 bucket credentials block failed (it might exist already).")

    try:
        Secret(
            value=connector_env.MOTHERDUCK_TOKEN,
            name=motherduck_token_name,  # Replace this with a name that will help you identify the secret.
        ).save(motherduck_token_name)  # Replace this with a descriptive block name.
    except ValueError:
        print("Saving motherduck credentials block failed (it might exist already).")


if __name__ == "__main__":
    main()
    get_aws_bucket_block()
    get_aws_credentials_block()
    get_motherduck_block()
