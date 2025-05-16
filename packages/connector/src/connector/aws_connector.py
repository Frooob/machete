import json

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv

from connector.setup_prefect import bucket_name

load_dotenv("./.env")

# Initialize a session using your IAM user's credentials (these are the default ones if you configured AWS CLI)
s3_client = boto3.client("s3")


# Function to generate and upload the JSON file to S3
def upload_json_to_s3():
    data = {"message": "Hello, world!", "status": "Success"}

    # Create a JSON string from the data
    json_data = json.dumps(data)

    # Define the file name
    file_name = "hello_world.json"

    try:
        # Upload the JSON data to S3
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=json_data)
        print(f"File '{file_name}' uploaded successfully to {bucket_name}")
    except (NoCredentialsError, PartialCredentialsError):
        print("Error: Credentials are missing or incomplete.")
    except Exception as e:
        print(f"Error uploading file: {e}")


# Function to download the JSON file from S3 and display it
def download_and_display_json():
    file_name = "hello_world.json"

    try:
        # Download the file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)

        # Read and load the JSON data
        data = json.loads(response["Body"].read().decode("utf-8"))

        # Print the JSON data in a nice format
        print(f"File Content: \n{json.dumps(data, indent=4)}")
    except s3_client.exceptions.NoSuchKey:
        print(f"Error: File '{file_name}' not found in bucket {bucket_name}.")
    except Exception as e:
        print(f"Error downloading or parsing file: {e}")


# Main function to execute both operations
def main():
    upload_json_to_s3()  # Upload the file
    download_and_display_json()  # Query and display the file content


if __name__ == "__main__":
    main()
