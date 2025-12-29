import os
from resources.dev import config
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import *

# Create S3 client
s3_client_provider = S3ClientProvider(
    decrypt(config.aws_access_key),
    decrypt(config.aws_secret_key)
)
s3_client = s3_client_provider.get_client()

# Correct file path
local_file_path = "/Users/vadhinijhaver/Documents/data_engineering/spark_data/sales_data.csv"

def upload_to_s3(s3_directory, s3_bucket, local_file_path):
    try:
        # If uploading a single file
        if os.path.isfile(local_file_path):
            file_name = os.path.basename(local_file_path)
            s3_key = f"{s3_directory}/{file_name}"
            print(f"Uploading {local_file_path} → s3://{s3_bucket}/{s3_key}")
            s3_client.upload_file(local_file_path, s3_bucket, s3_key)
            return

        # If uploading a directory
        for root, dirs, files in os.walk(local_file_path):
            for file in files:
                full_path = os.path.join(root, file)
                s3_key = f"{s3_directory}/{file}"
                print(f"Uploading {full_path} → s3://{s3_bucket}/{s3_key}")
                s3_client.upload_file(full_path, s3_bucket, s3_key)

    except Exception as e:
        print("Error uploading to S3:", e)
        raise e


# S3 folder & bucket
s3_directory = "sales_data"
s3_bucket = "dataprojectfirst"

upload_to_s3(s3_directory, s3_bucket, local_file_path)
