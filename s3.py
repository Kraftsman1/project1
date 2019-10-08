import boto3
import pandas as pd

def main():

    region = "eu-west-1"

    s3_client = boto3.client('s3', region_name=region)

    location = {'LocationConstraint': region}

    bucket = "blossom-data-engs"
    s3_client.download_file(bucket, 'free-7-million-company-dataset.zip')

    data = pd.read_csv('free-7-million-company-dataset.zip', compression='zip')

    data = data[data['domain'].isna()]
    data.to_parquet('sorted_data.parquet')
    data.to_json('sorted_data.json.gzip', compression="gzip")

    s3_client.upload_file("sorted_data.parquet", "blossom-data-eng-asigrishamsu", "sorted_data.parquet")
    s3_client.upload_file("sorted_data.json.gzip", "blossom-data-eng-asigrishamsu", "sorted_data.json.gzip")

if __name__ == "__main__":
    main()
