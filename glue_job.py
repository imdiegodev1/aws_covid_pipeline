import boto3
import pandas as pd
import time

from io import StringIO

AWS_REGION = "us-east-1"
SCHEMA_NAME = "covi_19"
BUCKET_NAME = "covid-pipeline-learning"
OUTPUT_FOLDER = "staging_data"
STAGING_FOLDER = f"s3://{BUCKET_NAME}/{OUTPUT_FOLDER}/"


athena_client = boto3.client(
    "athena",
    region_name=AWS_REGION
    )

dict = {}

def download_load_query_results(
        client: boto3.client,
        query_response: dict) -> pd.DataFrame:
    
    while True:
        try:
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    
    temp_file_location: str = "athena_query_result.csv"
    s3_client = boto3.client(
        "s3",
        region_name = AWS_REGION
    )

    s3_client.download_file(
        BUCKET_NAME,
        f"{OUTPUT_FOLDER}/{query_response['QueryExecutionId']}.csv",
        temp_file_location
    )

    return pd.read_csv(temp_file_location)

response = athena_client.start_query_execution(
    QueryString = "SELECT * FROM enigma_jhud",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration= {
        "OutputLocation": STAGING_FOLDER,
    },
)

df_data = download_load_query_results(athena_client, response)

print(df_data)