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

print(type(athena_client))

def main():
    
    dict = {}

    query_enigma_jhud = "SELECT * FROM enigma_jhud"

    enigma_jhud = download_load_query_results(athena_client,
                                          create_response(athena_client,
                                                          query_enigma_jhud,
                                                          SCHEMA_NAME,
                                                          STAGING_FOLDER))
    
    print(enigma_jhud)



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

def create_response(athena_client: boto3.client,
                    query: str,
                    schema_name: str,
                    staging_folder: str):
    
    response = athena_client.start_query_execution(
        QueryString = query,
        QueryExecutionContext={"Database": schema_name},
        ResultConfiguration= {
            "OutputLocation": staging_folder,
        },
    )

    print(type(response))

    return response



#df_data = download_load_query_results(athena_client, response)

#print(df_data)

if __name__ == '__main__':

    main()