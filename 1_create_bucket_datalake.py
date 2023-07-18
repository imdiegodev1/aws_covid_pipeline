import boto3
import botocore

BUCKET_NAME = "test-bucket-diego9475"
DIRECTORIES = ["enigma-jhud", "enigma-nytimes-data-in-usa/us_county",
               "enigma-nytimes-data-in-usa/us_state",
               "rearc-covid-19-testing-data/states_daily",
               "rearc-covid-19-testing-data/us_daily",
               "rearc-covid-19-testing-data/us-total-latest",
               "rearc-usa-hospital-beds", "staging_data",
               "static-datasets/countrycode", "static-datasets/countrypopulation",
               "tatic-datasets/state-abv"]

def main():

    s3_connection = connection()
    print('Connection Done')
#    create_bucket(s3_connection, BUCKET_NAME)
#    print('Bucket creted succesfully')
#    create_folders(s3_connection, BUCKET_NAME, DIRECTORIES)
#    print('Directories created succesfully')

#Connection
def connection() -> botocore.client:

    try:
        s3 = boto3.client('s34')

        return s3
    
    except Exception as e:
        print(f"error message: {e}")
        

#Create bucket
def create_bucket(s3_connection:botocore.client
                  , BUCKET_NAME: str):
    
    try:
        return s3_connection.create_bucket(Bucket= BUCKET_NAME)
    
    except Exception as e:
        print(f"error message: {e}")

#Create Folders
def create_folders(s3_connection: botocore.client,
                   BUCKET_NAME: str,
                   DIRECTORIES: list):
    
    for i in DIRECTORIES:
        try:
            s3_connection.put_object(Bucket = BUCKET_NAME,
                                     Key = (i + '/'))
        except Exception as e:
            print(f"error message: {e}")

if __name__ == '__main__':

    main()