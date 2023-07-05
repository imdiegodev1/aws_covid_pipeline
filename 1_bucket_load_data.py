import boto3

BUCKET_NAME = "test-bucket-diego9475"
DIRECTORIES = ["enigma-jhud", "enigma-nytimes-data-in-usa/us_county",
               "enigma-nytimes-data-in-usa/us_state/"]

def main():

    s3_connection = connection()
    #create_bucket(s3_connection, BUCKET_NAME)
    create_folders(s3_connection, BUCKET_NAME, DIRECTORIES)

#Connection
def connection():

    s3 = boto3.client('s3')

    return s3

#Create bucket
def create_bucket(s3_connection, BUCKET_NAME: str):
    
    s3_connection.create_bucket(Bucket= BUCKET_NAME)

#Create Folders
def create_folders(s3_connection, BUCKET_NAME: str, DIRECTORIES: list):
    
    for i in DIRECTORIES:
        s3_connection.put_object(Bucket = BUCKET_NAME,
                                 Key = (i + '/'))

if __name__ == '__main__':

    main()