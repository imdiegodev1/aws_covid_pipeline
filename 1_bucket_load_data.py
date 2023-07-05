import boto3

BUCKET_NAME = ""

def main():

    s3_connection = connection()
    create_bucket(BUCKET_NAME, s3_connection)

#Connection
def connection():

    s3 = boto3.resource('s3')

    return s3

#Create bucket
def create_bucket(BUCKET_NAME: str, s3_connection):
    
    s3_connection.create_bucket(Bucket= BUCKET_NAME)

#Create Folders
def create_folders():
    

if __name__ == '__main__':

    main()