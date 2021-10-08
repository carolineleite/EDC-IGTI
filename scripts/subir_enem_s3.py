import boto3

s3_client = boto3.client('s3')

s3_client.upload_file("MICRODADOS_ENEM_2019.csv", "datalake-carolineleite", "raw-data/MICRODADOS_ENEM_2019.csv")
