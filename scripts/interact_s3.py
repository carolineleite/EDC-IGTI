import boto3
import pandas as pd

# Criar um cliente para interagir com o AWS S3

s3_client = boto3.client("s3")

s3_client.download_file("datalake-carolineleite", "data/teste.csv", "teste.csv")

df = pd.read_csv("teste.csv", sep=';')
print(df)

# Subir arquivo para o S3
s3_client.upload_file("carol.csv", "datalake-carolineleite", "data/carol.csv")
