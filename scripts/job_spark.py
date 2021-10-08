from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName('ExerciseSpark').getOrCreate()
)

# Faz a leitura dos dados do enem no S3
enem = spark.read\
        .format('csv')\
        .option('header', True)\
        .option('inferSchema', True)\
        .option('delimiter', ';')\
        .load("s3://datalake-carolineleite/raw-data/")

# Salva os dados em Parquet no S3
enem\
    .write\
    .mode('overwrite')\
    .format('parquet')\
    .partitionBy('nu_ano')\
    .save("s3://datalake-carolineleite/staging/enem")
