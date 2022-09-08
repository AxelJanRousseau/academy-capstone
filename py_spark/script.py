from pyspark import SparkConf
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession, DataFrame
import os
import boto3
import json

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2')
conf.set('spark.jars.packages','net.snowflake:snowflake-jdbc:3.13.22')
conf.set('spark.jars.packages','net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3')
conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
def get_df() -> DataFrame:
    if __debug__:
        return spark.read.json('./data_part_1.json')
    return spark.read.json('s3a://dataminded-academy-capstone-resources/raw/open_aq/')
    

def flatten_frame(df: DataFrame):
    df = df.withColumn('latitude', sf.col("coordinates.latitude"))
    df = df.withColumn('longitude',sf.col('coordinates.longitude'))

    df = df.withColumn('date_time_in_utc', sf.to_date(sf.col('date.utc')))
    
    return df

def get_secret():
    secret_name = "snowflake/capstone/login"
    region_name = "eu-west-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )
    response = client.get_secret_value(
            SecretId=secret_name
        )
    creds = json.loads(response['SecretString'])
    return {
        "sfURL": f"{creds['URL']}",
        "sfPassword": creds["PASSWORD"],
        "sfUser": creds["USER_NAME"],
        "sfDatabase": creds["DATABASE"],
        "sfWarehouse": creds["WAREHOUSE"],
        "sfRole": creds["ROLE"]
    }

def main():
    df = get_df()
    df.printSchema()
    df = flatten_frame(df)
    for cols in ['coordinates','date']:
        df = df.drop(cols)
    secrets = get_secret()
    secrets['sfSchema']="AXELJAN_ROUSSEAU"

    df.write.format(SNOWFLAKE_SOURCE_NAME).options(**secrets).option("dbtable", "axel_table").mode('overwrite').save()
    print('debug')
    

if __name__ == "__main__":
    main()