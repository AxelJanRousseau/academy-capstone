from pyspark import SparkConf
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession, DataFrame
import os

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2')
conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
def get_df_remote() -> DataFrame:
    return spark.read.json('s3a://dataminded-academy-capstone-resources/raw/open_aq/data_part_1.json')
def get_df_local() -> DataFrame:
    return spark.read.json('./data_part_1.json')

def flatten_frame(df: DataFrame):
    df = df.withColumn('latitude', sf.col("coordinates.latitude"))
    df = df.withColumn('longitude',sf.col('coordinates.longitude'))

    df = df.withColumn('date_time_in_utc', sf.to_date(sf.col('date.utc')))
    
    return df

def main():
    df = get_df_local()
    df.printSchema()
    df = flatten_frame(df)
    for cols in ['coordinates','date']:
        df = df.drop(cols)
    print('debug')

if __name__ == "__main__":
    main()