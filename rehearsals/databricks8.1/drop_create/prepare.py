from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

if __name__ == "__main__":

    spark.sql("drop database if exists tdtest cascade")
    spark.sql("create database tdtest")
