from pyspark.sql import SparkSession as spark

if __name__ == "__main__":

    spark.sql("drop database if exists tdtest cascade")
    spark.sql("create database tdtest")
