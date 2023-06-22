import sys

 
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = "$HADOOP_HOME/etc/hadoop"
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['SPARK_LOCAL_IP'] = '127.0.1.1'
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

def main():
        #date = '2022-05-25'
        base_input_path = '/user/master/data/geo/events'
        base_output_path = '/user/voltschok/data/geo/events/'

        conf = SparkConf().setAppName(f"EventsPartitioningJob-test")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

 # Напишите директорию чтения в общем виде
        events = sql.read.parquet(f"{base_input_path}").sample(0.005)

# Напишите директорию записи
        events\
        .write\
        .mode('overwrite')\
        .partitionBy(['date', 'event_type'])\
        .format('parquet')\
        .save(f"{base_output_path}")


if __name__ == "__main__":
        main()
#!hdfs dfs -copyfromlocal /home/voltschok/Downloads/geo.csv /user/voltschok/data/geo/cities
#!hdfs dfs -copyfromlocal /home/voltschok/Downloads/geo.csv /user/voltschok/data/geo/cities