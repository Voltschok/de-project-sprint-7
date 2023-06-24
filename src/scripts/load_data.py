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
        base_input_path=sys.argv[1]
        date=sys.argv[2]
        depth=int(sys.argv[3])
        output_path=sys.argv[5]

        #base_input_path = '/user/master/data/geo/events'
        #date = '2022-05-25'
        #depth=1
        #output_path = '/user/voltschok/data/geo/events/'

        conf = SparkConf().setAppName(f"EventsPartitioningJob-test")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

#чтение данных
        events = sql.read.parquet(f"{base_input_path}") #.sample(0.005)

# запись данных
        events\
        .write\
        .mode('overwrite')\
        .partitionBy(['date', 'event_type'])\
        .format('parquet')\
        .save(f"{output_path}")


if __name__ == "__main__":
        main()

