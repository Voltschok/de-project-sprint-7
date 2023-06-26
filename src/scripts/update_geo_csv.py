import datetime
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['JAVA_HOME']='/usr'
# os.environ['SPARK_HOME'] ='/usr/lib/spark'
# os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

import pyspark.sql.functions as F
import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType


spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()   

geo_data_csv=spark.read.option("header", True)\
    .option("delimiter", ";").csv('/user/voltschok/data/geo/cities/geo.csv')
   
    #добавляем недостающие города 
missing_tz_data= [
            ('Sydney', 'Australia/Sydney'),
            ('Melbourne', 'Australia/Melbourne'),
            ('Brisbane', 'Australia/Brisbane'),
            ('Perth', 'Australia/Perth'),
            ('Adelaide', 'Australia/Adelaide'),
            ('Gold Coast', 'Australia/Brisbane'),
            ('Cranbourne', 'Australia/Melbourne'),
            ('Canberra', 'Australia/Canberra'),
            ('Newcastle', 'Australia/Sydney'),
            ('Wollongong', 'Australia/Sydney'),
            ('Geelong', 'Australia/Melbourne'),
            ('Hobart', 'Australia/Hobart'),
            ('Townsville', 'Australia/Brisbane'),
            ('Ipswich', 'Australia/Sydney'),
            ('Cairns', 'Australia/Brisbane'),
            ('Toowoomba', 'Australia/Brisbane'),
            ('Darwin', 'Australia/Darwin'),
            ('Ballarat', 'Australia/Melbourne'),
            ('Bendigo',  'Australia/Melbourne'),
            ('Launceston', 'Australia/Hobart'),
            ('Mackay',  'Australia/Brisbane'),
            ('Rockhampton',   'Australia/Brisbane'),
            ('Maitland', 'Australia/Sydney'),
            ('Bunbury', 'Australia/Perth')]

missing_columns_tz = ['city', 'timezone']
missing_tz = spark.createDataFrame(data=missing_tz_data, schema=missing_columns_tz) 
geo_data_csv\
.join(missing_tz, 'city', 'left')\
.write.format('csv')\
.mode('overwrite')\
.option("header", True)\
.save('/user/voltschok/data/geo/cities/geo.csv') 
