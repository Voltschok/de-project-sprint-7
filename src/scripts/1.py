import sys
import datetime
import os, math
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = "$HADOOP_HOME/etc/hadoop"
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['SPARK_LOCAL_IP'] = '127.0.1.1'

 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp

#from datetime import datetime, timedelta

from typing import List
import sys


import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

 

import pyspark.sql.functions as F


'/user/master/data/geo/events'

def get_distance(lat_1, lat_2, long_1, long_2):
    lat_1=(math.pi/180)*lat_1
    lat_2=(math.pi/180)*lat_2
    long_1=(math.pi/180)*long_1
    long_2=(math.pi/180)*long_2

    return 2*6371*F.asin(
    F.sqrt(F.pow(F.sin((F.col('lat_2') - F.col('lat_1'))/F.lit(2)), F.lit(2))+
    F.cos('lat_1')*F.cos('lat_2')*F.pow(F.sin((F.col('long_2') - 
                                               F.col('long_1'))/F.lit(2)), F.lit(2))
    ))
# def get_distance2(lat_1, lat_2, long_1, long_2):
#     lat_1=(math.pi/180)*lat_1
#     lat_2=(math.pi/180)*lat_2

#     long_1=(math.pi/180)*long_1
#     long_2=(math.pi/180)*long_2

#     return 2*6371*math.asin(
#     math.sqrt(math.pow(math.sin( lat_2- lat_1)/2, 2)+
#     math.cos(lat_1)*math.cos(lat_2)*math.pow(math.sin( long_2  -  long_1 )/ 2 ,  2 )
#     ))

print(get_distance(55,59,37,30))


