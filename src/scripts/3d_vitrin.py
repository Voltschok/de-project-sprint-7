import sys
import math
import datetime
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['SPARK_LOCAL_IP'] = '127.0.1.1'

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

def get_distance(lat_1, lat_2, long_1, long_2):
    lat_1=(math.pi/180)*lat_1
    lat_2=(math.pi/180)*lat_2
    long_1=(math.pi/180)*long_1
    long_2=(math.pi/180)*long_2
    return  2*6371*math.asin(math.sqrt(math.pow(math.sin((lat_2 - lat_1)/2), 2)+
    math.cos(lat_1)*math.cos(lat_2)*math.pow(math.sin((long_2 - long_1)/2),2)))

udf_func=F.udf(get_distance)


def input_paths(date, depth,base_input_path ):
    """ Функция для расчета путей по заданной дате и глубине  """
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{base_input_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(int(depth))]



def main():   
    #получаем параметры из командной строки
    #base_input_path=sys.argv[1]
    #date=sys.argv[2]
    #depth=sys.argv[3]
    #csv_path=sys.argv[4]
    #output_path=sys.argv[5]

    base_input_path='/user/voltschok/data/geo/events'
    date='2022-05-25'
    depth=10
    csv_path='/user/voltschok/data/geo/cities'
    output_path='/user/voltschok/data/geo/analytics/'
    spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()   


    #рассчитываем пути по заданным параметрам - дате, глубине расчета и источнику
    paths=input_paths(date, depth, base_input_path)
      
    #вычисляем датасет со всеми событиями
    events=spark.read.option("basePath", base_input_path).parquet(*paths)

    #рассчитываем необходимые датасеты с помощью функций 
    common_subs_distance1=get_common_subs_distance(events)
    no_contacts_users1=get_no_contacts(events)
    
    #рассчитываем финальный датасет
    recommendation=common_subs_distance1.join(no_contacts_users1, ['user_left', 'user_right'], 'inner')\
        .withColumn('timezone',  F.lit('Australia/Sydney'))\
        .withColumn("processed_dttm", current_date())\
        .withColumn('local_datetime',  F.from_utc_timestamp(F.col("processed_dttm"),F.col('timezone')))\
        .withColumn('local_time', date_format(col('local_datetime'), 'HH:mm:ss'))\
        .select('user_left', 'user_right', 'processed_dttm',  'local_time')
    
    recommendation.orderBy('user_left').show(3)
    
    #записываем результат по заданному пути
#     recommendation.write \
#         .mode("overwrite") \
#         .parquet(output_path)
                
def get_common_subs_distance(events):
                    
    #рассчитываем все пары пользователей, которые подписаны на один канал               
    subs_user_right=events.filter(F.col('event_type')=='subscription')\
    .select(F.col('event.user').alias('user_left'), F.col('lat').alias('lat_1'),
            F.col('lon').alias('lon_1'), 'event.channel_id')

    subs_user_left=subs_user_right.select(F.col('user_left').alias('user_right'), 
                                          F.col('lat_1').alias('lat_2'), F.col('lon_1').alias('lon_2'), 'channel_id')

    common_subs=subs_user_right.join(subs_user_left, 'channel_id' ,'inner')\
    .where(F.col('user_left')!=F.col('user_right')).distinct()
    
    #рассчитываем все пары пользователей, которые подписаны на один канал  и находятся менее, чем на 1 км друг от друга                
    common_subs_distance=common_subs\
    .withColumn('distance', udf_func( F.col('lat_1'), F.col('lat_2'), F.col('lon_1'), F.col('lon_2')).cast('float'))\
    .where((F.col('distance').isNotNull())&(F.col('distance')<1.0))\
    .select('user_left','user_right')
           
    return common_subs_distance

def get_no_contacts(events):
           
    #рассчитываем все пары пользователей, которые переписывались 
    user_left_contacts = events.where(F.col('event_type')=='message') \
        .select(col('event.message_from').alias('user_left'), col('event.message_to').alias('user_right'))
    
    user_right_contacts = events.where(F.col('event_type')=='message') \
        .select(col('event.message_to').alias('user_left'), col('event.message_from').alias('user_right'))
    
    real_contacts=user_left_contacts.union(user_right_contacts).distinct()
         
    #рассчитываем все возможные пары пользователей                    
    all_users=events.where(F.col('event_type')=='message')\
    .selectExpr('event.message_from as user_left')\
    .union(events.where(F.col('event_type')=='message').selectExpr('event.message_to as user_right')).distinct()
    
    all_possible_contacts=all_users.crossJoin(all_users.withColumnRenamed('user_left', 'user_right'))
    
 
    #рассчитываем все пары пользователей, которые не переписывались друг с другом, и удаляем дубликаты                      
    no_contacts_users_with_dupl=all_possible_contacts.subtract(real_contacts) 
    
    no_contacts_users=no_contacts_users_with_dupl\
                      .union(no_contacts_users_with_dupl)\
                      .select(F.col('user_right').alias('user_left'),F.col('user_left').alias('user_right'))\
                      .distinct()
           
    return no_contacts_users  
           

if __name__ == "__main__":

    main()
   
     
