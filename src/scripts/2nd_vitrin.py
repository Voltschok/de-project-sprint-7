import sys
import math
import datetime
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import pyspark.sql.functions as F
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import DoubleType

import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.window import Window

cols=['month', 'week','zone_id', 'week_message', 'week_reaction', 'week_subscription', 'week_user',
    'month_message',  'month_reaction', 'month_subscription', 'month_user']

def get_distance(lat_1, lat_2, long_1, long_2):
    """ Функция для расчета расстояния по заданным координатам  """
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

def get_geo_cities(csv_path, spark):
    """Функция рассчитывает датасет с геоданными городов из csv-файла""" 
    geo_data_csv=spark.read.option("header", True)\
    .option("delimiter", ";").csv(csv_path)
 
    geo_data = geo_data_csv.withColumn('lat', regexp_replace('lat', ',', '.').cast(DoubleType()))\
    .withColumn('lon', regexp_replace('lng', ',', '.').cast(DoubleType()))\
    .withColumnRenamed("lat", "lat_c") \
    .withColumnRenamed("lon", "lon_c")\
    .select('id', 'city', 'lat_c',  'lon_c')
        
    return geo_data

def get_message_city(events, csv_path, spark):
    
    """ Функция рассчитывает ближайший город и возвращает в виде датасета """
    events_messages=events.where(F.col('event_type')=='message')\
    .withColumn('date', F.date_trunc("day", 
                        F.coalesce(F.col('event.datetime'), F.col('event.message_ts')) ))\
    .selectExpr('event.message_id', 'event.message_from as user_id', 'date' ,'event.datetime',
                'lat', 'lon', 'event.message_ts'  )
    
    cities=get_geo_cities(csv_path, spark) 

    #рассчитываем датасет с информацией по городам, из которых направлены все сообщения 
    #(используем udf функцию для расчета расстояния)
    messages_cities=events_messages\
    .crossJoin(cities)\
    .withColumn('distance',udf_func( F.col('lat'), F.col('lat_c'), F.col('lon'), F.col('lon_c')).cast('float'))\
    .withColumn("distance_rank", F.row_number().over(Window().partitionBy(['user_id', 'message_id'])\
                                                            .orderBy(F.asc("distance")))) \
    .where("distance_rank == 1")\
    .drop('distance_rank' , 'distance' , 'lat', 'lon', 'id', 'lat_c', 'lon_c')\
    .select('message_id',  F.col('city').alias('zone_id'))
   
    return messages_cities

def last_message_city(events, csv_path, spark): 
    
    """ Функция рассчитывает ближайший город и возвращает в виде датасета """
    messages=events.where(F.col('event_type')=='message')\
    .withColumn('date', F.date_trunc("day", 
                        F.coalesce(F.col('event.datetime'), F.col('event.message_ts')) ))\
    .selectExpr('event.message_id', 'event.message_from as user_id', 'date' ,'event.datetime','lat', 'lon', 'event.message_ts'  )
  
    cities=get_geo_cities(csv_path, spark)

    messages_cities=messages\
    .crossJoin(cities)\
    .withColumn('distance',udf_func( F.col('lat'), F.col('lat_c'), F.col('lon'), F.col('lon_c')).cast('float'))\
    .withColumn("distance_rank", F.row_number().over(Window().partitionBy(['user_id']).orderBy(F.asc("distance"))))\
    .where("distance_rank == 1")\
    .drop('distance_rank' , 'distance' )\
    .select('user_id', F.col('city').alias('act_city'), "datetime", 'message_ts' , 'message_id' )

    last_message_city=messages\
    .withColumn("datetime_rank", F.row_number().over(Window().partitionBy(['user_id']).orderBy(F.desc("datetime"))))\
    .where("datetime_rank == 1").orderBy('user_id')\
    .join(messages_cities, 'user_id', 'left')\
    .select('user_id',  'act_city')

    return last_message_city

def main():
    #получаем параметры из командной строки
    base_input_path=sys.argv[1]
    date=sys.argv[2]
    depth=int(sys.argv[3])
    csv_path=sys.argv[4]
    output_path=sys.argv[5]
    
    #base_input_path='/user/voltschok/data/geo/events'
    #date='2022-05-15'
    #depth=1
    #csv_path='/user/voltschok/data/geo/cities/geo.csv'
    #output_path='/user/voltschok/data/geo/analytics/'
    spark = SparkSession.builder\
                        .master('local')\
                        .config('spark.executor.memory', '1G')\
                        .config('spark.driver.memory', '1G')\
                        .config('spark.executor.cores', 2)\
                        .config('spark.executor.instances', 4)\
                        .appName('City_week_month_stat')\
                        .getOrCreate()
    
    #получаем пути по заданному времени и глубине
    paths=input_paths(date, depth, base_input_path)
    paths2=[]

    #считываем все события по заданным путям
    try:
        events=spark.read.option("basePath", base_input_path).parquet(*paths)

        #получаем датасет с zone_id для каждого сообщения
        #user_zones_t=spark.read.parquet('/user/voltschok/data/analytics')
        message_zone=get_message_city(events, csv_path, spark)
        last_message_zone=last_message_city(events, csv_path, spark).select('user_id', F.col('act_city').alias('zone_id'))

        #получаем датасет со всеми сообщениями и делаем join с информацией по zone_id   
        messages=events.where(F.col('event_type')=='message')\
        .select('event.message_id', F.col('event.message_from').alias('user_id'), 'event.datetime', 
                F.date_trunc("day", F.coalesce(F.col('event.datetime'), F.col('event.message_ts'))).alias('date'))\
        .withColumn('event_type', F.lit('message'))\
        .join(message_zone, 'message_id', how='inner')\
        .drop('message_id')\
        .withColumn('month' , month(F.col('date')))\
        .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col('date')), 'yyyy-MM-dd')))

        #рассчитываем датасет с подписками пользователей и присваиваем им zone_id из последнего сообщения пользователя
        subscriptions=events.where(F.col('event_type')=='subscription')\
        .select(F.col('event.user').alias('user_id'), 'event.datetime',
                F.date_trunc("day", F.coalesce(F.col('event.datetime'), F.col('event.message_ts'))).alias('date'))\
        .withColumn('event_type', F.lit('subscription'))\
        .join(last_message_zone, 'user_id', 'inner')\
        .withColumn('month' , month(F.col('date')))\
        .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col('datetime')), 'yyyy-MM-dd')))

        #рассчитываем датасет с реакциями пользователей и присваиваем им zone_id из последнего сообщения пользователя
        reactions=events.where(F.col('event_type')=='reaction')\
        .select(F.col('event.reaction_from').alias('user_id'), 'event.datetime', 
                F.date_trunc("day", F.coalesce(F.col('event.datetime'), F.col('event.message_ts'))).alias('date'))\
        .withColumn('event_type', F.lit('reaction'))\
        .join(last_message_zone, 'user_id', 'inner')\
        .withColumn('month' , month(F.col('date')))\
        .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col('date')), 'yyyy-MM-dd')))

        #рассчитываем датасет с регистрациями пользователей и присваиваем им zone_id из последнего сообщения пользователя
        registrations=messages\
        .withColumn("reg_date_rank", F.row_number().over(Window().partitionBy(['user_id']).orderBy(F.asc("date"))))\
        .where(F.col('reg_date_rank')==1).drop('reg_date_rank')\
        .select('user_id','datetime', 'date')\
        .withColumn('event_type', F.lit('registration'))\
        .join(last_message_zone, 'user_id', 'inner')\
        .withColumn('month' , month(F.col('date')))\
        .withColumn("week", F.weekofyear(F.to_date(F.to_timestamp(F.col('date')), 'yyyy-MM-dd')))

        #объединяем все события
        result=messages\
        .union(subscriptions)\
        .union(reactions)\
        .union(registrations)

        #рассчитываем статистику по zone_id по месяцам
        result_month=result\
        .groupBy('month', 'zone_id')\
        .pivot('event_type').agg(F.count("*"))\
        .withColumnRenamed('message','month_message')\
        .withColumnRenamed('reaction','month_reaction')\
        .withColumnRenamed('subscription','month_subscription')\
        .withColumnRenamed('registration','month_user')

        #рассчитываем статистику по zone_id по неделям
        result_week=result\
        .groupBy('month', 'week', 'zone_id')\
        .pivot('event_type').agg(F.count("*"))\
        .withColumnRenamed('message','week_message')\
        .withColumnRenamed('reaction','week_reaction')\
        .withColumnRenamed('subscription','week_subscription')\
        .withColumnRenamed('registration','week_user')

        #объединяем датасеты
        result_final=result_week.join(result_month, ['month', 'zone_id'], 'left')
        result_final.show()

        for col in cols:
            if col not in result_final.columns:
                result_final=result_final.withColumn(col,lit('null'))

        #записываем результат
        result_final.select(cols).write.mode("overwrite").parquet(f'{output_path}/city_zone/city_zone-{date}-{depth}')
    except:
        print('All paths were ignored')
if __name__ == "__main__":
        main()
