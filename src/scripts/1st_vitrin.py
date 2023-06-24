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

#получаем пути с заданной даты на заданную глубину

def input_paths(date, depth,base_input_path ):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{base_input_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(int(depth))]

def get_geo_cities(csv_path, spark):
    #получаем датасет с геоданными городов из csv-файла
    geo_data_csv=spark.read.option("header", True)\
    .option("delimiter", ";").csv(csv_path)
 
    geo_data = geo_data_csv.withColumn('lat', regexp_replace('lat', ',', '.').cast(DoubleType()))\
    .withColumn('lon', regexp_replace('lng', ',', '.').cast(DoubleType()))\
    .withColumnRenamed("lat", "lat_c") \
    .withColumnRenamed("lon", "lon_c")\
    .select('id', 'city', 'lat_c',  'lon_c')
        
    return geo_data


def get_general_tab(events_messages, csv_path, spark): 

    #получаем датасет с геоданными городов из csv-файла через функцию get_geo_cities  
    cities=get_geo_cities(csv_path, spark) 

    #рассчитываем датасет с информацией по городам, из которых направлены все сообщения 
    #(используем udf функцию для расчета расстояния)
    messages_cities=events_messages\
    .crossJoin(cities)\
    .withColumn('distance',udf_func( F.col('lat'), F.col('lat_c'), F.col('lon'), F.col('lon_c')).cast('float'))\
    .withColumn("distance_rank", F.row_number().over(Window().partitionBy(['user_id', 'message_id'])\
                                                            .orderBy(F.asc("distance")))) \
    .where("distance_rank == 1")\
    .drop('distance_rank' , 'distance' , 'lat', 'lon', 'id', 'lat_c', 'lon_c')
    return messages_cities  

def get_act_city(events_messages, csv_path, spark): 

    #получаем датасет с геоданными городов из csv-файла через функцию get_geo_cities   

    cities=get_geo_cities(csv_path, spark) 

    #рассчитываем датасет с информацией по городам, из которых направлены последние отправленные сообщения 
    #(используем udf функцию для расчета расстояния) 
    messages_cities=events_messages\
    .withColumn("datetime_rank", F.row_number().over(Window().partitionBy(['user_id', 'message_id'])\
                                                     .orderBy(F.desc("datetime"))))\
    .where("datetime_rank == 1").orderBy('user_id')\
    .crossJoin(cities)\
    .withColumn('distance',udf_func( F.col('lat'), F.col('lat_c'), F.col('lon'), F.col('lon_c')).cast('float'))\
    .withColumn("distance_rank", F.row_number().over(Window().partitionBy(['user_id']).orderBy(F.asc("distance")))) \
    .where("distance_rank == 1")\
    .select('user_id', F.col('city').alias('act_city'), 'date' )

    return messages_cities

def main():   

    #получаем параметры из командной строки
    base_input_path=sys.argv[1]
    date=sys.argv[2]
    depth=int(sys.argv[3])
    csv_path=sys.argv[4]
    output_path=sys.argv[5]
    
    spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()   


    #рассчитываем пути по заданным параметрам - дате, глубине расчета и источнику
    paths=input_paths(date, depth, base_input_path)
 
    #вычисляем датасет со всеми событиями
    try:
        events=spark.read.option("basePath", base_input_path).parquet(*paths)

        #вычисляем датасет со всеми сообщениями
        events_messages=events.where(F.col('event_type')=='message')\
        .withColumn('date', F.date_trunc("day", F.coalesce(F.col('event.datetime'), F.col('event.message_ts'))) )\
        .selectExpr('event.message_from as user_id', 'event.message_id', 'date' ,'event.datetime','lat', 'lon' )

        #вычисляем датасет со всеми сообщениями за заданный период
        general_tb= get_general_tab(events_messages, csv_path, spark)

        #вычисляем датасет с городом, из которого было отправлено последнее сообщение
        act_cities_df = get_act_city(events_messages, csv_path, spark)


        #рассчитываем таблицу с изменениями города отправки сообщения
        temp_df=general_tb.withColumn('max_date',F.max('date')\
                            .over(Window().partitionBy('user_id')))\
                            .withColumn('city_lag',F.lead('city',1,'empty')\
                            .over(Window().partitionBy('user_id').orderBy(F.col('date').desc())))\
                            .filter(F.col('city') != F.col('city_lag'))

        #рассчитываем адрес города, из которого были отправлены 27 дней подряд сообщения от пользователя
        home_city=temp_df\
            .withColumnRenamed('city', 'home_city')\
            .withColumn('date_lag', F.coalesce( F.lag('date')\
                        .over(Window().partitionBy('user_id').orderBy(F.col('date').desc())), F.col('max_date')))\
            .withColumn('date_diff', F.datediff(F.col('date_lag'), F.col('date')))\
            .where(F.col('date_diff')>27)\
            .withColumn('rank', F.row_number()\
            .over(Window.partitionBy('user_id').orderBy(F.col('date').desc())))\
            .where(F.col('rank')==1)\
            .drop('date_diff', 'date_lag', 'max_date', 'city_lag', 'rank')

        #рассчитываем кол-во смен города по каждому пользователю   
        travel_count=(temp_df.groupBy('user_id').count().withColumnRenamed('count', 'travel_count'))

        #рассчитываем список городов, которые посетил пользователь
        travel_list=temp_df.groupBy('user_id').agg(F.collect_list('city').alias('travel_array'))

        #рассчитываем локальное время
        time_local=act_cities_df.withColumn('timezone',  F.lit('Australia/Sydney'))\
        .withColumn('localtime',  F.from_utc_timestamp(F.col("date"),F.col('timezone')))\
        .drop('timezone', 'city', 'date', 'datetime' , 'act_city')

        #объединяем все данные в одну витрину                                                                         
        final=act_cities_df.select('user_id', 'act_city')\
        .join(home_city, 'user_id', 'left')\
        .join(travel_count,'user_id', 'left')\
        .join(travel_list, 'user_id', 'left')\
        .join(time_local, 'user_id', 'left')\
        .select('user_id', 'act_city', 'home_city', 'travel_count',  'travel_array', 'localtime')
        final.orderBy('user_id').where('home_city is not null').show(30)

        #записываем результат по заданному пути
        final.write \
            .mode("overwrite") \
            .parquet(f'{output_path}/user_address/user_address_{date}_{depth}')
    except:
        print('All paths were ignored')
if __name__ == "__main__":

    main()
