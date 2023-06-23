

def main():
     
    #вычисляем датасет со всеми сообщениями за заданный период
    events=spark.read.parquet(*paths)
    messages=events.where(F.col('event_type')=='message')\
    .select(F.col('event.message_from').alias('user_id'),  'lon',  'lat', 'datetime', F.to_date(F.col('datetime')).alias('date'))

 
    general_tb = get_act_city('2022-05-25')

#     #рассчитываем таблицу с изменениями города отправки сообщения
    temp_df = general_tb.withColumn('max_date', F.max(F.col('date')).over(Window().PartitionBy('user_id') ))\
    .withColumn('city_lag', F.lag('city', -1, 'start').over(Window().PartitionBy('user_id').orderBy(F.col('date').desc())))\
    .where(F.col('city') != F.col('city_lag'))

#     #рассчитываем адрес города, из которого были отправлены 27 дней подряд сообщения
    home_city=temp_df.withColumn('date_lag', F.coalesce( F.lag(F.col('date')).over(Window().PartitionBy('user_id').orderBy(F.col('date').desc(), F.col('max_date')))\
    .withColumn('date_diff', F.diff(F.col('date_log'), F.col('date')).over(Window().PartitionBy('user_id')\
    .where(F.col('date_diff')>=27)\
    .withColumn('rank', F.row_number().over(Window().PartitionBy('user_id').orderBy(F.col('date').desc())))\
    .where(F.col('rank')==1)\
    .drop('date_diff', 'date_lag', 'max_date', 'city_lag', 'rank')

    #рассчитываем кол-во смен города по каждому пользователю
                                                                               
    travel_count=(temp_df.groupBy('user_id').count().withColumnRenamed('count', 'travel_count'))

    #рассчитываем список городов, которые посетил пользователь
    
    travel_list=temp_df.groupBy('user_id').agg(F.collect_list('city')).withColumnRenamed('city', 'travel_array')

    #рассчитываем локальное время
    
    time_l=act_city.withColumn('timezone', F.concat(F.lit('Australia/'), F.lit('city')))\
    .withColumn('localtime',  F.from_utc_timestamp(F.col("datetime"),F.col('timezone')))\
    .drop('timezone', 'city', 'date')

    #объединяем все датасеты в одну витрину
                                                                               
#     final=act_city.select('user_id', 'act_city').join(home_city, 'user_id', 'left')\
#     .join(travel_count,'user_id', 'left')\
#     .join(travel_list, 'user_id', 'left')\
#     .join(time_l, 'user_id, 'left)


                               
                               
 
                            
main()
     #рассчитываем все пары пользователей, которые переписывались
     user_left_contacts = events.where(F.col('event_type')=='message') \
        .select(col('event.message_from').alias('user_left'), col('event.message_to').alias('user_right')) \
    user_right_contacts = events.where(F.col('event_type')=='message') \
        .select(col('event.message_to').alias('user_left'), col('event.message_from').alias('user_right')) \
    realcontacts=user_left_contacts.union(user_right_contacts).distinct()
                
    #рассчитываем все возможные пары пользователей                    
     all_users=events.where(F.col('event_type')=='message')\
     .selectExpr('event.message_from as left_user')
     .union (events.where(F.col('event_type')=='message').selectExpr('event.message_to as right_user')).distinct()
     all_possible_contacts=all_users.crossJoin(all_users)

     #рассчитываем все пары пользователей, которые не переписывались                      
     no_contacts_users_with_dupl=all_possible_contacts.substract(real_contacts) 
     no_contacts_users=no_contacts_users_with_dupl\
                      .union(no_contacts_users_with_dupl\
                      .select(F.col('user_right').alias('user_left'),F.col('user_left').alias('user_right')))\
                      .distinct()
   
     