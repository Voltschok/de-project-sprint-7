from datetime import datetime 
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2020, 1, 1),
                                }

dag_spark = DAG(
                        dag_id = "sprint-7-project_dag",
                        default_args=default_args,
                        schedule_interval=None,
                        )


first_vitrin = SparkSubmitOperator(
                        task_id='user_address',
                        dag=dag_spark,
                        application ='/src/scripts/user_address.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [  
                            '/user/voltschok/data/geo/events'
                            '2022-05-25',
                            '1',
                            '/user/voltschok/data/geo/test.csv',
                            '/user/voltschok/data/geo/analytics/'                            
                        ],
                        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.sql.broadcastTimeout": 1200,
        },
                        executor_cores = 2,
                        executor_memory = '4g',
                        )


second_vitrin = SparkSubmitOperator(
                        task_id='zone_month_week',
                        dag=dag_spark,
                        application ='/src/scripts/city_stats.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [  
                            '/user/voltschok/data/geo/events'
                            '2022-05-25',
                            '1',
                            '/user/voltschok/data/geo/test.csv',
                            '/user/voltschok/data/geo/analytics/'                            
                        ],
                        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.sql.broadcastTimeout": 1200,
        },
                        executor_cores = 2,
                        executor_memory = '4g',
                        )

third_vitrin = SparkSubmitOperator(
                        task_id='friend_recommendation',
                        dag=dag_spark,
                        application ='/src/scripts/friend_offers.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [  
                            '/user/voltschok/data/geo/events'
                            '2022-05-25',
                            '1',
                            '/user/voltschok/data/geo/test.csv',
                            '/user/voltschok/data/geo/analytics/'                            
                        ],
                        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.sql.broadcastTimeout": 1200,
        },
                        executor_cores = 2,
                        executor_memory = '4g',
                        )

first_vitrin >> second_vitrin >> third_vitrin
