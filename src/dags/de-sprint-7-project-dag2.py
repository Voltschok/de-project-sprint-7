from datetime import datetime 
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.decorators import dag, task
from airflow.sensors.external_task  import ExternalTaskSensor

import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2022, 1, 1),
                                }

dag_spark = DAG(
                        dag_id = "sprint-7-project_dag_zone_week_month",
                        default_args=default_args,
                        schedule_interval='@weekly',
                        catchup=False
                        )

 
second_vitrin = SparkSubmitOperator(
                        task_id='zone_month_week',
                        dag=dag_spark,
                        application ='/lessons/dags/2nd_vitrin.py',
                        conn_id= 'yarn_spark',
                        application_args = [  
                            
                            '/user/voltschok/data/geo/events',
                            '{{ ds }}',
                            '1',
                            '/user/voltschok/data/geo/cities/geo.csv',
                            '/user/voltschok/data/analytics/'                            
                        ],
                        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.sql.broadcastTimeout": 1200,
        },
                        executor_cores = 2,
                        executor_memory = '4g',
                        )
 
 
sensor=ExternalTaskSensor(task_id='dag_sensor_2nd_vitrin_update',
                        external_dag_id = 'project_dag_user_address_friend_recommendation',
                        mode = 'poke',
                        external_task_id='update_data',
                        dag=dag_spark)  

sensor >>  second_vitrin 
