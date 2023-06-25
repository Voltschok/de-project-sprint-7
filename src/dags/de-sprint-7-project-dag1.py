from datetime import datetime 
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
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
                        dag_id = "sprint-7-project_dag_user_address_friend_recommendation",
                        default_args=default_args,
                        schedule_interval='@daily',
                        catchup=False
                        )

update_data = SparkSubmitOperator(
                        task_id='update_data',
                        dag=dag_spark,
                        application ='/lessons/dags/update_data.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [  
                           
                            '/user/master/data/geo/events',
                            '{{ ds }}',
                            '1',
                            '/user/voltschok/data/geo/events/'                            
                        ],
                        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.sql.broadcastTimeout": 1200,
        },
                        executor_cores = 2,
                        executor_memory = '4g',
                        )


first_vitrin = SparkSubmitOperator(
                        task_id='user_address',
                        dag=dag_spark,
                        application ='/lessons/dags/1st_vitrin.py' ,
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


branch = BranchDayOfWeekOperator(
        task_id="make_choice",
        follow_task_ids_if_true="zone_month_week",
        follow_task_ids_if_false="branch_false",
        week_day="Monday",
        dag=dag_spark
 
    )
empty_task = EmptyOperator(task_id='branch_false', dag=dag_spark)

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

third_vitrin = SparkSubmitOperator(
                        task_id='friend_recommendation',
                        dag=dag_spark,
                        application ='/lessons/dags/3d_vitrin.py' ,
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

update_data >> first_vitrin >> branch >> [second_vitrin, empty_task ] 
first_vitrin >> third_vitrin