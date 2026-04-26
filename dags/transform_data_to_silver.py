from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag, task
from datetime import datetime
import os




@dag(
    dag_id='transform_data_to_silver', 
    schedule='@daily', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['silver']
)

def transform_data_to_silver():

    def transform_bronze_to_silver():

        #PATH_BASE = '/usr/local/airflow/include'
        #path_origem = os.path.join(PATH_BASE, 'spark/transform_data_to_silver.py')
        
        test_submit = SparkSubmitOperator(
            task_id = 'transform_data',
            conn_id = 'spark_default',
            application = '/usr/local/airflow/include/spark/transform_bronze_to_silver.py',
            name = 'airflow-transform-data',
            packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            verbose = True,
            conf={
                "spark.master": "spark://spark-master:7077",
                "spark.driver.host": "scheduler", # <--- Diga ao Spark quem é o Driver (Astro)
                "spark.driver.bindAddress": "0.0.0.0"
            },
            deploy_mode='client'
        )

        test_submit

    transform_bronze_to_silver()


transform_data_to_silver()
