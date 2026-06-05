from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from datetime import datetime
import os




@dag( 
    dag_id='apply_gold', 
    schedule='@daily', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze']
    )
def apply_gold():
    
    def get_archive_gold():
        
        
        test_submit = SparkSubmitOperator(
            task_id = 'input_to_gold',
            conn_id = 'spark_default',
            application = '/usr/local/airflow/include/spark/get_gold.py',
            name = 'airflow-transform-data',
            packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            verbose = True,
            conf={
                "spark.master": "spark://spark-master:7077",
                "spark.driver.host": "scheduler",
                "spark.driver.bindAddress": "0.0.0.0"
            },
            deploy_mode='client'
        )

        test_submit
    


    get_archive_gold()
    






apply_gold()