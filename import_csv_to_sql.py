from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


database='postgres_localhost'# postgres_localhost had been set in airflow
s3conn='minio_conn'# minio_conn(s3) had been set in airflow


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook(s3conn)
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

default_args={
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
    }

with DAG(
    dag_id='data_pipeline',
    default_args=default_args,
    start_date=datetime(2023,5,22),
    schedule_interval='0 0 * * *'

) as dag:
    task1 = PostgresOperator(
        task_id='Create_postgres_bighand_table',
        postgres_conn_id=database,
        sql="""
            drop table if exists bighand;
            create table if not exists BigHand(
            	id int primary key,
            	date date,
            	foreignBUY real,
            	foreignSELL real,
            	foreignTOTAL real,
            	ProprietaryBUY real,
            	ProprietarySELL real,
            	ProprietaryTOTAL real
            );
            SET datestyle = dmy;
            COPY BigHand(id, date, foreignBUY, foreignSELL, foreignTOTAL, ProprietaryBUY, ProprietarySELL, ProprietaryTOTAL)
            FROM 'D:/Desktop/big_hand.csv'
            DELIMITER ','
            CSV HEADER;
            """
    )
    
    task2 = PostgresOperator(
        task_id='Create_postgres_suplydemand_table',
        postgres_conn_id=database,
        sql="""
            drop table if exists supplydemand;
            create table if not exists supplydemand(
            	id int primary key,
            	date date,
            	supply int,
            	demand int,
            	exchanged int,
            	RSI52 int
            );
            SET datestyle = dmy;
            COPY supplydemand(id, date, supply, demand, exchanged, RSI52)
            FROM 'D:/Desktop/supply_demand.csv'
            DELIMITER ','
            CSV HEADER;
            """
    )
    
    task3 = PostgresOperator(
        task_id='Create_postgres_combine_table',
        postgres_conn_id=database,
        sql="""
            drop table if exists combine;
            create table if not exists combine as(
            	select bighand.date, foreignBUY, foreignSELL, foreignTOTAL, ProprietaryBUY, ProprietarySELL, ProprietaryTOTAL,
            	supply, demand, exchanged, RSI52
            	from bighand
            		full outer join supplydemand
            		on bighand.date=supplydemand.date
            	order by bighand.date desc
            	);
            COPY combine TO 'D:/Desktop/over_view.csv' DELIMITER ',' CSV HEADER;
            """
    )
    
    # Upload the file
    task4 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': 'D:/Desktop/over_view.csv',
            'key': '{{ ds_nodash }}_airflows3_combine.csv',
            'bucket_name': 'airflows3'
        }
    )
    
    [task1,task2] >> task3 >> task4
    
