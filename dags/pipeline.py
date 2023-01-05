import os
import logging
import datetime
from airflow.models import DAG
from airflow.hooks import S3_hook, postgres_hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

def log_data(msg):
    return logging.info(msg)

def iterate_directory(dir, filetype):
    files = []
    for filename in os.listdir(dir):
        if filename.endswith(filetype):
            files.append((dir+"/"+filename, filename))
    return files

def create_bucket(bucket_name, s3_connection="shinyui_s3_dend"):
    hook = S3_hook.S3Hook(s3_connection)
    log_data("S3 is creating bucket: {}".format(bucket_name))
    hook.create_bucket(bucket_name=bucket_name)
    log_data("Bucket: {} is created".format(bucket_name))

def upload_file_to_s3(bucket_name, files, s3_connection="shinyui_s3_dend"):
    hook = S3_hook.S3Hook(s3_connection)
    for idx, file in enumerate(files):
        file_path = file[0]
        file_name = file[1]
        log_data("Uploading file {file} to {bucket}".format(file=file_name, bucket=bucket_name))
        hook.load_file(filename=file_path, key=file_name, bucket_name=bucket_name)
        log_data("{file} uploaded to {bucket}".format(file=file_name, bucket=bucket_name))

def check_number_of_files(bucket_name, files, s3_connection="shinyui_s3_dend"):
    hook = S3_hook.S3Hook(s3_connection)
    number_of_files_s3 = len(hook.list_keys(bucket_name=bucket_name))
    number_of_files_local = len(files)
    if number_of_files_s3 != number_of_files_local:
        raise Exception("The number of files on {bucket} isn't equal to the record".format(bucket=bucket_name))
    else:
        log_data("The number of files on {bucket} is equal to the record".format(bucket=bucket_name))

def copy_data_to_redshift(table, s3_files):
    aws_hook = AwsHook("shinyui_aws_credentials") #aws_user_credentials
    credentials = aws_hook.get_credentials()
    redshift_hook = postgres_hook.PostgresHook("shinyui_redshift") #aws_redshift_credentials
    sql_stmt = """
    COPY {table}
    FROM '{s3}'
    ACCESS_KEY_ID '{id}'
    SECRET_ACCESS_KEY '{key}'
    IGNOREHEADER 1
    CSV
    DELIMITER ','
    """.format(table=table,
               s3=s3_files,
               id=credentials.access_key,
               key=credentials.secret_key)
    redshift_hook.run(sql_stmt)

def quality_check(table):
    redshift_hook = postgres_hook.PostgresHook("shinyui_redshift") #aws_redshift_credentials
    sql_stmt = """
    SELECT COUNT(*)
    FROM {table}
    """.format(table=table)
    number = redshift_hook.run(sql_stmt)
    if number <= 0:
        raise Exception("Table {table} quality test wasn't passed".format(table=table))
    else:
        logging.info("Table {table} passed the quality test".format(table=table))



dag = DAG(
    dag_id="capstone_project_pipeline",
    start_date=datetime.datetime(2019, 5, 26),
    schedule_interval=None,
)

iterate_directory_task = PythonOperator(
    task_id="iterate_directory",
    python_callable=iterate_directory,
    op_kwargs={
        "dir":"/usr/local/airflow/data",
        "filetype":".csv"
    },
    dag=dag
)

create_bucket_task = PythonOperator(
    task_id="create_bucket",
    python_callable=create_bucket,
    op_kwargs={
        "bucket_name":"shinyui-dend-capstone"
    },
    dag=dag
)

upload_file_to_s3_task = PythonOperator(
    task_id="upload_file_to_s3",
    python_callable=upload_file_to_s3,
    op_kwargs={
        "bucket_name":"shinyui-dend-capstone",
        "files":iterate_directory(dir="/usr/local/airflow/data", filetype=".csv")
    },
    dag=dag
)

check_number_of_files_task = PythonOperator(
    task_id="check_number_of_files",
    python_callable=check_number_of_files,
    op_kwargs={
        "bucket_name":"shinyui-dend-capstone",
        "files":iterate_directory(dir="/usr/local/airflow/data", filetype=".csv")
    },
    dag=dag
)

create_hour_table_task = PostgresOperator(
    task_id="create_hour_table",
    postgres_conn_id="shinyui_redshift",
    sql="""CREATE TABLE IF NOT EXISTS staging_eur_hour (
    date VARCHAR NOT NULL PRIMARY KEY,
    hour VARCHAR,
    bid_open REAL NOT NULL,
    bid_high REAL NOT NULL,
    bid_low REAL NOT NULL,
    bid_close REAL NOT NULL,
    bid_change REAL NOT NULL,
    ask_open REAL NOT NULL,
    ask_high REAL NOT NULL,
    ask_low REAL NOT NULL,
    ask_close REAL NOT NULL,
    ask_change REAL NOT NULL
    )""",
    dag=dag
)

create_minute_table_task = PostgresOperator(
    task_id="create_minute_table",
    postgres_conn_id="shinyui_redshift",
    sql="""CREATE TABLE IF NOT EXISTS staging_eur_minute (
    date VARCHAR NOT NULL PRIMARY KEY,
    minute VARCHAR,
    bid_open REAL NOT NULL,
    bid_high REAL NOT NULL,
    bid_low REAL NOT NULL,
    bid_close REAL NOT NULL,
    bid_change REAL NOT NULL,
    ask_open REAL NOT NULL,
    ask_high REAL NOT NULL,
    ask_low REAL NOT NULL,
    ask_close REAL NOT NULL,
    ask_change REAL NOT NULL
    )""",
    dag=dag
)

create_news_table_task = PostgresOperator(
    task_id="create_news_table",
    postgres_conn_id="shinyui_redshift",
    sql="""CREATE TABLE IF NOT EXISTS staging_eur_news (
    date VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR (65535),
    article VARCHAR (65535)
    )""",
    dag=dag
)

print_msg_task = PythonOperator(
    task_id="log_info",
    python_callable=log_data,
    op_kwargs={
        "msg":"Table successfully created, ready to load data"
    },
    dag=dag
)

copy_data_to_hour_table_task = PythonOperator(
    task_id="copy_eur_hour",
    python_callable=copy_data_to_redshift,
    op_kwargs={
        "table":"staging_eur_hour",
        "s3_files":"s3://shinyui-dend-capstone/eurusd_hour.csv"
    },
    dag=dag
)

copy_data_to_minute_table_task = PythonOperator(
    task_id="copy_eur_minute",
    python_callable=copy_data_to_redshift,
    op_kwargs={
        "table":"staging_eur_minute",
        "s3_files":"s3://shinyui-dend-capstone/eurusd_minute.csv"
    },
    dag=dag
)

copy_data_to_news_table_task = PythonOperator(
    task_id="copy_eur_news",
    python_callable=copy_data_to_redshift,
    op_kwargs={
        "table":"staging_eur_news",
        "s3_files":"s3://shinyui-dend-capstone/eurusd_news.csv"
    },
    dag=dag
)

hour_table_check_task = PythonOperator(
    task_id="quality_check_hour",
    python_callable=quality_check,
    op_kwargs={
        "table":"staging_eur_hour"
    },
    dag=dag
)

minute_table_check_task = PythonOperator(
    task_id="quality_check_minute",
    python_callable=quality_check,
    op_kwargs={
        "table":"staging_eur_minute"
    },
    dag=dag
)

news_table_check_task = PythonOperator(
    task_id="quality_check_hour",
    python_callable=quality_check,
    op_kwargs={
        "table":"staging_eur_news"
    },
    dag=dag
)

create_bucket_task >> iterate_directory_task >> upload_file_to_s3_task
upload_file_to_s3_task >> check_number_of_files_task
check_number_of_files_task >> [create_hour_table_task, create_minute_table_task, create_news_table_task] >> print_msg_task
print_msg_task >> [copy_data_to_hour_table_task, copy_data_to_minute_table_task, copy_data_to_news_table_task]
hour_table_check_task >> minute_table_check_task >> news_table_check_task