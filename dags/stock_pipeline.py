import logging
import tempfile
import requests
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateClusterOperator,
                                                               DataprocDeleteClusterOperator,
                                                               DataprocSubmitJobOperator,
                                                               ClusterGenerator)
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# ------------------------------------------------------------------------------------
    # Variables

# 1. Using Variable for data prevention.
PROJECT_ID = Variable.get('stock_project')
BUCKET_NAME = Variable.get('bucket')
HOMEDIR = Variable.get('stock_home_dir')
API_KEY = Variable.get('stock_api_key')
STOCK_URL = Variable.get('stock_url')

# 2. Not sensitive datas.
GOOGLE_CONN_ID = "gcp"
CLUSTER_NAME = 'cluster-stocktrans'
REGION = 'us-central1'
ZONE = 'us-central1-c'
TODAY = "{{ ds_nodash }}"
OBJ_NAME = None

# 3. Directories being combinated with variables.
SPARKHOME = os.path.join(HOMEDIR, 'sparkfiles')
JSONHOME = os.path.join(HOMEDIR, 'jsonfiles')
LOGGINGHOME = os.path.join(HOMEDIR, 'logfiles')
CSVHOME = os.path.join(HOMEDIR, 'csvfiles')
PYSPARK_URI = os.path.join(SPARKHOME, Variable.get('the_file_stock_transform'))

# 4. Queries for idempotency.
sqls = ("""
        create table {0}.{1}.tmp_{2} as select * from {0}.{1}.{2}
        """.format(PROJECT_ID, Variable.get('stock_dataset'), Variable.get('stock_detail')),
        """
        truncate table {0}.{1}.{2}
        """.format(PROJECT_ID, Variable.get('stock_dataset'), Variable.get('stock_detail'))
        ,"""
        insert into stock_dataset.stock_detail(itmsNm, mrktCtg, clpr, vs, fltRt
                ,mkp ,hipr, lopr, trqu, trPrc, lstgStCnt, mrktTotAmt, update, modified, basDt)
        select itmsNm, mrktCtg, clpr, vs, fltRt
            ,mkp ,hipr, lopr, trqu, trPrc, lstgStCnt, mrktTotAmt, update, modified, basDt
        from
            (SELECT *, row_number() over(partition by basDt, itmsNm, mrktCtg order by update desc) as rnum
            FROM stock_dataset.tmp_stock_detail
            )
        where rnum = 1
        """.format(PROJECT_ID, Variable.get('stock_dataset'), Variable.get('stock_detail'))
        ,"""
        drop table {0}.{1}.tmp_{2}
        """.format(PROJECT_ID, Variable.get('stock_dataset'), Variable.get('stock_detail')))



# ------------------------------------------------------------------------------------
    # Configs

# 1. A configuration for dag
default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'start_date' : datetime(2023, 1, 1), #days_ago(2), # 정확한 의미 
    'retry_delay' : timedelta(minutes=30),
    'catchup' : False,
    'schedule_interval' : '0 0 * * *'
}

# 2. A configuration for Pyspark.
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI,
                    "args" : [CSVHOME,
                              CSVHOME,
                              LOGGINGHOME,
                              f'{TODAY}_stock_api.csv',
                              f'{TODAY}_stock_detail',
                              TODAY]
                    },
    }

# 3. A configuration for dataproc cluster set.
CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone="us-central1-a",
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    num_workers=2,
    worker_disk_size=300,
    master_disk_size=300,
    storage_bucket=BUCKET_NAME,
).make()

# 4. Schema for a temp table.
SCHEMA_FIELD = [
    { 'mode' : 'NULLABLE', 'name' : 'itmsNm', 'type' : 'STRING' },
    { 'mode' : 'NULLABLE', 'name' : 'mrktCtg', 'type' : 'STRING' }, 
    { 'mode' : 'NULLABLE', 'name' : 'clpr', 'type' : 'INTEGER' },
    { 'mode' : 'NULLABLE', 'name' : 'vs', 'type' : 'INTEGER' },
    { 'mode' : 'NULLABLE', 'name' : 'fltRt', 'type' : 'FLOAT' }, 
    { 'mode' : 'NULLABLE', 'name' : 'mkp', 'type' : 'INTEGER' }, 
    { 'mode' : 'NULLABLE', 'name' : 'hipr', 'type' : 'INTEGER' }, 
    { 'mode' : 'NULLABLE', 'name' : 'lopr', 'type' : 'INTEGER' },
    { 'mode' : 'NULLABLE', 'name' : 'trqu', 'type' : 'INTEGER' }, 
    { 'mode' : 'NULLABLE', 'name' : 'trPrc', 'type' : 'INTEGER' }, 
    { 'mode' : 'NULLABLE', 'name' : 'lstgStCnt', 'type' : 'INTEGER' }, 
    { 'mode' : 'NULLABLE', 'name' : 'mrktTotAmt', 'type' : 'INTEGER' }, 
    { 'mode' : 'NULLABLE', 'name' : 'update', 'type' : 'TIMESTAMP' }, 
    { 'mode' : 'NULLABLE', 'name' : 'modified', 'type' : 'TIMESTAMP' }, 
    { 'mode' : 'NULLABLE', 'name' : 'basDT', 'type' : 'DATE' } 
    ]


# ------------------------------------------------------------------------------------
    # Functions.

# 1. URL for REST API.
def MakeUrl(APIKEY, url, date):
    return url + f'serviceKey={APIKEY}' + f'&basDt={date}'

# 2. Extract stock data
def extract(today, **context):
    global OBJ_NAME, TODAY
    TODAY = today
    url = STOCK_URL + f'serviceKey={API_KEY}' + f'&basDt={today}'
    bucket = Variable.get('stock_bucket')
    result = requests.get(url).json()
    logging.info(f'check today : {today}, url : {url}')
    try:
        result = result['response']['body']['items']['item']
    except ValueError as v:
        logging.info(f'error is occurred during getting info using REST API. the day is {today}')

    df = pd.DataFrame(result)
    logging.info(df)
    if len(df) == 0:
        raise Exception(f"Even though the status of result of rest api is correct, but there is no row. please check the parameter, date : {today}")
    
    bucket = Variable.get('stock_bucket')

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = os.path.join(tmp_dir, f'_{today}.csv')
        df.to_csv(tmp_path, index = False)
        OBJ_NAME = f'csvfiles/{today}_stock_api.csv'
        gcs_hook = GCSHook(gcp_conn_id='gcp')
        gcs_hook.upload(
            bucket_name = bucket,
            object_name = OBJ_NAME,
            filename = tmp_path
        )
        logging.info(f'The file is completely inserted.')



# ------------------------------------------------------------------------------------
    # DAG.

with DAG(
    dag_id = 'stock_etl_pipeline', 
    default_args = default_args,
    catchup=False) as dag:

    
    extractOperator = PythonOperator(
        task_id = 'extract',
        python_callable = extract,
        # params = dict(today = TODAY), 왜 Params은 jinja template이 적용 안되는지
        op_args = [TODAY],
        dag = dag
    )
    
    create_cluster = DataprocCreateClusterOperator(
        task_id = "create_cluster",
        project_id = PROJECT_ID,
        cluster_config = CLUSTER_CONFIG,
        region = REGION,
        cluster_name = CLUSTER_NAME,
    )

    spark_transform = DataprocSubmitJobOperator(
        task_id = 'stock_spark_job_operator',
        job = PYSPARK_JOB,
        region = REGION,
        project_id = PROJECT_ID
    )
    
    delete_cluster = DataprocDeleteClusterOperator(
        task_id = "delete_cluster", 
        project_id = PROJECT_ID, 
        cluster_name = CLUSTER_NAME, 
        region = REGION
    )

    create_table = BigQueryExecuteQueryOperator(
            task_id = 'tmp_table_create',
            destination_dataset_table = None, 
            gcp_conn_id=GOOGLE_CONN_ID,
            sql = sqls[0],
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition="WRITE_TRUNCATE")
    
    gcs_to_biquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        destination_project_dataset_table = '.'.join([PROJECT_ID, Variable.get('stock_dataset'), 'tmp_' + Variable.get('stock_detail')]),
        bucket=BUCKET_NAME,
        write_disposition = "WRITE_APPEND", # WRITE_TRUNCATE : 쓰기 전 Truncate, WRITE_EMPTY : 비어있지 않으면 Fail.
        source_objects = os.path.join('csvfiles', f'{TODAY}_stock_detail', '*.csv'),
        source_format = "csv",
        schema_fields = SCHEMA_FIELD
    )

    merge_table = BigQueryExecuteQueryOperator(
            task_id = 'stock_idempotency',
            destination_dataset_table = None, 
            gcp_conn_id=GOOGLE_CONN_ID,
            sql = sqls[1:],
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition="WRITE_TRUNCATE")

    extractOperator >> create_cluster >> spark_transform 
    spark_transform >> delete_cluster
    spark_transform >> create_table >> gcs_to_biquery >> merge_table