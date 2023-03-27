import os
from datetime import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateClusterOperator,
                                                               DataprocDeleteClusterOperator,
                                                               DataprocSubmitJobOperator
                                                               )
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from plugins.sql_queries import NiceOneSon_SQL_Queries
from plugins.configs import NiceOneSon_config
from plugins.variables import NiceOneSon_variables
from plugins.udf_functions import extract
# ------------------------------------------------------------------------------------
    # Variables


default_args = {
                'owner' : 'airflow',
                'depends_on_past' : False,
                'email_on_failure' : False,
                'email_on_retry' : False,
                'retries' : 2,
                'start_date' : datetime(2023, 1, 1),
                # 'retry_delay' : timedelta(seconds = 300),
                'schedule_interval' : '0 0 * * *'
                }
with DAG(
    dag_id = 'stock_etl_pipeline', 
    default_args = default_args,
    catchup=False
    ) as dag:

    extractOperator = PythonOperator(
        task_id = 'extract',
        python_callable = extract,
        op_args = ["{{ ds_nodash }}"],
        dag = dag
    )
    
    create_cluster = DataprocCreateClusterOperator(
        task_id = "create_cluster",
        project_id = NiceOneSon_variables.PROJECT_ID,
        cluster_config = NiceOneSon_config.cluster_config,
        region = 'us-central1',
        cluster_name = 'cluster-stocktrans',
    )
    
    spark_transform = DataprocSubmitJobOperator(
        task_id = 'stock_spark_job_operator',
        job = NiceOneSon_config.pyspark_job_config,
        region = 'us-central1',
        project_id = NiceOneSon_variables.PROJECT_ID,
    
    )
    
    delete_cluster = DataprocDeleteClusterOperator(
        task_id = "delete_cluster", 
        project_id = NiceOneSon_variables.PROJECT_ID, 
        cluster_name = 'cluster-stocktrans', 
        region = 'us-central1'
    )

    create_table = BigQueryExecuteQueryOperator(
            task_id = 'tmp_table_create',
            destination_dataset_table = None, 
            gcp_conn_id = NiceOneSon_variables.GOOGLE_CONN_ID,
            sql = NiceOneSon_SQL_Queries.\
                create_temp_table.\
                format(NiceOneSon_variables.PROJECT_ID,\
                        NiceOneSon_variables.DATASET,\
                        NiceOneSon_variables.TABLE),
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition="WRITE_TRUNCATE")
    

    gcs_to_biquery = GCSToBigQueryOperator(
        task_id = "gcs_to_bigquery",
        destination_project_dataset_table = '.'.join([NiceOneSon_variables.PROJECT_ID,\
                                                    NiceOneSon_variables.DATASET,\
                                                    NiceOneSon_variables.TMP_TABLE]),
        bucket = NiceOneSon_variables.BUCKET_NAME,
        write_disposition = "WRITE_APPEND", # WRITE_TRUNCATE : 쓰기 전 Truncate, WRITE_EMPTY : 비어있지 않으면 Fail.
        source_objects = os.path.join('csvfiles', NiceOneSon_variables.TRANSFORMED_FOLDER, '*.csv'),
        source_format = "csv",
        schema_fields = NiceOneSon_config.table_schema
    )

    merge_table = BigQueryExecuteQueryOperator(
            task_id = 'stock_idempotency',
            destination_dataset_table = None, 
            gcp_conn_id = NiceOneSon_variables.GOOGLE_CONN_ID,
            sql = [NiceOneSon_SQL_Queries.truncate_origin_table.format(NiceOneSon_variables.PROJECT_ID,\
                                                                    NiceOneSon_variables.DATASET,\
                                                                    NiceOneSon_variables.TABLE),
                NiceOneSon_SQL_Queries.merge_tables.format(NiceOneSon_variables.PROJECT_ID,\
                                                            NiceOneSon_variables.DATASET,\
                                                            NiceOneSon_variables.TABLE),
                NiceOneSon_SQL_Queries.drop_temp_table.format(NiceOneSon_variables.PROJECT_ID, 
                                                                NiceOneSon_variables.DATASET, 
                                                                NiceOneSon_variables.TABLE)],
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition="WRITE_TRUNCATE")
    
    delete_object = GCSDeleteObjectsOperator(
        task_id = 'delete_obj',
        bucket_name=NiceOneSon_variables.BUCKET_NAME,
        prefix = 'csvfiles/',
        gcp_conn_id=NiceOneSon_variables.GOOGLE_CONN_ID,
        trigger_rule = TriggerRule.ALL_DONE
        )
    
    extractOperator >> create_cluster >> spark_transform 
    spark_transform >> delete_cluster
    spark_transform >> create_table >> gcs_to_biquery >> merge_table
    gcs_to_biquery >> delete_object