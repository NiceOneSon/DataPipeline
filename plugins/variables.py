from airflow.models import Variable
import os

class NiceOneSon_variables:
    # 1. Datas for prevention
    PROJECT_ID = Variable.get('stock_project')
    BUCKET_NAME = Variable.get('bucket')
    HOMEDIR = Variable.get('stock_home_dir')
    API_KEY = Variable.get('stock_api_key')
    STOCK_URL = Variable.get('stock_url')
    DATASET = Variable.get('stock_dataset')
    TABLE = Variable.get('stock_detail')
    TMP_TABLE = 'tmp_' + TABLE

    # 2. Not sensitive datas.
    GOOGLE_CONN_ID = "gcp"
    REGION = 'us-central1'
    ZONE = 'us-central1-c'
    TODAY = "{{ ds_nodash }}"
    CLUSTER_NAME = 'cluster-stocktrans'
    OBJ_NAME = None
    RAW_STOCK_CSV = 'stock_api.csv'
    TRANSFORMED_FOLDER = 'transformed_stock'
    AGGREGATED_FOLDER = 'aggregated_stock'
    LOCALHOME = '/var/lib/airflow/dags'

    # 3. Directories being combinated with variables.
    # SPARKHOME = os.path.join(LOCALHOME, 'sparkfiles')
    JSONHOME = os.path.join(HOMEDIR, 'jsonfiles')
    LOGGINGHOME = os.path.join(HOMEDIR, 'logfiles')
    CSVHOME = os.path.join(HOMEDIR, 'csvfiles')
    PYSPARK_URI = os.path.join(LOCALHOME, Variable.get('the_file_stock_transform'))
