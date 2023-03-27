import os
import logging
import tempfile
import requests
import pandas as pd
from plugins.variables import NiceOneSon_variables
from airflow.providers.google.cloud.operators.gcs import GCSHook

def extract(today):
    TODAY = today
    url = NiceOneSon_variables.STOCK_URL + f'serviceKey={NiceOneSon_variables.API_KEY}' + f'&basDt={today}'
    bucket = NiceOneSon_variables.BUCKET_NAME
    result = requests.get(url).json()

    logging.info(f'check today : {today}, url : {url}')
    try:
        result = result['response']['body']['items']['item']
    except ValueError as v:
        logging.info(f'error is occurred during getting info using REST API. the day is {today}')

    df = pd.DataFrame(result)
    if len(df) == 0:
        raise Exception(f"Even though the status of result of rest api is correct, but there is no row. please check the parameter, date : {today}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = os.path.join(tmp_dir, f'tmp.csv')
        df.to_csv(tmp_path, index = False)
        OBJ_NAME = os.path.join('csvfiles',\
                                NiceOneSon_variables.RAW_STOCK_CSV)
        gcs_hook = GCSHook(gcp_conn_id='gcp')
        gcs_hook.upload(
            bucket_name = bucket,
            object_name = OBJ_NAME,
            filename = tmp_path
        )
        logging.info(f'The file is completely inserted.')
