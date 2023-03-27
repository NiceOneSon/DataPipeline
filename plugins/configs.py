import json
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from plugins.variables import NiceOneSon_variables

class NiceOneSon_config:
    with open('../etc/schema.json', 'r') as f:
        table_schema = json.load(f)
    
    # 
    pyspark_job_config = {
                        'reference' : {'project_id' : NiceOneSon_variables.PROJECT_ID},
                        'placement' : {'cluster_name' : NiceOneSon_variables.CLUSTER_NAME},
                        'pyspark_job' : {'main_python_file_uri' :  NiceOneSon_variables.PYSPARK_URI,
                                         'args' : [
                                            NiceOneSon_variables.CSVHOME,
                                            NiceOneSon_variables.RAW_STOCK_CSV,
                                            NiceOneSon_variables.TRANSFORMED_FOLDER,
                                            NiceOneSon_variables.AGGREGATED_FOLDER
                                         ]}
    }
    
    cluster_config = ClusterGenerator(
        project_id = NiceOneSon_variables.PROJECT_ID,
        zone = "us-central1-a",
        master_machine_type = "n1-standard-2",
        worker_machine_type = "n1-standard-2",
        num_workers = 2,
        worker_disk_size = 300,
        master_disk_size = 300,
        storage_bucket = NiceOneSon_variables.BUCKET_NAME,
    ).make()
