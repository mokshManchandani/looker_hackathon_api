from google.cloud import bigquery
import os
class BigqueryService:
    def __init__(self, name_mapping):
        self.client = bigquery.Client()
        self.name_mapping = name_mapping
        self.__dataset_id = 'alpine-dynamo-404312.looker_hackathon_q'
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "key.json"

    
    def create_tables(self):
        for table_name, file_name in self.name_mapping.items():
            job_config = bigquery.LoadJobConfig(
                autodetect = True,
                source_format = bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            table_ref = f"{self.__dataset_id}.{table_name}"
            with open(file_name,'rb') as f:
                job = self.client.load_table_from_file(f,table_ref,job_config=job_config)
                job.result()