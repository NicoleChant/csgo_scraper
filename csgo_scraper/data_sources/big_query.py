from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

class BigQuery:

    def __init__(self , table_name : str):
        self.client = bigquery.Client()
        self.table_id = f"{os.environ.get('PROJECT_ID')}.{os.environ.get('DATASET_ID')}.{table_name}"

    def create_table(self):
        schema = [
            bigquery.SchemaField("match_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("league", "STRING"),
            bigquery.SchemaField("rank_num", "STRING"),
            bigquery.SchemaField("rank_title", "STRING"),
            bigquery.SchemaField("map", "STRING"),
            bigquery.SchemaField("team_scores", "RECORD" , mode = "NUlLABLE",
                            fields = (bigquery.SchemaField("first","INT64"),
                                      bigquery.SchemaField("second","INT64"))
                                      ),
            bigquery.SchemaField("upload_time", "STRING"),
            bigquery.SchemaField("teams", "RECORD" , mode = "NULLABLE",
                        fields=( bigquery.SchemaField("first" , "RECORD" , mode = "REPEATED" ,
                                                fields=(
                    bigquery.SchemaField('player', 'STRING'),
                            )),
                        bigquery.SchemaField("second" , "RECORD" , mode = "REPEATED",
                                fields = (
                                    bigquery.SchemaField("player","STRING"),
                                )
                        )
                        )
                    )
            ]
        table = bigquery.Table(self.table_id, schema=schema)
        table = self.client.create_table(table)
        print("Created table {}.{}.{}".format(table.project,
                                              table.dataset_id,
                                              table.table_id))


    def upload_data(self , data : dict):
        #table = self.client.get_table(self.table_id)
        errors = self.client.insert_rows_json(self.table_id , data)

    def delete_table(self):
        self.client.delete_table(self.table_id , not_found_ok = True)
