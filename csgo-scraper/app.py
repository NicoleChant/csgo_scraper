from scraper import ScraperFactory
import time
from random import gauss
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
import json

class App:

    def __init__(self):
        self.factory = ScraperFactory()
        self.scraper = self.factory.get_scraper("Matches")
        self.bq = BigQuery("match")

    def run(self):
        sampled_time = gauss(1200 , 120)
        while True:
            data = self.scraper.parse_data()
            self.bq.upload_data(data)

            break
            #self.big_query.upload_data(data)
            time.sleep(sampled_time)


class BigQuery:

    def __init__(self , table_name : str):
        self.client = bigquery.Client()
        self.table_id = f"{os.environ.get('PROJECT_ID')}.{os.environ.get('DATASET_ID')}.{table_name}"

    def create_table_1(self):
        schema = [
            bigquery.SchemaField(
                "team",
                "RECORD",
                mode="NULLABLE",
                fields=(bigquery.SchemaField("first", "STRING"),
                        bigquery.SchemaField("second", "STRING")))
        ]
        table = bigquery.Table(self.table_id , schema = schema)
        table = self.client.create_table(table)
        print("Created table {}.{}.{}".format(table.project,
                                              table.dataset_id,
                                              table.table_id))

    def insert_data(self):
        rows_to_insert = [{
            "team": {
                "first": "h",
                "second": "2"
            }
        }]

        errors = self.client.insert_rows_json(self.table_id, rows_to_insert)  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

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
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def delete_table(self):
        self.client.delete_table(self.table_id , not_found_ok = True)

    def clean_duplicates(self):
        pass


if __name__ == "__main__":
    bq = BigQuery("match")
    #bq.delete_table()
    #bq.create_table()

    #bq.insert_data()
    app = App()
    app.run()
