from csgo_scraper.scraper import ScraperFactory
from csgo_scraper.webhook import DiscordClient
import time
from random import gauss
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
import json
import logging
from typing import Union
from pathlib import Path

logging.basicConfig(level = logging.INFO ,
                    format = "%(asctime)s:%(message)s",
                    datefmt = "%Y-%M-%D %H:%M:%S")

class App:

    def __init__(self):
        self.factory = ScraperFactory()
        self.scraper = self.factory.get_scraper("Matches")
        self.bq = BigQuery("match")
        self.discord_client = DiscordClient(title = "Scraping Report")

    def run(self):
        sampled_time = gauss(3600 , 200)
        logging.info("Sending request...")
        while True:
            try:
                data = self.scraper.parse_data()
                unique_reports = self.get_unique_matches(data)

                ##data stats
                data_length = len(data)
                unique_reports_number = len(unique_reports)

                ##send discord webhook
                self.discord_client.send_webhook("CSGO Report" , data_length , unique_reports_number)

                ##upload data
                #self.bq.upload_data(unique_data)
            except Exception as e:

                ##notify discord if something went wrong
                self.discord_client.send_webhook("CSGO Report")
                logging.info(f"An error occurred! {e}")

            break
            time.sleep(sampled_time)

    def get_unique_matches(self , data : list[dict]) -> Union[bool ,list]:
        if not os.path.isdir("local_storage"):
            os.mkdir("local_storage")

        try:
            ##Read old data if they exist
            if os.path.isfile("local_storage/temp_data.json"):
                with open("local_storage/temp_data.json" , encoding = "utf-8" , mode = "r+") as f:
                    past_data = json.load(f)

                past_match_ids = list(map(lambda row : row["match_id"] , past_data))
                unique_data = list(filter(lambda x: x["match_id"] not in past_match_ids, data))

            #rewrite the current data as past data in the local storage directory
            with open("local_storage/temp_data.json" , encoding = "utf-8" , mode = "w+") as f:
                json.dump(data , f)

            #returns the filtered unique data
            return unique_data
        except OSError as e:
            ##if something goes wrong return False
            return False


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

    def clean_duplicates(self):
        pass


if __name__ == "__main__":
    bq = BigQuery("match")
    app = App()
    app.run()
