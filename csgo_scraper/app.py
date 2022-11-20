from csgo_scraper.scraper import ScraperFactory
from csgo_scraper.webhook import DiscordClient
import time
from random import gauss
import os
import json
import logging
from typing import Union
from pathlib import Path
import argparse
from csgo_scraper.data_sources.big_query import BigQuery

logging.basicConfig(level = logging.INFO ,
                    format = "%(asctime)s:%(message)s",
                    datefmt = "%Y-%M-%D %H:%M:%S")

class App:

    def __init__(self):
        self.factory = ScraperFactory()
        self.scraper = self.factory.get_scraper("Matches")
        self.bq = BigQuery(os.getenv("TABLE_ID"))
        self.discord_client = DiscordClient(title = "Scraping Report")

    def run(self , average_waiting_time : float = 1800 , std : float = 50 , verbose : bool = False , upload : bool = True , only_once : bool = False):
        sampled_time = gauss(average_waiting_time , std)
        logging.info("Sending request...")
        while True:
            try:
                data = self.scraper.parse_data()
                unique_reports = self.get_unique_matches(data , verbose = verbose)

                ##data stats
                data_length = len(data)
                unique_reports_number = len(unique_reports)

                ##send discord webhook
                self.discord_client.send_webhook("CSGO Report" , data_length , unique_reports_number)

                ##upload data
                if upload:
                    self.bq.upload_data(unique_reports)

            except Exception as e:

                ##notify discord if something went wrong
                self.discord_client.send_webhook("CSGO Report")
                logging.info(f"An error occurred! {e}")

            if only_once:
                break

            time.sleep(sampled_time)


    def get_unique_matches(self , data : list[dict] , verbose : bool = False) -> Union[bool ,list]:
        if not os.path.isdir("local_storage"):
            os.mkdir("local_storage")

        try:
            ##Read old data if they exist
            unique_data = None
            if os.path.isfile("local_storage/temp_data.json"):
                with open("local_storage/temp_data.json" , encoding = "utf-8" , mode = "r+") as f:
                    past_data = json.load(f)

                past_match_ids = list(map(lambda row : row["match_id"] , past_data))
                unique_data = list(filter(lambda x: x["match_id"] not in past_match_ids , data))

                if verbose:
                    for row in data:
                        if row["match_id"] not in past_match_ids:
                            print("[*" + row["rank_title"] + " " + str(row["match_id"]) + "*]")
                        else:
                            print("  " + row["rank_title"] + " " + str(row["match_id"]) + "  ")


            #rewrite the current data as past data in the local storage directory
            with open("local_storage/temp_data.json" , encoding = "utf-8" , mode = "w+") as f:
                json.dump(data , f , indent = 2)

            #returns the filtered unique data
            return unique_data if unique_data else data
        except OSError as e:

            ##if something goes wrong return False
            return False


def main():
    ##offline scraping

    factory = ScraperFactory()
    scraper = factory.get_scraper("Matches")
    #scraper.store_html(endpoint = "match")

    parsed_data = scraper.parse_data(offline = False)
    import sys
    unique_data = app.get_unique_matches(parsed_data)
    print(f"Unique data length {len(unique_data)}.")
    print(f"All data length {len(parsed_data)}")

    sys.exit(0)


if __name__ == "__main__":
    app = App()
    parser = argparse.ArgumentParser()
    parser.add_argument("-t","--table",type=str,default="match")
    parser.add_argument("-u","--upload",type=str,default=True)
    parser.add_argument("-oo","--only_once",type=str,default=False)
    parser.add_argument("-v","--verbose",type=str,default=False)

    args = parser.parse_args()

    bq = BigQuery(args.table)

    upload = args.upload
    only_once = args.only_once
    verbose = args.verbose

    if isinstance(upload , str):
        upload = eval(upload)

    if isinstance(only_once , str):
        only_once = eval(only_once)

    if isinstance(verbose , str):
        verbose = eval(verbose)

    app.run(upload = upload , only_once = only_once , verbose = verbose)
