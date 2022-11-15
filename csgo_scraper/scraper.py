import cloudscraper
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import bs4
import pathlib
from abc import abstractmethod , ABC
from dataclasses import dataclass , field
import re
from datetime import datetime , timedelta
from pytz import timezone

# class SecureSoup(BeautifulSoup):

#     def find(self , *args , **kwargs):
#         return super().find(*args , **kwargs)

# class SecureSoup(bs4.element.Tag):

#     def secure(self, missing_attribute: str, fill_value: str):
#         if not hasattr(self, missing_attribute):
#             self.attrs = {missing_attribute: fill_value}
#         return self

class Scraper:

    def __init__(self):
        self.scraper = cloudscraper.create_scraper()
        self.base_url = os.getenv("BASE_URL")
        self.offline_dir = pathlib.Path("offline_scraping/")
        self.tz = timezone(os.getenv("LOCATION"))

    def store_html(self , endpoint : str , max_depth : int = 1) -> bool:
        if max_depth > 2:
            return False

        try:
            filepath = self.offline_dir / f"{endpoint}.html"
            html = self.get_soup(endpoint = endpoint , return_html = True)
            with filepath.open(encoding = "utf-8" , mode = "w+") as f:
                f.write(html)
            return True
        except OSError as e:
            self.offline_dir.mkdir(parents=True , exist_ok = True)
            return self.store_html(endpoint , max_depth + 1)

    def read_html(self , endpoint : str):
        try:
            filepath = self.offline_dir / f"{endpoint}.html"
            with filepath.open(encoding = "utf-8" , mode = "r+") as f:
                return f.read()
        except OSError as e:
            return False

    def get_soup(self , endpoint : str , offline : bool = False , return_html : bool = False):
        if offline:
            return BeautifulSoup(self.read_html(endpoint) , 'html.parser')

        url = urljoin(self.base_url , endpoint)
        html = self.scraper.get(url).text
        if return_html:
            return html
        return BeautifulSoup(html , 'html.parser')

    @abstractmethod
    def parse_data(self):
        pass



class MatchesScraper(Scraper):


    def protect_tag(self , tag , title : bool = False) -> str:
        if not tag:
            return "/"

        tag = tag.attrs

        if title and "title" in tag:
            return tag["title"]
        elif "src" in tag:
            return tag["src"]
        elif "data-cfsrc" in tag:
            return tag["data-cfsrc"]
        return "/"

    def _parse_row(self , row) -> dict:
        """HTML row parser for matches table"""

        match_id = re.search(re.compile(r"/match/(\d+)") ,
                    row.find("a" , href = lambda href : href and re.compile(r"/match/(\d+)").search(href)).attrs["href"]).group(1)
        league = self.protect_tag(row.find("td").find("img")).split("/")[-1].strip(".png")

        if "-" in league:
            league = league.split("-")[0]

        rank_title = self.protect_tag(row.select_one("td:nth-of-type(2)").find("img") , title = True).strip().strip("/")

        rank_num = self.protect_tag(row.select_one("td:nth-of-type(2)").find("img"))\
                                            .split("/")[-1]\
                                            .strip(".png")

        uploaded_mins_ago = row.select_one("td:nth-of-type(3)")\
                                .text.strip()\
                                .split(" ")[0]

        ##to string
        upload_time = (datetime.now(self.tz) - timedelta(minutes= int(uploaded_mins_ago))).strftime("%y-%m-%d %H:%M:%S")
        #upload_time = datetime.now(self.tz) - timedelta(minutes= int(uploaded_mins_ago))

        maps = self.protect_tag(row.find("img" , src = lambda src : src and re.compile("maps").search(src)))\
                                    .split("/")[-1]\
                                    .strip(".png")

        first_team , second_team = row.find_all("td" , class_ = "team-td")

        players_first_team = list(map(lambda tag : {"player" : tag.attrs["title"]} ,
                                first_team.find_all("img" , {"src":True})))

        players_second_team = list(map(lambda tag : {"player" : tag.attrs["title"]} ,
                                second_team.find_all("img" , {"src":True})))


        first_team_score , second_team_score = tuple(map(lambda tag : tag.text.strip() ,
                                        row.find_all("td" , class_ = "team-score")))

        return {"match_id" : int(match_id) ,
                "league":league ,
                "rank_title" : rank_title ,
                "rank_num": rank_num ,
                "upload_time": upload_time,
                "map": maps,
                "teams": {"first" : players_first_team , "second" : players_second_team },
                "team_scores": {"first" : int(first_team_score) , "second" : int(second_team_score) }
                }


    def parse_data(self , offline : bool = False) -> list[dict]:
        ## get online or offline soup
        soup = self.get_soup(endpoint = "match" , offline = offline)
        table = soup.find("table" , class_ = "table").find("tbody")
        rows = table.select("tr[class^='p-row']")

        data = []
        for row in rows:
            try:
                data.append(self._parse_row(row))
            except Exception as e:
                continue
        return data


class ScraperFactory():

    def __init__(self):
        self.scrapers = {"Matches": MatchesScraper}

    def get_scraper(self , scraper_name : str , *args , **kwargs):
        scraper = self.scrapers.get(scraper_name.strip())
        if scraper:
            return scraper(*args , **kwargs)
