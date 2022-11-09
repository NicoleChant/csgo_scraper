import cloudscraper
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pathlib
from abc import abstractmethod , ABC
from dataclasses import dataclass , field
import re
from datetime import datetime , timedelta
from pydantic import BaseModel

class SecuredEntity:

    def __init__(self , entity , missing_attribute , fill_value):
        if not hasattr(entity , "attrs"):
            self.entity.attrs = {missing_attribute: fill_value}

class Match(BaseModel):

    league : str
    rank : str
    upload_time : datetime
    maps : str
    first_team : list[str]
    second_team : list[str]
    first_team_score : int
    second_team_score : int


class Scraper:

    def __init__(self):
        self.scraper = cloudscraper.create_scraper()
        self.base_url = "https://csgostats.gg"
        self.offline_dir = pathlib.Path("offline_scraping/")

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

    def parse_data(self , offline : bool = False):
        soup = self.get_soup(endpoint = "match" , offline = offline)
        table = soup.find("table" , class_ = "table").find("tbody")
        rows = table.select("tr[class^='p-row']")

        data = []
        for row in rows:
            league = row.find("td").find("img").attrs["src"].split("/")[-1].strip(".png")
            rank = row.select_one("td:nth-of-type(2)").find("img").attrs["src"].split("/")[-1].strip(".png")
            uploaded_mins_ago = row.select_one("td:nth-of-type(3)").text.strip().split(" ")[0]
            upload_time = datetime.now() - timedelta(minutes= int(uploaded_mins_ago))
            maps = row.find("img" , src = lambda src : src and re.compile("maps").search(src)).attrs["src"].split("/")[-1].strip(".png")
            first_team , second_team = row.find_all("td" , class_ = "team-td")

            players_first_team = list(map(lambda tag : tag.attrs["title"] ,
                                    first_team.find_all("img" , {"src":True})))
            players_second_team = list(map(lambda tag : tag.attrs["title"] ,
                                    second_team.find_all("img" , {"src":True})))

            first_team_score , second_team_score = tuple(map(lambda tag : tag.text.strip() ,
                                            row.find_all("td" , class_ = "team-score")))

            # print(league)
            # print(rank)
            # print(uploaded_mins_ago)
            # print(maps)
            # print(players_first_team)
            # print(first_team_score)
            # print(players_second_team)
            # print(second_team_score)

            data.append(Match.parse_obj({"league":league ,
                        "rank": rank ,
                        "upload_time": upload_time,
                        "maps": maps,
                        "first_team": players_first_team,
                        "second_team": players_second_team,
                        "first_team_score":first_team_score,
                        "second_team_score":second_team_score}))



        return data


class ScraperFactory():

    def __init__(self):
        self.scrapers = {"Matches": MatchesScraper}

    def get_scraper(self , scraper_name : str , *args , **kwargs):
        scraper = self.scrapers.get(scraper_name.strip())
        if scraper:
            return scraper(*args , **kwargs)

if __name__ == "__main__":
    ##offline scraping

    factory = ScraperFactory()
    scraper = factory.get_scraper("Matches")
    #scraper.store_html(endpoint = "match")

    parsed_data = scraper.parse_data(offline = True)
    print(repr(parsed_data[0]))
