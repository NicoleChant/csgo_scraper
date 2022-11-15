import pytest
from csgo_scraper.scraper import ScraperFactory

@pytest.fixture(scope="class")
def match_scraper():
    match = ScraperFactory().get_scraper("Matches")
    yield match
