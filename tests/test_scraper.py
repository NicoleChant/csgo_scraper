import pytest

class TestMatchScraper:

    @pytest.fixture(scope="class")
    def base_url(self , match_scraper):
        yield match_scraper.base_url

    def test_valid_base_url(self , base_url):
        assert isinstance(base_url , str)
        assert "/" in base_url
        assert ":" in base_url
        assert "." in base_url

    # NOT AVAILABLE IN FREE TRIAL

    # @pyest.fixture(scope="class")
    # def match_data(self , match_scraper):
    #     yield match_scraper.parse_data()

    # def test_data_type(self , match_data):
    #     assert isinstance(match_data , list)

    # def test_instance_type(self , match_data):
    #     assert isinstance(match_data[0] , dict)

    # def test_data_length(self , match_data):
    #     assert len(match_data) == 50
    #     assert len(match_data[0]) == 8

    # @pytest.mark.parametrize("expected_key",["match_id",
    #                                         "league",
    #                                         "rank_title",
    #                                         "rank_num",
    #                                         "upload_time",
    #                                         "map",
    #                                         "teams",
    #                                         "team_scores"])
    # def test_valid_keys(self , expected_key , match_data):
    #     for data in match_data:
    #         assert expected_key in data
