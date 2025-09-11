"""
This file will be for parsing through the fpl API and getting all data for the current season and matchweek.

It will upload the raw data directly into azure blob storage.
"""

from .api_data_getter import get_data



def get_full_data():
    """ 
    Retrieves full fpl status data. If unsuccessful, returns None.
    """
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"

    return get_data(url)



def get_individual_player_data(player_id: int):
    """ 
    Retrieves the player-specific detailed data.

    Args:
        player_id (int): ID of the player whose data is to be retrieved
    """
    url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
    
    return get_data(url)


def get_fixtures_data():
    """
    Retrieves the fixture information for the current season.
    """

    url = "https://fantasy.premierleague.com/api/fixtures/"

    return get_data(url)