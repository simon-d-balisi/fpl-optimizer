"""
This file will be for reading data directly from the fpl data github by vaastav (link below) directly into Azure blob storage.

Each season will have a player ID list table and a table with one row per player-matchweek.

Link: https://github.com/vaastav/Fantasy-Premier-League/tree/master
"""
import pandas as pd
import requests
import json




START_SEASON = 15 # beginning of first season of available data (representing 2015-16)
END_SEASON = 24 # beginning of final season of available data (representing 2024-25)



def get_season_gw_file(season: int):
    season_tag = f'20{season}-{season+1}'
    url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season_tag}/gws/merged_gw.csv"

    try:
        csv_file = pd.read_csv(url)

    except Exception as e:
        print(f'Exception occurred: {e}')
        return None

    else:
        return csv_file



def upload_to_azure(csv_file: pd.DataFrame):
    pass



def main():
    for s in range(START_SEASON, END_SEASON+1):
        season_tag = f'20{s}-{s+1}'
        print(f'Moving data for the {season_tag} season')

        file = get_season_gw_file(s)
        if file is None:
            print('Data loading unsuccessful')
            continue

        upload_to_azure(file)

