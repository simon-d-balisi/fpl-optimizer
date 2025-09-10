"""
This file will be for reading data directly from the fpl data github by vaastav (link below) directly into Azure blob storage.

Each season will have a player ID list table and a table with one row per player-matchweek.

Link: https://github.com/vaastav/Fantasy-Premier-League/tree/master
"""

import os
import dotenv
import io

import pandas as pd

from azure.storage.blob import BlobServiceClient




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



def upload_to_azure(csv_file: pd.DataFrame, cxn_str: str, azure_container: str, azure_blob_name: str):
    csv_name = io.StringIO()
    csv_file.to_csv(csv_name, index=False)
    csv_string = csv_name.getvalue()

    try:
        blob_service_clinet = BlobServiceClient.from_connection_string(cxn_str)
        container_client = blob_service_clinet.get_container_client(azure_container)

        if not container_client.exists():
            container_client.create_container()
            print(f'Container "{azure_container}" created.')

        blob_client = container_client.get_blob_client(azure_blob_name)
        blob_client.upload_blob(csv_string, overwrite=True, content_settings={"content_type": "text/csv"})
        return True

    except Exception as e:
        print(f'An error occurred when uploading {azure_blob_name}: {e}')
        return False


def main():
    dotenv.load_dotenv
    cxn_str = os.getenv("AZURE_CXN_STRING")
    container_name = os.getenv("AZURE_CSV_CONTAINER_NAME")

    for s in range(START_SEASON, END_SEASON+1):
        season_tag = f'20{s:02}-{s+1:02}'
        print(f'Moving data for the {season_tag} season')

        file = get_season_gw_file(s)
        if file is None:
            print('Data loading unsuccessful')
            continue

        azure_blob_name = f'player_gw_raw__20{s}_{s+1}.csv'
        upload_to_azure(file, cxn_str, container_name, azure_blob_name)

