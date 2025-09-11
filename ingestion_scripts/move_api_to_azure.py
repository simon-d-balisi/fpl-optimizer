"""
This file has the functions for moving the api data into azure blob storage
"""
import datetime
import json
import dotenv
import os

from .get_fpl_api_data import *

from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContentSettings


def upload_to_azure(json_dict, cxn_str: str, azure_container: str, azure_blob_name: str):
    json_str = json.dumps(json_dict, indent=4)

    try:
        blob_service_clinet = BlobServiceClient.from_connection_string(cxn_str)
        container_client = blob_service_clinet.get_container_client(azure_container)

        if not container_client.exists():
            container_client.create_container()
            print(f'Container "{azure_container}" created.')

        blob_client = container_client.get_blob_client(azure_blob_name)
        blob_client.upload_blob(json_str, overwrite=True, content_settings=ContentSettings(content_type='application/json'))
        return True

    except Exception as e:
        print(f'An error occurred when uploading {azure_blob_name}: {e}')
        return False






def get_and_move_data():
    dotenv.load_dotenv()
    cxn_str = os.getenv("AZURE_CXN_STRING")
    container_name = os.getenv("AZURE_CSV_CONTAINER_NAME")

    full = get_full_data()
    if full is not None:
        print('Successfully obtained full data from API')

    fix = get_fixtures_data()
    if fix is not None:
        print('Successfully obtained fixture data from API')

    player_ids = [i['id'] for i in full['elements']]

    player_data = []

    for id in player_ids:
        q = get_individual_player_data(id)
        if q is not None:
            q['id'] = id
            player_data.append(q)

    print(f'Successfully obtained player data from API for {len(q)} players')


    time_tag = datetime.datetime.now().astimezone(datetime.timezone.utc).strftime("%Y_%m_%d_%H_%M_%S")

    f = upload_to_azure(full, cxn_str, container_name, f'full_fpl_{time_tag}.json')
    x = upload_to_azure(fix, cxn_str, container_name, f'fixtures_fpl_{time_tag}.json')
    p = upload_to_azure(player_data, cxn_str, container_name, f'players_merged_fpl_{time_tag}.json')

    return f and x and p