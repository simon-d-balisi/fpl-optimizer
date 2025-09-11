import requests
import json
import time



def get_data(url):
    response = None

    while response is None:
        try:
            response = requests.get(url)
        except:
            time.sleep(5)
    
    if response.status_code != 200:
        print(f'Response was code: {response.status_code} from url {url}')
        return None
    
    return json.loads(response.text)