import requests
from pprint import pprint
from src.utils import *

class DMIForecastClient:
    _model = 'harmonie_dini_sf' # model Harmonie - Denmark, Iceland, Netherlands and Ireland - surface

    def __init__(self, model_run, api_key, bbox):     
        self._model_run = model_run
        self._api_key = api_key
        self._bbox = bbox

    def getForecastUrls(self) -> list | list | int:
        url = f'https://dmigw.govcloud.dk/v1/forecastdata/collections/{self._model}/items?modelRun={self._model_run}&bbox={self._bbox}&api-key={self._api_key}'
        r = requests.get(url)

        if r.status_code != 200:
            log(f"Request not 200, but insted {r.status_code}", level=logging.ERROR)
            return {}, {}, 0

        data = r.json()
        if data['numberReturned'] != 61:
            log("Not exactly 61 returned urls ", level=logging.ERROR)
            log(f"numberReturned: {data['numberReturned']}")
            if data['numberReturned'] == 0 or 'features' not in data:
                return {}, {}, data['numberReturned']

        return_list = []
        keys = []
        for feature in data['features']:
            list_element = {
                'url': feature['asset']['data']['href'],
                'bbox': feature['bbox'],
                'properties': feature['properties']
            }
            return_list.append(list_element)
            keys.append(feature['id'])

        return keys, return_list, data['numberReturned']   