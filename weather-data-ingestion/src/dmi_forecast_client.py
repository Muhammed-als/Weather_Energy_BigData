import requests
from pprint import pprint

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
            print(f"Error - request not 200, but insted {r.status_code}")
            return {}, {}, 0

        data = r.json()
        if data['numberReturned'] != 61:
            print("Error - Not exactly 61 returned urls ")
            print(f"numberReturned: {data['numberReturned']}")
            if data['numberReturned'] == 0 or 'features' not in data:
                return {}, {}, data['numberReturned']
            #print("First data entry:")
            #pprint(data['features'][0])
            #if data['numberReturned'] > 1:
            #    print("Last entry:")
            #    pprint(data['features'][data['numberReturned']-1])
            #return {}, {}, data['numberReturned']

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