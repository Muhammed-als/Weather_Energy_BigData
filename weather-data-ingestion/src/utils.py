import re
from datetime import datetime, timedelta
from pprint import pprint

weather_stations = {
	'06030': {'Egrid': 'DK1', 'coordinates': (57.0963, 9.8505), 'name': 'Aalborg'},
	'06034': {'Egrid': 'DK1', 'coordinates': (57.5044, 10.2227), 'name': 'Hjørring'},
	'06041': {'Egrid': 'DK1', 'coordinates': (57.7364, 10.6316), 'name': 'Frederikshavn'},
	'06043': {'Egrid': 'DK1', 'coordinates': (57.4064, 10.5157), 'name': 'Frederikshavn'},
	'06049': {'Egrid': 'DK1', 'coordinates': (56.5604, 10.0929), 'name': 'Mariagerfjord'},
	'06052': {'Egrid': 'DK1', 'coordinates': (56.7068, 8.215), 'name': 'Lemvig'},
	'06058': {'Egrid': 'DK1', 'coordinates': (56.0005, 8.1293), 'name': 'Ringkøbing-Skjern'},
	'06060': {'Egrid': 'DK1', 'coordinates': (56.2935, 9.1138), 'name': 'Herning'},
	'06065': {'Egrid': 'DK1', 'coordinates': (56.7558, 9.5067), 'name': 'Vesthimmerlands'},
	'06070': {'Egrid': 'DK1', 'coordinates': (56.3083, 10.6254), 'name': 'Syddjurs'},
	'06071': {'Egrid': 'DK1', 'coordinates': (56.4432, 10.9579), 'name': 'Norddjurs'},
	'06073': {'Egrid': 'DK1', 'coordinates': (56.0955, 10.5135), 'name': 'Aarhus'},
	'06074': {'Egrid': 'DK1', 'coordinates': (56.0803, 10.1353), 'name': 'Aarhus'},
	'06079': {'Egrid': 'DK1', 'coordinates': (56.7011, 11.5436), 'name': 'Norddjurs'},
	'06080': {'Egrid': 'DK1', 'coordinates': (55.5281, 8.5626), 'name': 'Esbjerg'},
	'06081': {'Egrid': 'DK1', 'coordinates': (55.5575, 8.0828), 'name': 'Fanø'},
	'06089': {'Egrid': 'DK1', 'coordinates': (55.4956, 8.398), 'name': 'Esbjerg'},
	'06096': {'Egrid': 'DK1', 'coordinates': (55.1908, 8.56), 'name': 'Fanø'},
	'06104': {'Egrid': 'DK1', 'coordinates': (55.7383, 9.1744), 'name': 'Billund'},
	'06108': {'Egrid': 'DK1', 'coordinates': (55.4376, 9.3338), 'name': 'Kolding'},
	'06110': {'Egrid': 'DK1', 'coordinates': (55.2251, 9.2634), 'name': 'Haderslev'},
	'06111': {'Egrid': 'DK1', 'coordinates': (55.2976, 9.7994), 'name': 'Assens'},
	'06118': {'Egrid': 'DK1', 'coordinates': (54.9616, 9.793), 'name': 'Sønderborg'},
	'06119': {'Egrid': 'DK1', 'coordinates': (54.8528, 9.988), 'name': 'Sønderborg'},
	'06120': {'Egrid': 'DK1', 'coordinates': (55.4735, 10.3297), 'name': 'Nordfyns'},
	'06124': {'Egrid': 'DK1', 'coordinates': (55.0144, 10.5693), 'name': 'Svendborg'},
	'06135': {'Egrid': 'DK2', 'coordinates': (55.3224, 11.3879), 'name': 'Slagelse'},
	'06141': {'Egrid': 'DK2', 'coordinates': (54.8275, 11.3292), 'name': 'Lolland'},
	'06147': {'Egrid': 'DK2', 'coordinates': (54.879, 12.1841), 'name': 'Vordingborg'},
	'06149': {'Egrid': 'DK2', 'coordinates': (54.5639, 11.964), 'name': 'Guldborgsund'},
	'06151': {'Egrid': 'DK1', 'coordinates': (55.1598, 11.1339), 'name': 'Nyborg'},
	'06154': {'Egrid': 'DK2', 'coordinates': (55.2075, 11.8605), 'name': 'Næstved'},
	'06156': {'Egrid': 'DK2', 'coordinates': (55.7154, 11.7088), 'name': 'Holbæk'},
	'06159': {'Egrid': 'DK2', 'coordinates': (55.7435, 10.8694), 'name': 'Kalundborg'},
	'06168': {'Egrid': 'DK2', 'coordinates': (56.1193, 12.3424), 'name': 'Gribskov'},
	'06169': {'Egrid': 'DK2', 'coordinates': (56.0067, 11.2805), 'name': 'Odsherred'},
	'06170': {'Egrid': 'DK2', 'coordinates': (55.5867, 12.1366), 'name': 'Solrød'},
	'06180': {'Egrid': 'DK2', 'coordinates': (55.614, 12.6455), 'name': 'Tårnby'},
	'06181': {'Egrid': 'DK2', 'coordinates': (55.7664, 12.5263), 'name': 'Lyngby-Taarbæk'},
	'06183': {'Egrid': 'DK2', 'coordinates': (55.5364, 12.7114), 'name': 'Dragør'},
	'06188': {'Egrid': 'DK2', 'coordinates': (55.8764, 12.4121), 'name': 'Allerød'},
	'06190': {'Egrid': 'DK2', 'coordinates': (55.0677, 14.7494), 'name': 'Bornholm'},
	'06191': {'Egrid': 'DK2', 'coordinates': (55.3218, 15.1875), 'name': 'Bornholm'},
	'06193': {'Egrid': 'DK2', 'coordinates': (55.2979, 14.7718), 'name': 'Bornholm'}
}


import logging
import time
# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,  # Set the desired logging level
    format="%(asctime)s|%(levelname)s| %(message)s",  # Define the log format
    handlers=[
        logging.StreamHandler()  # StreamHandler sends logs to stdout
    ]
)
logger = logging.getLogger()

def log(msg, log_to_kafka_pod=True, log_with_print=False, level=logging.INFO):
    if log_to_kafka_pod:
        if level == logging.INFO:
            logger.info(msg)
        elif level == logging.WARNING:
            logger.warning(msg)
        elif level == logging.ERROR:
            logger.error(msg)
    if log_with_print:
        print(msg)

def str_to_date(date_string) -> datetime:
    return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")

def get_current_model_run() -> str:
    current_datetime = datetime.now()
    rounded_datetime = current_datetime - timedelta(hours=(current_datetime.hour) % 3, # When accounting for beeing one hour ahead: hours=(current_datetime.hour-1) % 3 +1
                                                    minutes=current_datetime.minute, 
                                                    seconds=current_datetime.second, 
                                                    microseconds=current_datetime.microsecond)
    return rounded_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') #formattet to look like 'YYYY-MM-DDTHH:MM:SSZ'

def get_next_model_run(date_str) -> str:
    dt = str_to_date(date_str)
    now = datetime.now()
    dt += timedelta(hours=3)
    while dt + timedelta(days=2) < now:
        print(f"Time for DMI query refers to model run more than two days old: {dt} + {timedelta(days=2)} < {now} == {dt + timedelta(days=2) < now}. Add 3 hours and check again")
        dt += timedelta(hours=3)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def find_latest_date_string(strings) -> str:
    latest_date = None

    for s in strings:
        # HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-17T030000Z.grib
        #                  ^^^^^^^^^^^^^^^^^^
        date = s.split('_')[3]
        
        first_date = datetime.strptime(date, "%Y-%m-%dT%H%M%SZ")
        
        # Check if this is the latest second date
        if latest_date is None or first_date > latest_date:
            latest_date = first_date

    return latest_date.strftime("%Y-%m-%dT%H:%M:%SZ")

"""
# Example usage
strings = [
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-17T030000Z.grib",
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-18T030000Z.grib",
    "HARMONIE_DINI_SF_2024-11-16T150000Z_2024-11-18T040000Z.grib",
    "HARMONIE_DINI_SF_2024-11-16T160000Z_2024-11-18T000000Z.grib",
    "HARMONIE_DINI_SF_2024-11-16T140000Z_2024-11-18T040000Z.grib",
    "HARMONIE_DINI_SF_2024-11-16T150000Z_2024-11-18T040000Z.grib",
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-18T070000Z.grib",
    "HARMONIE_DINI_SF_2024-11-14T160000Z_2024-11-18T060000Z.grib",
    "HARMONIE_DINI_SF_2024-11-14T180000Z_2024-11-16T030000Z.grib"
    "HARMONIE_DINI_SF_2024-11-15T150000Z_2024-11-16T060000Z.grib"
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-16T050000Z.grib"
]

result = find_latest_date_string(strings)
print("String with the latest first date:", result)
"""

from math import radians, sin, cos, sqrt, asin

def haversine(lat1, lon1, lat2, lon2) -> float:
    """
    Calculate the great-circle distance between two points 
    on the Earth using the Haversine formula.

    returns distance in km.
    """
    R = 6371  # Radius of Earth in kilometers
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return R * 2 * asin( sqrt(a) )

def find_closest_geolocations_for_municipalities(station_locations):
    """
    For each municipality, find the closest station.
    """
    return_dict = {}
    
    for name, (lat1, lon1, energygrid) in municipalities_coordinates.items():
        closest_distance = float('inf')
        
        for stationID, (lat2, lon2) in station_locations.items():
            distance = haversine(lat1, lon1, lat2, lon2)
            if distance < closest_distance:
                closest_distance = distance
                lat = lat2
                lon = lon2
        
        return_dict[stationID] = {"name": name, "lat": lat, "lon": lon, "Egrid": energygrid}
    
    return return_dict

#haversine(55.657, 12.353, 55.871, 12.350)

municipalities_coordinates = {
    "Albertslund": (55.657, 12.353, "DK2"),
    "Allerød": (55.871, 12.350, "DK2"),
    "Ballerup": (55.731, 12.360, "DK2"),
    "Bornholm": (55.103, 14.916, "DK2"),
    "Brøndby": (55.646, 12.414, "DK2"),
    "Dragør": (55.592, 12.671, "DK2"),
    "Egedal": (55.768, 12.215, "DK2"),
    "Fredensborg": (55.970, 12.403, "DK2"),
    "Frederiksberg": (55.678, 12.531, "DK2"),
    "Frederikssund": (55.831, 12.068, "DK2"),
    "Furesø": (55.785, 12.370, "DK2"),
    "Gentofte": (55.750, 12.550, "DK2"),
    "Gladsaxe": (55.733, 12.487, "DK2"),
    "Glostrup": (55.666, 12.401, "DK2"),
    "Gribskov": (56.070, 12.307, "DK2"),
    "Halsnæs": (55.970, 12.020, "DK2"),
    "Helsingør": (56.036, 12.612, "DK2"),
    "Herlev": (55.728, 12.439, "DK2"),
    "Hillerød": (55.927, 12.308, "DK2"),
    "Hvidovre": (55.657, 12.475, "DK2"),
    "Høje-Taastrup": (55.650, 12.300, "DK2"),
    "Hørsholm": (55.882, 12.501, "DK2"),
    "Ishøj": (55.616, 12.357, "DK2"),
    "København": (55.676, 12.568, "DK2"),
    "Lyngby-Taarbæk": (55.771, 12.503, "DK2"),
    "Rudersdal": (55.819, 12.500, "DK2"),
    "Rødovre": (55.681, 12.455, "DK2"),
    "Tårnby": (55.617, 12.605, "DK2"),
    "Vallensbæk": (55.615, 12.374, "DK2"),
    "Faxe": (55.255, 12.110, "DK2"),
    "Greve": (55.583, 12.300, "DK2"),
    "Guldborgsund": (54.769, 11.870, "DK2"),
    "Holbæk": (55.718, 11.713, "DK2"),
    "Kalundborg": (55.681, 11.088, "DK2"),
    "Køge": (55.458, 12.182, "DK2"),
    "Lejre": (55.602, 11.967, "DK2"),
    "Lolland": (54.769, 11.356, "DK2"),
    "Næstved": (55.230, 11.760, "DK2"),
    "Odsherred": (55.847, 11.562, "DK2"),
    "Ringsted": (55.442, 11.791, "DK2"),
    "Roskilde": (55.641, 12.080, "DK2"),
    "Slagelse": (55.402, 11.352, "DK2"),
    "Solrød": (55.537, 12.200, "DK2"),
    "Sorø": (55.431, 11.554, "DK2"),
    "Stevns": (55.303, 12.334, "DK2"),
    "Vordingborg": (55.009, 11.910, "DK2"),
    "Assens": (55.270, 9.883, "DK1"),
    "Billund": (55.729, 9.114, "DK1"),
    "Esbjerg": (55.476, 8.459, "DK1"),
    "Fanø": (55.426, 8.406, "DK1"),
    "Fredericia": (55.570, 9.753, "DK1"),
    "Faaborg-Midtfyn": (55.104, 10.239, "DK1"),
    "Haderslev": (55.250, 9.489, "DK1"),
    "Kerteminde": (55.448, 10.652, "DK1"),
    "Kolding": (55.493, 9.472, "DK1"),
    "Langeland": (54.874, 10.667, "DK1"),
    "Middelfart": (55.505, 9.749, "DK1"),
    "Nordfyns": (55.496, 10.260, "DK1"),
    "Nyborg": (55.310, 10.791, "DK1"),
    "Odense": (55.394, 10.388, "DK1"),
    "Svendborg": (55.059, 10.609, "DK1"),
    "Sønderborg": (54.913, 9.791, "DK1"),
    "Tønder": (54.935, 8.863, "DK1"),
    "Varde": (55.621, 8.481, "DK1"),
    "Vejen": (55.483, 9.134, "DK1"),
    "Vejle": (55.711, 9.536, "DK1"),
    "Ærø": (54.884, 10.407, "DK1"),
    "Aabenraa": (55.044, 9.417, "DK1"),
    "Favrskov": (56.341, 10.040, "DK1"),
    "Hedensted": (55.766, 9.707, "DK1"),
    "Herning": (56.139, 8.973, "DK1"),
    "Holstebro": (56.361, 8.619, "DK1"),
    "Horsens": (55.860, 9.850, "DK1"),
    "Ikast-Brande": (56.086, 9.156, "DK1"),
    "Lemvig": (56.546, 8.313, "DK1"),
    "Norddjurs": (56.495, 10.733, "DK1"),
    "Odder": (55.975, 10.145, "DK1"),
    "Randers": (56.460, 10.036, "DK1"),
    "Ringkøbing-Skjern": (56.088, 8.223, "DK1"),
    "Samsø": (55.857, 10.610, "DK1"),
    "Silkeborg": (56.174, 9.547, "DK1"),
    "Skanderborg": (56.041, 9.926, "DK1"),
    "Skive": (56.566, 9.029, "DK1"),
    "Struer": (56.491, 8.583, "DK1"),
    "Syddjurs": (56.307, 10.683, "DK1"),
    "Viborg": (56.451, 9.402, "DK1"),
    "Aarhus": (56.162, 10.203, "DK1"),
    "Brønderslev": (57.270, 9.945, "DK1"),
    "Frederikshavn": (57.440, 10.536, "DK1"),
    "Hjørring": (57.462, 9.982, "DK1"),
    "Jammerbugt": (57.084, 9.618, "DK1"),
    "Læsø": (57.274, 11.005, "DK1"),
    "Mariagerfjord": (56.644, 9.989, "DK1"),
    "Morsø": (56.797, 8.697, "DK1"),
    "Rebild": (56.833, 9.824, "DK1"),
    "Thisted": (56.955, 8.694, "DK1"),
    "Vesthimmerlands": (56.802, 9.360, "DK1"),
    "Aalborg": (57.048, 9.918, "DK1")
}
def find_closest_geolocations_to_stations_from_grib(coordinateset, latlons):
    closest_distance = float('inf')
    for lat, lon in latlons:
        distance = haversine(lat, lon, coordinateset[0], coordinateset[1])
        if distance < closest_distance:
            closest_distance = distance
            closest_lat = lat
            closest_lon = lon
    return closest_lat, closest_lon
