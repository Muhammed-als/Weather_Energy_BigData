from datetime import datetime, timedelta
from pprint import pprint

weather_stations_forecast_latlons = {
    '06030': (57.097828920285934, 9.844028398663596),
    '06034': (57.50916610834763, 10.218220183567224),
    '06041': (57.72696814935525, 10.64159599892833),
    '06043': (57.41017573960198, 10.514625319731493),
    '06049': (56.55953487990565, 10.093814704806032),
    '06052': (56.7096055327481, 8.206874045672516),
    '06058': (55.998112896386125, 8.136161304153303),
    '06060': (56.28918752786709, 9.127528916951665),
    '06065': (56.75499845850469, 9.510547977914023),
    '06070': (56.31413085591719, 10.614046496946118),
    '06071': (56.44904735505679, 10.95116213511235),
    '06073': (56.08861396222594, 10.503180981385757),
    '06074': (56.088049868272236, 10.13563677426968),
    '06079': (56.691580985745084, 11.551359736720022),
    '06080': (55.53369576142447, 8.562991487571828),
    '06081': (55.56147559737792, 8.084393206267213),
    '06089': (55.50235745546692, 8.38621300980649),
    '06096': (55.184640109968214, 8.544191425164422),
    '06104': (55.743884733684, 9.180439873078921),
    '06108': (55.44259386769113, 9.340800815925185),
    '06110': (55.228988451238266, 9.277943803375612),
    '06111': (55.30098032576936, 9.799477297576406),
    '06118': (54.96502257287729, 9.808211625879041),
    '06119': (54.84601402207899, 9.980157637248073),
    '06120': (55.47960974470112, 10.34184832695028),
    '06124': (55.01436313393742, 10.577662215094268),
    '06135': (55.315737411737594, 11.37931988048978),
    '06141': (54.819177144293675, 11.327482941889082),
    '06147': (54.88306179867862, 12.17348446729029),
    '06149': (54.56062970291269, 11.975166865945694),
    '06151': (55.16733475028155, 11.14131538850832),
    '06154': (55.20107766277809, 11.84675725785963),
    '06156': (55.710487794300825, 11.7129112375071),
    '06159': (55.75210136506135, 10.870436461831277),
    '06168': (56.114803101556696, 12.328217838783763),
    '06169': (56.00745706019388, 11.264986109824841),
    '06170': (55.58245261927481, 12.144460780304955),
    '06180': (55.607720607511894, 12.656760679071466),
    '06181': (55.759641545290656, 12.538882726500612),
    '06183': (55.540314238669254, 12.720024104174836),
    '06188': (55.8770156778927, 12.401521788251294),
    '06190': (55.0730395160242, 14.746176086510165),
    '06191': (55.32674896796971, 15.198503880301285),
    '06193': (55.294752576212446, 14.778257254775967)
}

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

#print(haversine(57.99775667226363, 15.977079989679511, 57.0963, 9.8505))

def find_closest_geolocations_to_stations_from_grib(coordinateset, latlons):
    closest_distance = float('inf')
    for lat, lon in latlons:
        distance = haversine(lat, lon, coordinateset[0], coordinateset[1])
        if distance < closest_distance:
            closest_distance = distance
            closest_lat = lat
            closest_lon = lon
    return closest_lat, closest_lon
