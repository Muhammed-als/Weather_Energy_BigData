import pandas as pd
import os
import json
from zipfile import ZipFile

# Download all.zip file link:
# https://dmigw.govcloud.dk/v2/metObs/bulk/?api-key=50b0870c-6f45-4dab-8269-43890d141299


STATIONS = {
	"06030": {"Egrid": "DK1", "coordinates": [57.0963, 9.8505], "name": "Aalborg"},
	"06034": {"Egrid": "DK1", "coordinates": [57.5044, 10.2227], "name": "Hjørring"},
	"06041": {"Egrid": "DK1", "coordinates": [57.7364, 10.6316], "name": "Frederikshavn"},
	"06043": {"Egrid": "DK1", "coordinates": [57.4064, 10.5157], "name": "Frederikshavn"},
	"06049": {"Egrid": "DK1", "coordinates": [56.5604, 10.0929], "name": "Mariagerfjord"},
	"06052": {"Egrid": "DK1", "coordinates": [56.7068, 8.215], "name": "Lemvig"},
	"06058": {"Egrid": "DK1", "coordinates": [56.0005, 8.1293], "name": "Ringkøbing-Skjern"},
	"06060": {"Egrid": "DK1", "coordinates": [56.2935, 9.1138], "name": "Herning"},
	"06065": {"Egrid": "DK1", "coordinates": [56.7558, 9.5067], "name": "Vesthimmerlands"},
	"06070": {"Egrid": "DK1", "coordinates": [56.3083, 10.6254], "name": "Syddjurs"},
	"06071": {"Egrid": "DK1", "coordinates": [56.4432, 10.9579], "name": "Norddjurs"},
	"06073": {"Egrid": "DK1", "coordinates": [56.0955, 10.5135], "name": "Aarhus"},
	"06074": {"Egrid": "DK1", "coordinates": [56.0803, 10.1353], "name": "Aarhus"},
	"06079": {"Egrid": "DK1", "coordinates": [56.7011, 11.5436], "name": "Norddjurs"},
	"06080": {"Egrid": "DK1", "coordinates": [55.5281, 8.5626], "name": "Esbjerg"},
	"06081": {"Egrid": "DK1", "coordinates": [55.5575, 8.0828], "name": "Fanø"},
	"06089": {"Egrid": "DK1", "coordinates": [55.4956, 8.398], "name": "Esbjerg"},
	"06096": {"Egrid": "DK1", "coordinates": [55.1908, 8.56], "name": "Fanø"},
	"06104": {"Egrid": "DK1", "coordinates": [55.7383, 9.1744], "name": "Billund"},
	"06108": {"Egrid": "DK1", "coordinates": [55.4376, 9.3338], "name": "Kolding"},
	"06110": {"Egrid": "DK1", "coordinates": [55.2251, 9.2634], "name": "Haderslev"},
	"06111": {"Egrid": "DK1", "coordinates": [55.2976, 9.7994], "name": "Assens"},
	"06118": {"Egrid": "DK1", "coordinates": [54.9616, 9.793], "name": "Sønderborg"},
	"06119": {"Egrid": "DK1", "coordinates": [54.8528, 9.988], "name": "Sønderborg"},
	"06120": {"Egrid": "DK1", "coordinates": [55.4735, 10.3297], "name": "Nordfyns"},
	"06124": {"Egrid": "DK1", "coordinates": [55.0144, 10.5693], "name": "Svendborg"},
	"06135": {"Egrid": "DK2", "coordinates": [55.3224, 11.3879], "name": "Slagelse"},
	"06141": {"Egrid": "DK2", "coordinates": [54.8275, 11.3292], "name": "Lolland"},
	"06147": {"Egrid": "DK2", "coordinates": [54.879, 12.1841], "name": "Vordingborg"},
	"06149": {"Egrid": "DK2", "coordinates": [54.5639, 11.964], "name": "Guldborgsund"},
	"06151": {"Egrid": "DK1", "coordinates": [55.1598, 11.1339], "name": "Nyborg"},
	"06154": {"Egrid": "DK2", "coordinates": [55.2075, 11.8605], "name": "Næstved"},
	"06156": {"Egrid": "DK2", "coordinates": [55.7154, 11.7088], "name": "Holbæk"},
	"06159": {"Egrid": "DK2", "coordinates": [55.7435, 10.8694], "name": "Kalundborg"},
	"06168": {"Egrid": "DK2", "coordinates": [56.1193, 12.3424], "name": "Gribskov"},
	"06169": {"Egrid": "DK2", "coordinates": [56.0067, 11.2805], "name": "Odsherred"},
	"06170": {"Egrid": "DK2", "coordinates": [55.5867, 12.1366], "name": "Solrød"},
	"06180": {"Egrid": "DK2", "coordinates": [55.614, 12.6455], "name": "Tårnby"},
	"06181": {"Egrid": "DK2", "coordinates": [55.7664, 12.5263], "name": "Lyngby-Taarbæk"},
	"06183": {"Egrid": "DK2", "coordinates": [55.5364, 12.7114], "name": "Dragør"},
	"06188": {"Egrid": "DK2", "coordinates": [55.8764, 12.4121], "name": "Allerød"},
	"06190": {"Egrid": "DK2", "coordinates": [55.0677, 14.7494], "name": "Bornholm"},
	"06191": {"Egrid": "DK2", "coordinates": [55.3218, 15.1875], "name": "Bornholm"},
	"06193": {"Egrid": "DK2", "coordinates": [55.2979, 14.7718], "name": "Bornholm"}
}

STATIONS_TIMES = {
	"06030": {"name": "Aalborg", "times": 0},
	"06034": {"name": "Hjørring", "times": 0},
	"06041": {"name": "Frederikshavn", "times": 0},
	"06043": {"name": "Frederikshavn", "times": 0},
	"06049": {"name": "Mariagerfjord", "times": 0},
	"06052": {"name": "Lemvig", "times": 0},
	"06058": {"name": "Ringkøbing-Skjern", "times": 0},
	"06060": {"name": "Herning", "times": 0},
	"06065": {"name": "Vesthimmerlands", "times": 0},
	"06070": {"name": "Syddjurs", "times": 0},
	"06071": {"name": "Norddjurs", "times": 0},
	"06073": {"name": "Aarhus", "times": 0},
	"06074": {"name": "Aarhus", "times": 0},
	"06079": {"name": "Norddjurs", "times": 0},
	"06080": {"name": "Esbjerg", "times": 0},
	"06081": {"name": "Fanø", "times": 0},
	"06089": {"name": "Esbjerg", "times": 0},
	"06096": {"name": "Fanø", "times": 0},
	"06104": {"name": "Billund", "times": 0},
	"06108": {"name": "Kolding", "times": 0},
	"06110": {"name": "Haderslev", "times": 0},
	"06111": {"name": "Assens", "times": 0},
	"06118": {"name": "Sønderborg", "times": 0},
	"06119": {"name": "Sønderborg", "times": 0},
	"06120": {"name": "Nordfyns", "times": 0},
	"06124": {"name": "Svendborg", "times": 0},
	"06135": {"name": "Slagelse", "times": 0},
	"06141": {"name": "Lolland", "times": 0},
	"06147": {"name": "Vordingborg", "times": 0},
	"06149": {"name": "Guldborgsund", "times": 0},
	"06151": {"name": "Nyborg", "times": 0},
	"06154": {"name": "Næstved", "times": 0},
	"06156": {"name": "Holbæk", "times": 0},
	"06159": {"name": "Kalundborg", "times": 0},
	"06168": {"name": "Gribskov", "times": 0},
	"06169": {"name": "Odsherred", "times": 0},
	"06170": {"name": "Solrød", "times": 0},
	"06180": {"name": "Tårnby", "times": 0},
	"06181": {"name": "Lyngby-Taarbæk", "times": 0},
	"06183": {"name": "Dragør", "times": 0},
	"06188": {"name": "Allerød", "times": 0},
	"06190": {"name": "Bornholm", "times": 0},
	"06191": {"name": "Bornholm", "times": 0},
	"06193": {"name": "Bornholm", "times": 0}
}

def convert_to_parquet_year(zip_file, output_file):
  """
  Extracts JSON data from nested zip files, converts it to a DataFrame,
  and writes it as a single Parquet file.

  Args:
      zip_file (str): Path to the nested zip file.
      output_file (str): Path to the output Parquet file.
  """
  print("\nSTARTING\n")
  data = []
  with ZipFile(zip_file, 'r') as zip_obj:
    for month_file in zip_obj.namelist():
      if month_file.endswith('.txt'):
        with zip_obj.open(month_file) as f:
          lines = f.readlines()
          for line in lines:
            line_json = json.loads(line.decode())

            if line_json['properties']['parameterId'] in ["temp_dry", "cloud_cover", "humidity", "wind_dir", "wind_speed"]:

              try:
                formated_json = {
                  "stationId": line_json['properties']['stationId'],
                  "stationName": STATIONS[line_json['properties']['stationId']]['name'],
                  "Egrid": STATIONS[line_json['properties']['stationId']]['Egrid'],
                  "parameterId": line_json['properties']['parameterId'],
                  "value": line_json['properties']['value'],
                  "observed": line_json['properties']['observed'],
                  "coordinates": line_json['geometry']['coordinates']
                }

                data.append(formated_json)
              except:
                pass
    
  # Writing to sample.json
  # with open("sample.json", "w", encoding='utf-8') as outfile:
  #     json.dump(data, outfile, ensure_ascii=False, indent=2)

  df_existing = pd.read_parquet(output_file)
  df = pd.DataFrame(data)

  df_combined = pd.concat([df_existing, df], ignore_index=True)

  df_combined.to_parquet(output_file)

def check_stations():
	histrocical_data = 'historical_data_files/historical_weather_data.parquet'
   
	df = pd.read_parquet(histrocical_data)
    
	json_data = df.to_json(orient='records')
   
	for line in json_data:
		print(line)
		STATIONS_TIMES[line['stationId']]['times'] += 1
    
		break
	

if __name__ == "__main__":
	check_stations()

	print(STATIONS_TIMES)

  # # Last one done 2024 - Done
  # # zip_file = 'historical_data_files/2024.zip'
  # # output_file = 'historical_data_files/historical_weather_data.parquet'
  # # convert_to_parquet_year(zip_file, output_file)
  # # print("\nDONE\n")
  # # print(f"Converted data from {zip_file} to Parquet file: {output_file}\n")
  # 
  # # print(pd.read_parquet(output_file))


#           stationId    stationName Egrid parameterId  value              observed         coordinates
# 0             06079      Norddjurs   DK1    wind_dir  275.0  2000-01-01T23:50:00Z  [11.5098, 56.7169]
# 1             06156         Holbæk   DK2  wind_speed    2.8  2000-01-01T23:50:00Z  [11.6035, 55.7358]
# 2             06168       Gribskov   DK2    wind_dir  290.0  2000-01-01T23:50:00Z  [12.3429, 56.1193]
# 3             06049  Mariagerfjord   DK1    humidity   99.0  2000-01-01T23:50:00Z  [10.0929, 56.5604]
# 4             06156         Holbæk   DK2    humidity   97.0  2000-01-01T23:50:00Z  [11.6035, 55.7358]
# ...             ...            ...   ...         ...    ...                   ...                 ...
# 212173265     06118     Sønderborg   DK1    temp_dry   -0.6  2024-11-22T00:00:00Z    [9.793, 54.9616]
# 212173266     06170         Solrød   DK2    temp_dry   -5.6  2024-11-22T00:00:00Z  [12.1366, 55.5867]
# 212173267     06159     Kalundborg   DK2    temp_dry    2.1  2024-11-22T00:00:00Z  [10.8694, 55.7435]
# 212173268     06147    Vordingborg   DK2    humidity   87.0  2024-11-22T00:00:00Z   [12.1841, 54.879]
# 212173269     06081           Fanø   DK1  wind_speed    6.4  2024-11-22T00:00:00Z   [8.0828, 55.5575]

# [212173270 rows x 7 columns]