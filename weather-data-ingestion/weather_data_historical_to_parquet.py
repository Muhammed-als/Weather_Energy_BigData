import pandas as pd
import os
import json
from zipfile import ZipFile

def convert_to_parquet(zip_file, output_file):
  """
  Extracts JSON data from nested zip files, converts it to a DataFrame,
  and writes it as a single Parquet file.

  Args:
      zip_file (str): Path to the nested zip file.
      output_file (str): Path to the output Parquet file.
  """
  data = []
  with ZipFile(zip_file, 'r') as zip_obj:
    for year_folder in zip_obj.namelist():
      if not year_folder.endswith('/'):  # Skip top-level directory
        for month_file in zip_obj.namelist():
          if month_file.startswith(year_folder) and month_file.endswith('.txt'):
            # Extract JSON lines from each month file
            with zip_obj.open(month_file) as f:
              lines = f.readlines()
              for line in lines:
                data.append(json.loads(line.decode()))

  # Convert JSON data to DataFrame and write to Parquet
  df = pd.DataFrame(data)
  df.to_parquet(output_file)

# Example usage
zip_file = './historical_data_files/all.zip'
output_file = './historical_data_files/historical_weather_data.parquet'
convert_to_parquet(zip_file, output_file)

print(f"Converted data from {zip_file} to Parquet file: {output_file}")