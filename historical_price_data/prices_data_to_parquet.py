import pandas as pd
import json

el_json_path = "./Elspotprices.json"

# Open and read the JSON file
with open(el_json_path, 'r') as file:
    price_data = json.load(file)

final_price_data = []

print("\nSTARTING\n")

for price in price_data:
    try:
        if price['SpotPriceDKK'] != None:
            formated_json = {
                "PriceArea": price['PriceArea'],
                "SpotPriceDKK": price['SpotPriceDKK'],
                "observedDKK": price['HourDK'],
                "observedUTC": price['HourUTC']
            }

            final_price_data.append(formated_json)

    except:
        pass

output_file = './historical_el_prices_data.parquet'

df = pd.DataFrame(final_price_data)
df.to_parquet(output_file)

print(pd.read_parquet(output_file))

print("\nDONE\n")