import re
from datetime import datetime, timedelta

def get_next_model_run(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    dt += timedelta(hours=3)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def find_latest_date_string(strings):
    latest_date = None
    latest_string = None
    
    # Regular expression to match and extract both date parts in the format YYYY-MM-DDTHHMMSSZ
    date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}T\d{6}Z')

    for s in strings:
        # Find all dates in the string
        dates = date_pattern.findall(s)
        if len(dates) < 2:
            continue  # Skip if there aren't two dates in the expected format
        
        # Parse the second date
        second_date = datetime.strptime(dates[0], "%Y-%m-%dT%H%M%SZ")
        
        # Check if this is the latest second date
        if latest_date is None or second_date > latest_date:
            latest_date = second_date
            latest_string = s

    return latest_string

# Example usage
strings = [
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-17T030000Z.grib",
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-18T030000Z.grib",
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-18T040000Z.grib",
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-18T070000Z.grib",
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-18T060000Z.grib",
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-16T030000Z.grib"
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-16T060000Z.grib"
    "HARMONIE_DINI_SF_2024-11-14T150000Z_2024-11-16T050000Z.grib"
]

result = find_latest_date_string(strings)
print("String with the latest second date:", result)
