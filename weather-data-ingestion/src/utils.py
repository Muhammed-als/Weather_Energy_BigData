import re
from datetime import datetime, timedelta

def str_to_date(date_string) -> datetime:
    return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")

def get_current_model_run() -> str:
    current_datetime = datetime.now()
    rounded_datetime = current_datetime - timedelta(hours=(current_datetime.hour-1) % 3 +1, # When accounting for beeing one hour ahead: hours=(current_datetime.hour-1) % 3 +1
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