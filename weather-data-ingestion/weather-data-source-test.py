from datetime import datetime
import os
import json
from pprint import pprint

from dmi_open_data import DMIOpenDataClient, Parameter, ClimateDataParameter

client = DMIOpenDataClient(api_key='44509e99-cd08-4d5d-80f7-637beae711f1')

# Get observations from DMI station in given time period
observations = client.get_observations(
    from_time=datetime(2024, 7, 20),
    to_time=datetime(2024, 7, 24),
    limit=1000)