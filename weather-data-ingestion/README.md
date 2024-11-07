# Weather Data Injection

## Sources:
- [DMI Weather API](https://opendatadocs.dmi.govcloud.dk/DMIOpenData)
    - [Getting started](https://opendatadocs.dmi.govcloud.dk/Getting_Started)
    - [Authentication and retrieving API key](https://opendatadocs.dmi.govcloud.dk/Authentication)
    </br></br>

    - [Bulk data download](https://opendatadocs.dmi.govcloud.dk/Download) - For downloading historic data to HDFS

    - [Forecast_Data_STAC-API](https://opendatadocs.dmi.govcloud.dk/APIs/Forecast_Data_STAC-API) - 48 hours forecast. Files in [GRIB format](https://en.wikipedia.org/wiki/GRIB). 
        - EDR support a limited set of query options (time, area and parameter). More advanced filtering requires downloading and manual filtering.
        - GRIB format requires specialized tooling to parse: [Official tooling](https://confluence.ecmwf.int/display/ECC/GRIB+tools)
        - Example: [Download process and save forecast data](https://github.com/angelinkatula/Preprocessing-forecast-data-from-DMI/blob/main/Preprocessing%20DMI%20forecast%20data.ipynb)
        - Better for large queries

    - [Forecast_Data_EDR_API](https://opendatadocs.dmi.govcloud.dk/en/APIs/Forecast_Data_EDR_API) - 24 hours forecast. Files in [GeoJSON](https://geojson.org/) or [CoverageJSON](https://covjson.org/)
        -  Supports the following filtering options:
            - Querying a specific time range, f.ex. next 5 hours
            - Querying a position or bounded box area, f.ex. position of your house or the area of a specific town 
            - Querying a subset of parameters, f.ex. only wind speed and wind direction
        - Better for small queries

    - [Meteorological_Observation_API](https://opendatadocs.dmi.govcloud.dk/APIs/Meteorological_Observation_API) - To get live updates of actual meassured weather
</br></br>
- [Simple Python interface to the The Danish Meteorological Institute's (DMI) v2 Open Data API.](https://github.com/LasseRegin/dmi-open-data) - Supports: Meteorological or climate data


