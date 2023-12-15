#!/usr/bin/env python
# coding: utf-8

#import libraries
import pandas as pd
import requests
from pandas import json_normalize
from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset, Definitions


#passing weather API params
BASE_URL = "http://api.weatherapi.com/v1/current.json"
API_KEY = "212a0353103949f68ff83745231112"
q = "London"


@asset(group_name="weatherapi", compute_kind="Weather API")
def call_api(BASE_URL, API_KEY, q):
    """The functions calls a weather API to extract the current forecast"""
    resp = requests.get(f"{BASE_URL}?key={API_KEY}&q={q}")
    json_response = resp.json()       
    objects = json_normalize(json_response)
    #extracting only required columns
    objects = objects[["location.name", "location.region", "location.lat", "location.lon", 'current.precip_in', "current.humidity",
              "current.cloud", "current.feelslike_c", "current.feelslike_f",  "current.vis_km",  "current.vis_miles" , "current.uv",
              "current.gust_mph", "current.gust_kph"  ]]
    #renaming column names
    objects.columns = ["name", "region", "lat", "lon", "precip_in", "humidity", "cloud", "feelslike_c", "feelslike_f",
                       "vis_km", "vis_miles", "uv", "gust_mph", "gust_kph" ]
    return objects

"""  @asset(deps=[call_api], group_name="weatherapi", compute_kind="Database")
def save_data_to_db(duckdb: DuckDBResource) -> None:
    '''The function saves the output file as a duckdb'''
    df = call_api(BASE_URL, API_KEY, q)
    #creating a database connection and table
    sql = '''CREATE OR REPLACE TABLE curr_weather (name string, 
                                        region string, 
                                        lat string, 
                                        lon string, 
                                        precip_in numeric,
                                        humidity numeric, 
                                        cloud numeric, 
                                        feelslike_c numeric, 
                                        feelslike_f numeric,
                                        vis_km numeric, 
                                        vis_miles numeric, 
                                        uv numeric, 
                                        gust_mph numeric, 
                                        gust_kph numeric)'''

    with duckdb.get_connection() as conn:
        conn.execute(sql)
        conn.execute("INSERT INTO curr_weather SELECT * FROM df")
        print("Done")
  """

""" defs = Definitions(
    assets=[save_data_to_db],
    resources={
        "duckdb": DuckDBResource(
            database="weather_db.duckdb",  
        )
    },
) """

defs = Definitions(
    assets=[call_api],
    resources={"io_manager": DuckDBPandasIOManager(database="weather_db.duckdb")}
)