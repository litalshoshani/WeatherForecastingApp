from MySqlHandler import *
import pandas as pd
from AppConfig import DB_NAME, TABLE_NAME
#from app import mysql

def get_precipitation_type(precipitation_str):
    '''
    The method receives a string representation of the precipitation header,
    and returns if the values of the header's column are in millimeters or in inches (per hour).
    :param precipitation_str:
    :return:
    '''
    if "mm/hr" in precipitation_str:
        return "mm"
    if "in/hr" in precipitation_str:
        return "in"


def get_precipitation_value(precipitation_type, val):
    '''
    The method receives the type of the the precipitation value (millimeters or inches) and the value itself,
    and returns the value in millimeters.
    :param precipitation_type:
    :param val:
    :return:
    '''
    # if the type is millimeters, return it as is
    if precipitation_type == "mm":
        return val
    # if the type is inches, it should be converted to millimeters
    if precipitation_type == "in":
        # multiply the value by 25.4 to convert it to millimeters
        return val * 25.4


def get_weather_data_from_db(longitude, latitude, mysql):
    '''
    The method creates a sql query that gets from the app's db the forecastTime, Temperature and Precipitation values
    of the rows with the given longitude and latitude.
    The method returns an array of json formats. each json format contains the forecastTime, Temperature and Precipitation
    as keys, and the returned values from the query as values.
    :param longitude:
    :param latitude:
    :return:
    '''
    cur = mysql.connection.cursor()
    # create the sql query
    sql_command = "SELECT forecast_time as forecastTime, temperature as Temperature, precipitation as Precipitation " \
                  "FROM " + DB_NAME + "." + TABLE_NAME + " where longitude=" + str(longitude) + " and latitude=" + str(
        latitude) + ";"
    # execute the command
    cur.execute(sql_command)
    data = cur.fetchall()
    # define the headers (the json's keys)
    headers = ["forecastTime", "Temperature", "Precipitation"]
    json_data = []
    # create json with the returned values from the query and headers.
    for forecast in data:
        weather_data = dict(zip(headers, forecast))
        json_data.append(weather_data)
    return json_data


def get_weather_summarize_from_db(longitude, latitude, mysql):
    '''
    The method receives longitude and latitude values and returns a json with the max, min and avg of
    temperature and precipitation values from the rows with the given longitude and latitude.
    :param longitude:
    :param latitude:
    :return:
    '''
    # get the max temperature and precipitation
    max_query = get_temperature_precipitation("MAX", longitude, latitude, mysql)
    # get the min temperature and precipitation
    min_query = get_temperature_precipitation("MIN", longitude, latitude, mysql)
    # get the avg temperature and precipitation
    avg_query = get_temperature_precipitation("AVG", longitude, latitude, mysql)

    # define the results of the queries
    json_res = {
        'max': max_query,
        'min': min_query,
        'avg': avg_query
    }

    return json_res

def get_temperature_precipitation(query_command, longitude, latitude, mysql):
    '''
    The method receives a query command (of the format - "MAX"/"MIN"/"AVG"), creates a sql query
    according to the given query and according to the given longitude and latitude.
    The method returns a json of the temperature and precipitation values of the rows with the given
    longitude and latitude.
    :param query_command:
    :param longitude:
    :param latitude:
    :return:
    '''
    # create the query
    sql_command = "SELECT " + query_command + "(temperature) as Temperature, " + query_command + "(precipitation) as Precipitation FROM " + \
                  DB_NAME + "." + TABLE_NAME + " where longitude=" + str(longitude) + \
                  " and latitude=" + str(latitude) + ";"
    # define the headers (the json's keys)
    headers = ("Temperature", "Precipitation")
    # execute the command
    cur = mysql.connection.cursor()
    cur.execute(sql_command)
    # get the data from the query
    data = cur.fetchall()
    ''' explanation about the data:
    the data is a tuple of the format: (temperature,precipitation) meaning data[0] = (temperature,precipitation).
    in order to get the temperature we need to access the 0 index of data[0], 
    and to get the precipitation we need to access the 1 index of data[0]'''
    res = (data[0][0], data[0][1])
    # put the headers and the results in a dictionary
    query = dict(zip(headers, res))
    return query