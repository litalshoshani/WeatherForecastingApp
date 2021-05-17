from WeatherData import *
from MySqlHandler import *
from AppConfig import DB_NAME, TABLE_NAME
import sys
import pandas as pd

def create_db_and_data(argv):
    '''
    The method is responsible for the preparing the data.
    1. connects to the sql server
    2. creates the db (if not exists)
    3. creates the table (if not exists)
    4. imports the given csv files to the table.
    :param argv:
    :return:
    '''
    mysql_conn = MySqlConnector()
    # create the database (if not exists)
    mysql_conn.create_db(DB_NAME)
    # create the table (if not exists)
    create_table(TABLE_NAME, mysql_conn)
    # add the program csv files to the database's table
    for cvs_path in argv:
        import_csv_to_db(cvs_path, DB_NAME, TABLE_NAME, mysql_conn)


def import_csv_to_db(csv_path, db_name, table_name, connector):
    '''
    The method imports a given csv file path to the given table.
    The cvs file contains the next headers: longitude, latitude, forecast_time ,temperator, precipitation.
    :param csv_path:
    :param db_name:
    :param table_name:
    :param connector:
    :return:
    '''
    data = pd.read_csv(csv_path, index_col=False, delimiter=',')
    data.head()
    # get the csv columns
    data_columns = data.columns.array
    # check if the Precipitation value is in inches or millimiters
    precipitation_type = get_precipitation_type(data_columns[4])
    print("inserting " + csv_path + " to db")
    # insert each row of the cvs file to the db
    for i, row in data.iterrows():
        table_row = row
        precipitation_value = get_precipitation_value(precipitation_type, table_row[4])
        table_row[4] = precipitation_value
        table_row = tuple(table_row)
        # sql_insert = "INSERT INTO weather.forecast VALUES (%s,%s,%s,%s,%s)"
        connector.insert_row_to_db(db_name, table_name, table_row)
        print(i)


def create_table(table_name, conn):
    '''
    The method creates a new table in db, with the created sql command.
    :param table_name:
    :param conn:
    :return:
    '''
    # define the table fields: Longitude, Latitude, forecast time, Temperature, Precipitation
    longitude = "longitude FLOAT"
    latitude = "latitude FLOAT"
    forecast_time = "forecast_time VARCHAR(30)"
    temperature = "temperature FLOAT"
    precipitation = "precipitation FLOAT"
    # create the command
    sql_command = "CREATE TABLE IF NOT EXISTS " + table_name + "(" \
                  + longitude + "," + latitude + "," \
                  + forecast_time + "," + temperature + "," \
                  + precipitation + ");"
    # create the table with the given sql command
    conn.create_table_in_db(sql_command)

if __name__ == '__main__':
    args = sys.argv
    create_db_and_data(args[1:])



