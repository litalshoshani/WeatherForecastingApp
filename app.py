from flask import Flask, request, render_template
from flask import jsonify
from WeatherData import *
from flask_mysqldb import MySQL
from AppConfig import MYSQL_HOST, MYSQL_USER, MYSQL_PASS, DB_NAME, TABLE_NAME

app = Flask(__name__)

# update the app config with the mysql connection details
app.config['MYSQL_HOST'] = MYSQL_HOST
app.config['MYSQL_USER'] = MYSQL_USER
app.config['MYSQL_PASSWORD'] = MYSQL_PASS
app.config['MYSQL_DB'] = DB_NAME

# define the sql
mysql = MySQL(app)

@app.route('/')
def home():
    return render_template("home.html", content="Welcome To Weather App")

@app.route('/weather-data', methods=["GET", "POST"])
def weather_data():
    '''
    The method gets a request from the user (via form) with longitude and latitude values,
    and redirects to a page where there are all of the forecast times, temperatures and precipitiation
    of the given values.
    :return:
    '''
    print('/get-all-weather-data')
    if request.method == "POST":
        # get the longitude and latitude from the request
        req = request.form
        longitude = req["longitude"]
        latitude = req["latitude"]
        # get the weather data - an array with all the data from the rows in the db
        weather_data = get_weather_data_from_db(longitude, latitude, mysql)
        return render_template("weatherDataResults.html", weather_data=weather_data, longitude=longitude, latitude=latitude)
    return render_template("weatherDataTemplate.html", content="Weather Data")

@app.route('/weather-summary', methods=["GET", "POST"])
def weather_sum():
    '''
    The method gets a request from the user (via form) with longitude and latitude values,
    and redirects to a page where there are all of the max/min/avg values of temperatures and precipitiation
    of the given values.
    :return:
    '''
    print('/get-weather-data-summary')
    if request.method == "POST":
        # get the longitude and latitude from the request
        req = request.form
        longitude = req["longitude"]
        latitude = req["latitude"]
        # get the weather data - a json with all the min, max, and avg results
        weather_data = get_weather_summarize_from_db(longitude, latitude, mysql)
        return render_template("weatherSummarizeResults.html", weather_data=weather_data, longitude=longitude, latitude=latitude)
    return render_template("weatherSummarizeTemplate.html", content="Weather Summary")

@app.route('/weather/data', methods=["GET"])
def get_all_weather_data():
    '''
    The method receives from the request two parameters: longitude, latitude,
    and returns the data forecastTime, temperature and precipitation of all rows with
    the given longitude and latitude.
    :return:
    '''
    print('/get-all-weather-data')
    if request.method == "GET":
        # get the longitude and latitude from the request
        longitude = request.args.get('lon', '')
        latitude = request.args.get('lat', '')
        # get the weather data - an array with all the data from the rows in the db
        weather_data = get_weather_data_from_db(longitude, latitude, mysql)
        return jsonify(weather_data)

@app.route('/weather/summarize', methods=["GET"])
def get_weather_summarize():
    '''
    The method receives from the request two parameters: longitude, latitude,
    and returns the mav,min, and average of the temperature and precipitation of all rows with
    the given longitude and latitude.
    :return:
    '''
    print('/get-weather-data-summary')
    if request.method == "GET":
        # get the longitude and latitude from the request
        longitude = request.args.get('lon', '')
        latitude = request.args.get('lat', '')
        # get the weather data - a json with all the min, max, and avg results
        weather_data = get_weather_summarize_from_db(longitude, latitude, mysql)
        return jsonify(weather_data)

if __name__ == '__main__':
    app.run()