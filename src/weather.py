'''
A set of tools, that will grab the list and location of cities in the database, look for the
current weather in OpenWeatherMap API and store it back in the database.
'''

import datetime
from os import getenv
from dotenv import load_dotenv
import psycopg2
from requests import get

load_dotenv()

def get_cities() -> list[tuple[str, float, float]]:
    '''
    This function get the city_id, it's latitude and longtitude from the cities table in the
    database and returns this list as a list of tuples.

    Returns:
        none
    '''
    conn = psycopg2.connect(database = getenv('DB_NAME'),
                        host = getenv('DB_HOSTNAME'),
                        user = getenv('DB_USERNAME'),
                        password = getenv('DB_PASSWORD'))

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT city_id, name, latitude, longtitude, country FROM cities;")

        return cursor.fetchall()
    except Exception as error:
        print("Database connection error:", error)
    finally:
        cursor.close()
        conn.close()


def get_city_weather(latitude: float, longtitude: float):
    '''
    Gets weather data for a place provided by geographical latitude and longtitude from
    OpenWeatherMap API.

    Arguments:
        latitude - a geographical latitude for the place you want to check current weather for
        longtitude - a geographical longtitude for the place you want to check current weather for
    
    Returns:
        A tuple with the following data:
            timestamp - a UNIX epoch timestamp, which states when was the weather data captured
            temp - a temperature in Celsius degrees
            description - a human-readable weather description
    '''

    try:
        response = get(f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}"
                    f"&lon={longtitude}&units=metric&appid={getenv('OPENWEATHER_API_KEY')}",
                    timeout=5)

        if response.status_code == 200:
            json_data = response.json()
        else:
            raise Exception('Response code from OpenWeatherAPI: ', response.status_code)

        return (datetime.datetime.fromtimestamp(json_data['dt']),
                json_data['main']['temp'],
                json_data['weather'][0]['description'])

    except Exception as error:
        print("OpenWeatherAPI connection error: ", error)

def upload_city_weather_data_to_db(city_id: int, timestamp: datetime.datetime, temperature: float,
                                   description: str) -> None:
    '''
    Stores the weather data provided to a weather table in the database.

    Arguments:
        city_id - a primary key of the city from the cities database that represents the city
        timestamp - a time, that the measurement was taken as reported by the OpenWeatherMap API
        temperature - a temperature in Celsius for the provided location
        description - brief weather description for the location provided

    Returns:
        none
    '''

    conn = psycopg2.connect(database = getenv('DB_NAME'),
                        host = getenv('DB_HOSTNAME'),
                        user = getenv('DB_USERNAME'),
                        password = getenv('DB_PASSWORD'))

    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO weather (city_id, time, temperature, description)"
                       "VALUES (%s, %s, %s, %s)", (city_id, timestamp, temperature, description))

        conn.commit()

    except Exception as error:
        print("Error while uploading weather data into the database:", error)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    for city_id, _, lat, lon, _ in get_cities():
        timestamp, temp, description = get_city_weather(lat, lon)
        upload_city_weather_data_to_db(city_id, timestamp, temp, description)
