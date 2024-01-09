'''
A helper tools to find a geographical location of cities and store them in a PostgreSQL database.
Those are intended to be run just once to populate the database. Cities populated in this step will
server as input for all the other tools.
'''

from os import getenv
from dotenv import load_dotenv
from requests import get
import psycopg2

load_dotenv()

def upload_city_location_to_db(city_name: str, latitude: float, longtitude: float, country: str) -> None:
    '''
    A function, that takes city name and location and stores it in the PostgreSQL database. It
    takes the connection details from .env file.

    Arguments:
        name - a city name (ex. Oslo), a string
        latitude - a geographical latitude
        longtitude - a geographical longtitude
        country - a country, that the city belongs to

    Returns:
        none
    '''
    conn = psycopg2.connect(database = getenv('DB_NAME'),
                            host = getenv('DB_HOSTNAME'),
                            user = getenv('DB_USERNAME'),
                            password = getenv('DB_PASSWORD'))
    cursor = conn.cursor()
    cursor.execute("INSERT INTO cities (name, latitude, longtitude, country) VALUES (%s, %s, %s, %s)",
                    (city_name, latitude, longtitude, country))
    conn.commit()
    cursor.close()
    conn.close()

def get_city_location(city_name: str) -> tuple[str, float, float]:
    '''
    Gets geographical position from OpenWeatherMap Geocoding API. Uses API key from .env file.

    Arguments:
        city_name - a name of the city that we want to know the location data from
    
    Returns:
        tuple with three arguments - city name, city latitude, city longtitude, country that the city is in
    '''
    response = get(f"http://api.openweathermap.org/geo/1.0/direct?q={city_name}&limit=1&"
                   f"appid={getenv('OPENWEATHER_API_KEY')}", timeout=10)
    return response.json()[0]['name'], response.json()[0]['lat'], response.json()[0]['lon'], response.json()[0]['country']

if __name__ == "__main__":
    # Tuple with city names, that we want to check and import to the database.
    cities_to_locate = ('Istanbul', 'London', 'Saint Petersburg', 'Berlin', 'Madrid', 'Kyiv',
                        'Rome', 'Bucharest', 'Paris', 'Minsk', 'Vienna', 'Warsaw', 'Hamburg',
                        'Budapest', 'Belgrade', 'Barcelona', 'Munich', 'Kharkiv', 'Milan')

    for city in cities_to_locate:
        city_name, city_lat, city_lon, country = get_city_location(city)
        upload_city_location_to_db(city_name, city_lat, city_lon, country)
