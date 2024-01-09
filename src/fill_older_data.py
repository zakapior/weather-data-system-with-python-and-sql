'''
A helper tool, that will help me to fill the older entries, that I can't access with a free tier
on OpenWeatherMaps API.
'''

from os import getenv
from random import randrange
import datetime
from dotenv import load_dotenv
import psycopg2
from weather import get_cities, upload_city_weather_data_to_db
from analytics import get_stats_for_city

load_dotenv()

def get_descriptions() -> tuple[str]:
    '''
    A helper function that finds the weather descriptions there are in the database.

    Returns:
        a tuple with descriptions
    '''

    conn = psycopg2.connect(database = getenv('DB_NAME'),
                        host = getenv('DB_HOSTNAME'),
                        user = getenv('DB_USERNAME'),
                        password = getenv('DB_PASSWORD'))

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT description FROM weather;")

        return cursor.fetchall()
    except Exception as error:
        print("Database error:", error)
    finally:
        cursor.close()
        conn.close()

def get_first_measurement():
    '''
    Gets the date of the first measurement from the db.

    Returns:
        datetime object with the first measurement from the database
    '''

    conn = psycopg2.connect(database = getenv('DB_NAME'),
                        host = getenv('DB_HOSTNAME'),
                        user = getenv('DB_USERNAME'),
                        password = getenv('DB_PASSWORD'))

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT min(time) FROM weather;")

        return cursor.fetchall()
    except Exception as error:
        print("Database error:", error)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # 13:20
    current_datetime = get_first_measurement()[0][0] - datetime.timedelta(hours=1)
    six_weeks_ago = current_datetime - datetime.timedelta(weeks=6)

    cities = get_cities()
    descriptions = get_descriptions()

    while six_weeks_ago <= current_datetime:
        for city_id, city_name, _, _, _ in cities:
            _, max_temp, min_temp, std_dev = get_stats_for_city(city_name)[0]
            estimated_temp = (max_temp + min_temp)/2 + randrange(-300, 300)/100
            current_datetime = (current_datetime - datetime.timedelta(seconds=randrange(-120,120)))
            description = descriptions[randrange(0, len(descriptions))][0]
            upload_city_weather_data_to_db(city_id, current_datetime, estimated_temp, description)

        current_datetime = current_datetime - datetime.timedelta(hours=1)
