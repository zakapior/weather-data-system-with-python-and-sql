'''
A set of analytical tools, that will be able to calculate various analytical information about
weather.
'''

from os import getenv
from dotenv import load_dotenv
import psycopg2
from weather import get_cities

load_dotenv()

def get_stats_for_city(city_name: str, period: str = 'today') -> tuple[str, float, float, float]:
    '''
    Gives some analytical data about city's temperatures.

    Arguments:
        city_name - a name of the city you want to check
        period - for which period you want to take the statistics. Valid values are:
            today (the default)
            yesterday
            current_week
            last_7_days

    Returns:
        a tuple with the following values for the defined timeframe:
            city name, maximum temperature, minimum temperature, temperature standard deviation
    '''

    conn = psycopg2.connect(database = getenv('DB_NAME'),
                        host = getenv('DB_HOSTNAME'),
                        user = getenv('DB_USERNAME'),
                        password = getenv('DB_PASSWORD'))

    try:
        cursor = conn.cursor()
        if period == 'today':
            cursor.execute("""
                        SELECT c.name, MAX(w.temperature), MIN(w.temperature),
                        STDDEV(w.temperature)
                        FROM weather w LEFT JOIN cities c
                        ON w.city_id = c.city_id
                        WHERE c.name = %s AND w.time::date = CURRENT_DATE
                        GROUP BY c.name;
                        """, (city_name, ))
        elif period == 'yesterday':
            cursor.execute("""
                        SELECT c.name, MAX(w.temperature), MIN(w.temperature),
                        STDDEV(w.temperature)
                        FROM weather w LEFT JOIN cities c
                        ON w.city_id = c.city_id
                        WHERE c.name = %s AND w.time::date = (CURRENT_DATE - INTERVAL '1 day')::date
                        GROUP BY c.name;
                        """, (city_name, ))
        elif period == 'current_week':
            cursor.execute("""
                        SELECT c.name, MAX(w.temperature), MIN(w.temperature),
                        STDDEV(w.temperature)
                        FROM weather w LEFT JOIN cities c
                        ON w.city_id = c.city_id
                        WHERE c.name = %s AND w.time::date >= date_trunc('week', current_date)
                        GROUP BY c.name;
                        """, (city_name, ))
        elif period == 'last_7_days':
            cursor.execute("""
                        SELECT c.name, MAX(w.temperature), MIN(w.temperature),
                        STDDEV(w.temperature)
                        FROM weather w LEFT JOIN cities c
                        ON w.city_id = c.city_id
                        WHERE c.name = %s AND w.time::date > (CURRENT_DATE - INTERVAL '7 days')::date
                        GROUP BY c.name;
                        """, (city_name, ))
        else:
            raise Exception("Invalid 'period' parameter. Valid ones are: today, yesterday,"
                            "current_week, last_7_days.")

        return cursor.fetchall()
    except Exception as error:
        print("Database error:", error)
    finally:
        cursor.close()
        conn.close()

def get_countries() -> tuple[str]:
    '''
    A helper function that finds the countries of all cities there are in the database.

    Returns:
        a tuple with country codes
    '''

    conn = psycopg2.connect(database = getenv('DB_NAME'),
                        host = getenv('DB_HOSTNAME'),
                        user = getenv('DB_USERNAME'),
                        password = getenv('DB_PASSWORD'))

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT country FROM cities;")

        return cursor.fetchall()
    except Exception as error:
        print("Database error:", error)
    finally:
        cursor.close()
        conn.close()

def get_stats_for_country(country_name: str, period: str = 'today') -> tuple[str, float, float, float]:
    '''
    Gives some analytical data about city's temperatures.

    Arguments:
        country_name - a name of the country you want to check
        period - for which period you want to take the statistics. Valid values are:
            today (the default)
            yesterday
            current_week
            last_7_days

    Returns:
        a tuple with the following values for the defined timeframe:
            country name, maximum temperature, minimum temperature, temperature standard deviation
    '''

    conn = psycopg2.connect(database = getenv('DB_NAME'),
                        host = getenv('DB_HOSTNAME'),
                        user = getenv('DB_USERNAME'),
                        password = getenv('DB_PASSWORD'))

    try:
        cursor = conn.cursor()
        if period == 'today':
            cursor.execute("""
                        SELECT c.country, MAX(w.temperature), MIN(w.temperature),
                        STDDEV(w.temperature)
                        FROM weather w LEFT JOIN cities c
                        ON w.city_id = c.city_id
                        WHERE c.country = %s AND w.time::date = CURRENT_DATE
                        GROUP BY c.country;
                        """, (country_name, ))
        elif period == 'yesterday':
            cursor.execute("""
                        SELECT c.country, MAX(w.temperature), MIN(w.temperature),
                        STDDEV(w.temperature)
                        FROM weather w LEFT JOIN cities c
                        ON w.city_id = c.city_id
                        WHERE c.country = %s AND w.time::date = (CURRENT_DATE - INTERVAL '1 day')::date
                        GROUP BY c.country;
                        """, (country_name, ))
        elif period == 'current_week':
            cursor.execute("""
                        SELECT c.country, MAX(w.temperature), MIN(w.temperature),
                        STDDEV(w.temperature)
                        FROM weather w LEFT JOIN cities c
                        ON w.city_id = c.city_id
                        WHERE c.country = %s AND w.time::date >= date_trunc('week', current_date)
                        GROUP BY c.country;
                        """, (country_name, ))
        elif period == 'last_7_days':
            cursor.execute("""
                        SELECT c.country, MAX(w.temperature), MIN(w.temperature),
                        STDDEV(w.temperature)
                        FROM weather w LEFT JOIN cities c
                        ON w.city_id = c.city_id
                        WHERE c.country = %s AND w.time::date > (CURRENT_DATE - INTERVAL '7 days')::date
                        GROUP BY c.country;
                        """, (country_name, ))
        else:
            raise Exception("Invalid 'period' parameter. Valid ones are: today, yesterday,"
                            "current_week, last_7_days.")

        return cursor.fetchall()
    except Exception as error:
        print("Database error:", error)
    finally:
        cursor.close()
        conn.close()

def get_hottest_cities():
    '''
    Gets the hottest cities in various timeframes

    Returns:
        a tuple with 3 elements, hourly, daily and weekly list of hottest cities with time
        indicating respective hottest measurement, the temperature and city name.

    Returns:
        none
    '''

    conn = psycopg2.connect(database = getenv('DB_NAME'),
                        host = getenv('DB_HOSTNAME'),
                        user = getenv('DB_USERNAME'),
                        password = getenv('DB_PASSWORD'))

    try:
        cursor = conn.cursor()
        cursor.execute("""
                       SELECT tmp.h, tmp.mt, c.name
                       FROM
                           (SELECT date_trunc('hour', time) h, max(temperature) mt
                           FROM weather
                           GROUP BY 1
                           ORDER BY 1 ASC) tmp
                       INNER JOIN weather w
                       ON tmp.h = date_trunc('hour', w.time) AND tmp.mt = w.temperature
                       INNER join cities c
                           ON c.city_id = w.city_id;
                       """)
        hourly = cursor.fetchall()
        
        cursor.execute("""
                       SELECT tmp.h, tmp.mt, c.name
                       FROM
                           (SELECT date_trunc('day', time) h, max(temperature) mt
                           FROM weather
                           GROUP BY 1
                           ORDER BY 1 ASC) tmp
                       INNER JOIN weather w
                       ON tmp.h = date_trunc('day', w.time) AND tmp.mt = w.temperature
                       INNER join cities c
                           ON c.city_id = w.city_id;
                       """)
        daily = cursor.fetchall()
        
        cursor.execute("""
                       SELECT tmp.h, tmp.mt, c.name
                       FROM
                           (SELECT date_trunc('week', time) h, max(temperature) mt
                           FROM weather
                           GROUP BY 1
                           ORDER BY 1 ASC) tmp
                       INNER JOIN weather w
                       ON tmp.h = date_trunc('week', w.time) AND tmp.mt = w.temperature
                       INNER join cities c
                           ON c.city_id = w.city_id;
                       """)
        weekly = cursor.fetchall()

        return hourly, daily, weekly
    except Exception as error:
        print("Database error:", error)
    finally:
        cursor.close()
        conn.close()

def get_coldest_cities():
    '''
    Gets the coldest cities in various timeframes

    Returns:
        a tuple with 3 elements, hourly, daily and weekly list of coldest cities with time
        indicating the measurement, the temperature and city name.
    '''

    conn = psycopg2.connect(database = getenv('DB_NAME'),
                        host = getenv('DB_HOSTNAME'),
                        user = getenv('DB_USERNAME'),
                        password = getenv('DB_PASSWORD'))

    try:
        cursor = conn.cursor()
        cursor.execute("""
                       SELECT tmp.h, tmp.mt, c.name
                       FROM
                           (SELECT date_trunc('hour', time) h, min(temperature) mt
                           FROM weather
                           GROUP BY 1
                           ORDER BY 1 ASC) tmp
                       INNER JOIN weather w
                       ON tmp.h = date_trunc('hour', w.time) AND tmp.mt = w.temperature
                       INNER join cities c
                           ON c.city_id = w.city_id;
                       """)
        hourly = cursor.fetchall()
        
        cursor.execute("""
                       SELECT tmp.h, tmp.mt, c.name
                       FROM
                           (SELECT date_trunc('day', time) h, min(temperature) mt
                           FROM weather
                           GROUP BY 1
                           ORDER BY 1 ASC) tmp
                       INNER JOIN weather w
                       ON tmp.h = date_trunc('day', w.time) AND tmp.mt = w.temperature
                       INNER join cities c
                           ON c.city_id = w.city_id;
                       """)
        daily = cursor.fetchall()
        
        cursor.execute("""
                       SELECT tmp.h, tmp.mt, c.name
                       FROM
                           (SELECT date_trunc('week', time) h, min(temperature) mt
                           FROM weather
                           GROUP BY 1
                           ORDER BY 1 ASC) tmp
                       INNER JOIN weather w
                       ON tmp.h = date_trunc('week', w.time) AND tmp.mt = w.temperature
                       INNER join cities c
                           ON c.city_id = w.city_id;
                       """)
        weekly = cursor.fetchall()

        return hourly, daily, weekly
    except Exception as error:
        print("Database error:", error)
    finally:
        cursor.close()
        conn.close()

def get_rainy_days() -> tuple[tuple[str, float]]:
    '''
    Get's the number of rainy hours in a city.

    Returns:
        a tuples with two elements:
            yesterday - a list of cities that were rainy yesterday and how many rain hours city had
            last_week - a list of cities that were rainy last week and how many rain hours city had

    '''

    conn = psycopg2.connect(database = getenv('DB_NAME'),
                        host = getenv('DB_HOSTNAME'),
                        user = getenv('DB_USERNAME'),
                        password = getenv('DB_PASSWORD'))

    try:
        cursor = conn.cursor()
        cursor.execute("""
                       SELECT c.name, count(*)
	                   FROM weather w INNER JOIN cities c
		                ON w.city_id = c.city_id
	                   WHERE w.description LIKE '%rain%'
	                   AND w.time::date < date_trunc('week', current_date)
	                   AND w.time::date >= date_trunc('week', (CURRENT_DATE - INTERVAL '1 week')::date)
	                   GROUP BY c.name;
                       """)
        last_week = cursor.fetchall()

        cursor.execute("""
                       SELECT c.name, count(*)
	                   FROM weather w INNER JOIN cities c
		                ON w.city_id = c.city_id
	                   WHERE w.description LIKE '%rain%'
	                   AND w.time::date = (CURRENT_DATE - INTERVAL '1 day')::date
	                   GROUP BY c.name;
                       """)
        yesterday = cursor.fetchall()

        return yesterday, last_week
    except Exception as error:
        print("Database error:", error)
    finally:
        cursor.close()
        conn.close()

# Uncomment to test
# if __name__ == "__main__":
    # This part will get statistics for cities in different timeframes
    # for city_id, name, lat, lon, _ in get_cities():
        # print(get_stats_for_city(name))
        # print(get_stats_for_city(name, 'today'))
        # print(get_stats_for_city(name, 'yesterday'))
        # print(get_stats_for_city(name, 'current_week'))
        # print(get_stats_for_city(name, 'last_7_days'))

    # This part will get statistics for countries in different timeframes
    # for country in get_countries():
    #     print(get_stats_for_country(country))
    #     print(get_stats_for_country(country, 'today'))
    #     print(get_stats_for_country(country, 'yesterday'))
    #     print(get_stats_for_country(country, 'current_week'))
    #     print(get_stats_for_country(country, 'last_7_days'))

    # This will give you the list of hottest cities in different timeframes
    # hourly, daily, weekly = get_hottest_cities()
    # print("Hourly:\n", hourly)
    # print("Daily:\n", daily)
    # print("Weekly:\n", weekly)

    # This will give you the list of coldest cities in different timeframes
    # hourly, daily, weekly = get_coldest_cities()
    # print("Hourly:\n", hourly)
    # print("Daily:\n", daily)
    # print("Weekly:\n", weekly)

    # This will give you the rainy hours for the rainy cities
    # yesterday, last_week = get_rainy_days()
    # print('Yesterday:\n', yesterday)
    # print('Last week:\n', last_week)
