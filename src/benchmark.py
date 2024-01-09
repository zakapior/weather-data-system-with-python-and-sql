'''
A set of functions to measure how long will it take to download
'''

import time
import asyncio
from concurrent import futures
from weather import get_cities, get_city_weather

cities = get_cities()
location = [(lat, lon) for _, _, lat, lon, _ in cities]

def sequential(locations):
    result = []
    for lat, lon in locations:
        result.append(get_city_weather(lat, lon))
    return len(result)

def threads(locations):
    result = []
    with futures.ThreadPoolExecutor() as executor:
        for lat, lon in locations:
            result.append(executor.submit(get_city_weather, lat, lon))
        return len(result)

def processes(locations):
    result = []
    with futures.ProcessPoolExecutor() as executor:
        for lat, lon in locations:
            result.append(executor.submit(get_city_weather, lat, lon))
        return len(result)

def coroutines(locations):
    loop = asyncio.get_event_loop()
    tasks = asyncio.gather(*[coro_get_city_weather(lat, lon) for lat, lon in locations])
    result = loop.run_until_complete(tasks)
    loop.close()
    return len(result)

async def coro_get_city_weather(lat, lon):
    await get_city_weather(lat, lon)
    return 1


def main(downloader):
    t0 = time.perf_counter()
    count = downloader(location)
    elapsed = time.perf_counter() - t0
    print(f'\n{count} downloads in {elapsed:.2f}s')

if __name__ == '__main__':
    main(processes)
