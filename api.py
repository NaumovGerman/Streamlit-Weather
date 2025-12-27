import requests
import aiohttp
import asyncio

API_URL = "http://api.openweathermap.org/data/2.5/weather"



def fetch_data_sync(city: str, key: str):
    response = requests.get(
        API_URL,
        params={"q": city, "appid": key, "units": "metric"}
    )
    return response.json()
    
async def fetch_data_async(city: str, key: str, session):
    async with session.get(
        API_URL,
        params={"q": city, "appid": key, "units": "metric"}
    ) as resp:
        return await resp.json()
    
async def fetch_many_async(cities: list[str], key: str):
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_data_async(city, key, session)
            for city in cities
        ]
        return await asyncio.gather(*tasks)