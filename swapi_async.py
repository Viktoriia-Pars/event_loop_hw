import aiohttp
import asyncio
import time
import more_itertools
import asyncpg
import config
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
import requests
from sqlalchemy.orm import sessionmaker
from create_table import Starhero

BASE_URL = 'https://swapi.dev/api/people/'

engine = create_async_engine(config.PG_DSN_ALC, echo=True)
Base = declarative_base()

all_people = int(requests.get(f'{BASE_URL}').json()['count'])

async def get_character(character_id, session):
    async with session.get(f'{BASE_URL}{character_id}') as response:
        json_data = await response.json()
        return character_id, json_data

async def insert_character(pool: asyncpg.Pool, id_character, character_json):
    query = 'INSERT INTO starhero (id_character, birth_year, eye_color, films,' \
            ' gender, hair_color, height, homeworld, mass, name, skin_color,' \
            ' species, starships, vehicles ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)'
    my_data = [(id_character,  character_json['birth_year'],character_json['eye_color'], character_json['films'],
                character_json['gender'], character_json['hair_color'], character_json['height'],
                list(character_json['homeworld']), character_json['mass'], character_json['name'], character_json['skin_color'],
                character_json['species'], character_json['starships'], character_json['vehicles'])]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, my_data)

async def main_2():
    async with aiohttp.ClientSession(trust_env=True) as session:
        tasks_list = []
        for i in range(1,all_people+1):
            task = asyncio.create_task(get_character(i, session))
            tasks_list.append(task)

        result = await asyncio.gather(*tasks_list)
        print(result[16][1]['detail'])
        pool = await asyncpg.create_pool(config.PG_DSN, min_size=20, max_size=20)
        tasks_list1 = []
        for i in result:
            if 'detail' in i[1]:
                pass
            else:
                tasks_list1.append(asyncio.create_task(insert_character(pool, i[0], i[1])))

        await asyncio.gather(*tasks_list1)
        await pool.close()
        print('ready')

if __name__ == '__main__':
    start = time.time()
    asyncio.run(main_2())
    print(time.time() - start)