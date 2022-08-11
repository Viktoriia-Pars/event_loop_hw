import asyncio
from sqlalchemy import Integer, String, Column, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import config


engine = create_async_engine(config.PG_DSN_ALC, echo=True)
Base = declarative_base()


class Starhero(Base):

    __tablename__ = 'starhero'

    id = Column(Integer, primary_key=True)
    id_character = Column(Integer)
    birth_year = Column(String(60), default=None)
    eye_color = Column(String(60))
    films = Column(ARRAY(String), default=[])
    gender = Column(String(60))
    hair_color = Column(String(60))
    height = Column(String(60))
    homeworld = Column(ARRAY(String), default=[])
    mass = Column(String(60))
    name = Column(String(128), index=True)
    skin_color = Column(String(60))
    species = Column(ARRAY(String), default=[])
    starships = Column(ARRAY(String), default=[])
    vehicles = Column(ARRAY(String), default=[])

async def get_async_session(
    drop: bool = False, create: bool = False
):

    async with engine.begin() as conn:
        if drop:
            await conn.run_sync(Base.metadata.drop_all)
        if create:
            print(1)
            await conn.run_sync(Base.metadata.create_all)
    async_session_maker = sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )

    return async_session_maker


async def main():
    await get_async_session(True, True)


if __name__ == '__main__':
    asyncio.run(main())