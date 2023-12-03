import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy import select

class SpaceXApi:
    def __init__(self):
        #Loading DB parameters from .env file
        load_dotenv()
        #Starting the DB
        self.engine = create_engine(f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/postgres")
        self.metadata = MetaData()
        self.metadata.bind = self.engine

    def createStarlinkTable(self):
        self.SLTable = Table(
        'starlink_historical_data', self.metadata,
        Column('id', String),
        Column('satellite_id', String),
        Column('longitude', Integer),
        Column('latitude', Integer),
        timescaledb_hypertable={
            'creation_date': 'timestamp'
            }
        )

    def instantiateDB(self):
        self.metadata.create_all(self.engine)

    def selectData(self):
        stmt = select(self.SLTable)
        print(stmt)

    def insertDummyData(self):
        new_user = self.SLTable(id='1', satellite_id='2', longitude=1, latitude=2, creation_date='2020-10-13T04:16:08')
        session.add(new_user)
        session.commit()
        print(repr(self.metadata.tables['starlink_historical_data']))


space_api = SpaceXApi()
space_api.createStarlinkTable()
space_api.instantiateDB()
space_api.selectData()
