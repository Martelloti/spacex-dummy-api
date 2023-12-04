import json
import os
import urllib.request

from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Float, String
from sqlalchemy.dialects.mysql import TIMESTAMP

class BaseSpaceX:
    def __init__(self):
        #Loading DB parameters from .env file
        load_dotenv()
        #Starting the DB
        self.engine = create_engine(f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/postgres")
        self.metadata = MetaData()
        self.metadata.bind = self.engine

    def createStarlinkTable(self):
        SLTable = Table(
        'starlink_historical_data', 
        self.metadata,
        Column('id', String),
        Column('satellite_id', String),
        Column('latitude', Float, nullable=True),
        Column('longitude', Float, nullable=True),
        Column('creation_date', TIMESTAMP),
        timescaledb_hypertable={
            'time_column_name': 'creation_date'
            }
        )

        return SLTable

    def instantiateDB(self):
        self.metadata.create_all(self.engine)


    def gatherStarlinkJsonData(self):
        self.starlink_data = []
        row_counter = 1

        with urllib.request.urlopen("https://raw.githubusercontent.com/BlueOnionLabs/api-spacex-backend/master/starlink_historical_data.json") as url:
            data = json.load(url)
            
        for record in data:
            row_dict = {
                'id': row_counter, 
                'satellite_id': record['id'], 
                'latitude': record['latitude'],
                'longitude': record['longitude'], 
                'creation_date': record['spaceTrack']['CREATION_DATE']
                }
            self.starlink_data.append(row_dict)
            row_counter += 1

        return self.starlink_data

if __name__ == "__main__":
    space_api = BaseSpaceX()
    #Returning this object outside the class gives me the freedom to specify which table I want to operate on
    SLTable = space_api.createStarlinkTable()
    print(SLTable.c.keys())
    space_api.instantiateDB()
    print(space_api.gatherStarlinkJsonData()[0:5])