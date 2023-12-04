from sqlalchemy import desc, distinct, insert, select, text

from operations import SpaceXOperations

def startup_starlink_operations():
    space_api = SpaceXOperations()
    #Returning this object outside the class gives me the freedom to specify which table I want to operate on
    SLTable = space_api.createStarlinkTable()
    print(SLTable.c.keys())
    space_api.instantiateDB()

    #Returning a sample of the starlink Json
    print(space_api.gatherStarlinkJsonData()[0:5])
    starlink_data = space_api.gatherStarlinkJsonData()

    #Inserting the JSON into the DB
    space_api.insertData(table=SLTable, insert_rows=starlink_data)

    return space_api, SLTable

def query_satellite_last_position(SpaceXClass, SpaceXTable, satellite_id, date_lower_bound, date_upper_bound):

    dict_result = {}

    s = select(SpaceXTable.c.longitude, SpaceXTable.c.latitude).where(
        SpaceXTable.c.satellite_id == satellite_id 
        and SpaceXTable.c.creation_date > date_lower_bound
        and SpaceXTable.c.creation_date < date_upper_bound).order_by(desc(SpaceXTable.c.creation_date)).limit(1)

    query_result = space_api.selectData(table=SLTable, select_statement = s)
    dict_result['longitude'] = query_result[0][0]
    dict_result['latitude'] = query_result[0][1]

    print(f'Last known position of satellite {satellite_id}: {dict_result}')

space_api, SLTable = startup_starlink_operations()

query_satellite_last_position(
    SpaceXClass=space_api,
    SpaceXTable=SLTable,
    satellite_id='60106f21e900d60006e32cc7',
    date_lower_bound='2018-01-01',
    date_upper_bound='2023-01-01')

query_satellite_last_position(
    SpaceXClass=space_api,
    SpaceXTable=SLTable,
    satellite_id='5eed770f096e590006985610',
    date_lower_bound='2018-01-01',
    date_upper_bound='2023-01-01')

#Dropping the table
space_api.dropData(table=SLTable)
