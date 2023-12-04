from datetime import datetime
from functools import wraps
from haversine import haversine

from sqlalchemy import and_, desc, distinct, insert, select, text

from operations import SpaceXOperations

def startup_starlink_operations():
    """
    This function initializes the Starlink operations by gathering data and inserting it into the database.

    Returns:
        space_api (object): An instance of the SpaceX API class.
        SLTable (object): An instance of the Starlink Table class.
    """
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

def check_date_format(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        date_formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]
        date_lower_bound_dt = None
        date_upper_bound_dt = None
        for key, value in kwargs.items():
            if 'date' in key and value is not None:
                for format in date_formats:
                    try:
                        date_dt = datetime.strptime(value, format)
                        if 'lower' in key:
                            date_lower_bound_dt = date_dt
                        elif 'upper' in key:
                            date_upper_bound_dt = date_dt
                        break
                    except ValueError:
                        pass
                else:
                    raise ValueError(f"{key} must be in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format")
        if date_lower_bound_dt is not None and date_upper_bound_dt is not None and date_upper_bound_dt <= date_lower_bound_dt:
            raise ValueError("Upper bound date must be greater than lower bound date")
        return func(*args, **kwargs)
    return wrapper

@check_date_format
def query_satellite_last_position(
    SpaceXClass, 
    SpaceXTable, 
    satellite_id, 
    date_lower_bound = None, 
    date_upper_bound = None):
    """
    This function queries the last known position of a satellite within a given date range.

    Args:
        SpaceXClass (object): An instance of the SpaceX API class.
        SpaceXTable (object): An instance of the Starlink Table class.
        satellite_id (str): The ID of the satellite to query.
        date_lower_bound (str, optional): The lower bound of the date range to query. Must be in "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS" format. Defaults to None.
        date_upper_bound (str, optional): The upper bound of the date range to query. Must be in "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS" format. Defaults to None.

    Returns:
        dict_result (dict): A dictionary containing the last known longitude and latitude of the satellite.
    """
    dict_result = {}

    conditions = [SpaceXTable.c.satellite_id == satellite_id]

    if date_lower_bound is not None:
        conditions.append(SpaceXTable.c.creation_date > date_lower_bound)
    if date_upper_bound is not None:
        conditions.append(SpaceXTable.c.creation_date < date_upper_bound)

    s = select(SpaceXTable.c.longitude, SpaceXTable.c.latitude).where(
        and_(*conditions)).order_by(desc(SpaceXTable.c.creation_date)).limit(1)

    query_result = SpaceXClass.selectData(table=SpaceXTable, select_statement = s)

    if len(query_result) == 0:
        print(f'No data found for satellite {satellite_id} within the specified date range.')
        return None

    dict_result['latitude'] = query_result[0][0]
    dict_result['longitude'] = query_result[0][1]

    print(f'Last known position of satellite {satellite_id}: {dict_result}')

    return dict_result

@check_date_format
def get_closest_satellite(
    SpaceXClass, 
    SpaceXTable, 
    latitude,
    longitude,
    date_lower_bound = None, 
    date_upper_bound = None):
    """
    Fetches the closest satellite at a given time and position.

    Args:
        SpaceXClass (object): An instance of the SpaceX API class.
        SpaceXTable (object): An instance of the Starlink Table class.
        time (str): The time at which to fetch the satellite positions. Must be in "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS" format.
        latitude (float): The latitude of the position.
        longitude (float): The longitude of the position.

    Returns:
        str: The ID of the closest satellite.
    """

    conditions = [SpaceXTable.c.longitude != None, SpaceXTable.c.latitude != None]

    if date_lower_bound is not None:
        conditions.append(SpaceXTable.c.creation_date > date_lower_bound)
    if date_upper_bound is not None:
        conditions.append(SpaceXTable.c.creation_date < date_upper_bound)
    
    s = select(SpaceXTable.c.satellite_id, SpaceXTable.c.longitude, SpaceXTable.c.latitude).where(
    and_(*conditions))
    query_result = SpaceXClass.selectData(table=SpaceXTable, select_statement = s)

    closest_satellite = None
    min_distance = float('inf')

    print(latitude)
    print(longitude)

    # Calculate the distance from each satellite to the given position
    for satellite_id, sat_longitude, sat_latitude in query_result:
        distance = haversine((latitude, longitude), (sat_latitude, sat_longitude))
        if distance < min_distance:
            closest_satellite = satellite_id
            min_distance = distance

    print(f'Closest satellite to the given position: {closest_satellite}. Total distance: {round(min_distance)} km')

    return closest_satellite

if __name__ == "__main__":

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

    get_closest_satellite(
        SpaceXClass=space_api,
        SpaceXTable=SLTable,
        latitude=-25.480877,
        longitude=-49.304424,
        date_lower_bound='2018-01-01',
        date_upper_bound='2023-01-01')

    #Dropping the table
    space_api.dropData(table=SLTable)

