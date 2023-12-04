import unittest
from main import SpaceXOperations, startup_starlink_operations, query_satellite_last_position, get_closest_satellite

class TestSpaceXOperations(unittest.TestCase):
    def setUp(self):
        self.spaceXOps = SpaceXOperations()
        self.space_api, self.SLTable = startup_starlink_operations()

    def test_query_satellite_last_position(self):
        result = query_satellite_last_position(
            SpaceXClass=self.space_api, 
            SpaceXTable=self.SLTable, 
            satellite_id='60106f21e900d60006e32cc7', 
            date_lower_bound='2018-01-01', 
            date_upper_bound='2023-01-01')

        self.assertEqual(result, {'latitude': 17.0, 'longitude': 1.7747363230340207})

        result = query_satellite_last_position(
            SpaceXClass=self.space_api, 
            SpaceXTable=self.SLTable, 
            satellite_id='5eed770f096e590006985610', 
            date_lower_bound='2018-01-01',
            date_upper_bound='2023-01-01')

        self.assertEqual(result, {'latitude': 109.0, 'longitude': 25.453949445394215})

    def test_query_satellite_last_position_wrong_date_format(self):
        with self.assertRaises(ValueError):
            query_satellite_last_position(
                SpaceXClass=self.space_api, 
                SpaceXTable=self.SLTable, 
                satellite_id='60106f21e900d60006e32cc7', 
                date_lower_bound='2018/01/01',  # wrong format
                date_upper_bound='2023-01-01')

    def test_query_satellite_last_position_wrong_date_range(self):
        with self.assertRaises(ValueError):
            query_satellite_last_position(
                SpaceXClass=self.space_api, 
                SpaceXTable=self.SLTable, 
                satellite_id='60106f21e900d60006e32cc7', 
                date_lower_bound='2023-01-01',  # wrong range
                date_upper_bound='2018-01-01')      

    def test_query_satellite_last_position_nonexistent_id(self):
        result = query_satellite_last_position(
            SpaceXClass=self.space_api, 
            SpaceXTable=self.SLTable, 
            satellite_id='nonexistent_id',  # non-existent id
            date_lower_bound='2018-01-01', 
            date_upper_bound='2023-01-01')

        self.assertIsNone(result)

    def test_query_satellite_last_position_no_dates(self):
        result = query_satellite_last_position(
            SpaceXClass=self.space_api, 
            SpaceXTable=self.SLTable, 
            satellite_id='60106f21e900d60006e32cc7')  # no dates

        self.assertEqual(result, {'latitude': 17.0, 'longitude': 1.7747363230340207})

    def test_get_closest_satellite_no_dates(self):
        result = get_closest_satellite(
            SpaceXClass=self.space_api, 
            SpaceXTable=self.SLTable, 
            latitude=-25.480877,
            longitude=-49.304424)

        self.assertEqual(result, '5eed7715096e590006985715')

if __name__ == '__main__':
    unittest.main()