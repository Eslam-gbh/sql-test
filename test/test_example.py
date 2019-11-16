from .dbtest import (DbTest, dbconnect)

import os
from psycopg2.extras import (RealDictCursor, RealDictRow)

PATH_TO_SQL_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "sql"))


class TestExample(DbTest):
    @dbconnect
    def test_select_organizations(self, conn):
        self.load_fixtures(conn,
                           os.path.join(PATH_TO_SQL_DIR, "organizations.sql"))

        sql = """
        SELECT * FROM organizations;
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            organizations = cur.fetchall()

            assert len(organizations) == 7

    @dbconnect
    def test_count_the_number_of_subordinates(self, conn):
        self.load_fixtures(conn,
                           os.path.join(PATH_TO_SQL_DIR, "organizations.sql"))

        sql = """
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            actual = cur.fetchall()
            print(actual)
            assert len(actual) == 7
            assert actual == [
                RealDictRow(**{
                    "subordinates_count": 0,
                    "id": 1,
                }),
                RealDictRow(**{
                    "subordinates_count": 4,
                    "id": 2,
                }),
                RealDictRow(**{
                    "subordinates_count": 0,
                    "id": 3,
                }),
                RealDictRow(**{
                    "subordinates_count": 0,
                    "id": 4,
                }),
                RealDictRow(**{
                    "subordinates_count": 0,
                    "id": 5,
                }),
                RealDictRow(**{
                    "subordinates_count": 1,
                    "id": 6,
                }),
                RealDictRow(**{
                    "subordinates_count": 0,
                    "id": 7,
                })
            ]

    @dbconnect
    def test_calculate_center_of_each_segment(self, conn):
        self.load_fixtures(conn,
                           os.path.join(PATH_TO_SQL_DIR, "japan_segments.sql"))

        sql = """
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            actual = cur.fetchall()
            print(actual)
            assert len(actual) == 10
            assert actual == [
                RealDictRow(
                    **{
                        "id": "KAGOSHIMA_1",
                        "longitude": 130.642228315775,
                        "latitude": 30.7045454545455,
                    }),
                RealDictRow(
                    **{
                        "id": "KAGOSHIMA_2",
                        "longitude": 130.694183864916,
                        "latitude": 30.7045454545455,
                    }),
                RealDictRow(
                    **{
                        "id": "KAGOSHIMA_3",
                        "longitude": 130.746139414057,
                        "latitude": 30.7045454545455,
                    }),
                RealDictRow(
                    **{
                        "id": "KAGOSHIMA_4",
                        "longitude": 129.707028431231,
                        "latitude": 30.75,
                    }),
                RealDictRow(
                    **{
                        "id": "KAGOSHIMA_5",
                        "longitude": 129.758983980373,
                        "latitude": 30.75,
                    }),
                RealDictRow(
                    **{
                        "id": "KAGOSHIMA_6",
                        "longitude": 129.810939529514,
                        "latitude": 30.75,
                    }),
                RealDictRow(
                    **{
                        "id": "KAGOSHIMA_7",
                        "longitude": 129.862895078655,
                        "latitude": 30.75,
                    }),
                RealDictRow(
                    **{
                        "id": "KAGOSHIMA_8",
                        "longitude": 129.914850627797,
                        "latitude": 30.75,
                    }),
                RealDictRow(
                    **{
                        "id": "KAGOSHIMA_9",
                        "longitude": 129.966806176937,
                        "latitude": 30.75,
                    }),
                RealDictRow(
                    **{
                        "id": "KAGOSHIMA_10",
                        "longitude": 130.018761726079,
                        "latitude": 30.75,
                    })
            ]

    @dbconnect
    def test_segments_using_geojson_boundary(self, conn):
        self.load_fixtures(conn,
                           os.path.join(PATH_TO_SQL_DIR, "japan_segments.sql"))

        sql = """
        """
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            actual = cur.fetchall()
            print(actual)
            assert len(actual) == 3
            assert actual == [
                RealDictRow(**{
                    "id": "KAGOSHIMA_1",
                }),
                RealDictRow(**{
                    "id": "KAGOSHIMA_2",
                }),
                RealDictRow(**{
                    "id": "KAGOSHIMA_3",
                })
            ]
