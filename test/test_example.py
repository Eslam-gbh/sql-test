from .dbtest import (DbTest, dbconnect)

import os
from psycopg2.extras import (RealDictCursor, RealDictRow)

PREDIFIENED_GEO_JSON = '{"SRID":4326,"type":"Polygon","coordinates":[[[130.27313232421875,30.519681272749402],[131.02020263671875,30.519681272749402],[131.02020263671875,30.80909017893796],[130.27313232421875,30.80909017893796],[130.27313232421875,30.519681272749402]]]}'  # noqa

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
            SELECT
               COUNT(t2.sales_organization_id) AS subordinates_count,
               t1.id
            FROM
               organizations AS t1
               LEFT OUTER JOIN
                  enterprise_sales_enterprise_customers AS t2
                  ON t1.id = t2.sales_organization_id
            GROUP BY
               t1.id
            ORDER BY
               t1.id;
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
            WITH centroids AS
            (
               SELECT
                  id,
                  ST_Centroid(bounds) as centroid
               from
                  japan_segments
            )

            SELECT
               id,
               ST_X(centroid) AS longitude,
               ST_Y(centroid) AS latitude
            FROM
               centroids;
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
        """
        bounds geos were fetched then processed by converting to Text
        and then back to geos because of not matching SRID between
        Stored Geos and Geo Json.

        you can replicate the error by applying ST_Within directly on
        Stored Geo and the Geo Json
        """
        self.load_fixtures(conn,
                           os.path.join(PATH_TO_SQL_DIR, "japan_segments.sql"))

        sql = f"""
            WITH segment_geo_texts AS
            (
               SELECT
                  id,
                  ST_AsText(bounds) AS seg_geo_text
               FROM
                  japan_segments
            )
            ,
            segment_geos AS
            (
               SELECT
                  id,
                  ST_GeomFromText(seg_geo_text) AS segment_geo
               FROM
                  segment_geo_texts
            )
            ,
            segment_buffered_polygons AS
            (
               SELECT
                  id,
                  ST_Buffer(ST_GeomFromGeoJSON('{PREDIFIENED_GEO_JSON}'), 20) AS geo_json_polygon,
                  ST_BUFFER(segment_geo, 20) AS segment_polygon
               FROM
                  segment_geos
            )
            ,
            results AS
            (
               SELECT
                     id,
                     ST_Within(segment_polygon, geo_json_polygon) As is_within_req_polygon
               FROM
                  segment_buffered_polygons
            )



            SELECT
               id
            FROM
               results
            WHERE
               is_within_req_polygon = 't';
        """ # noqa
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
