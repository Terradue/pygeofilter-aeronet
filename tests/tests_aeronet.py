# Copyright 2025 Terradue
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from pygeofilter.util import IdempotentDict
from pygeofilter.parsers.cql2_json import parse as json_parse
from pygeofilter_aeronet.evaluator import to_aeronet_api


class TestQueryAttributes(unittest.TestCase):
    def setUp(self):
        pass

    def test_site(self):
        cql2_filter = {
            "op": "and",
            "args": [{"op": "eq", "args": [{"property": "site"}, "Cart_Site"]}],
        }

        expected = "site=Cart_Site"
        current, _ = to_aeronet_api(cql2_filter)

        self.assertEqual(expected, current)

    def test_wrong_site(self):
        cql2_filter = {
            "op": "and",
            "args": [{"op": "eq", "args": [{"property": "site"}, "Wrong_Site"]}],
        }

        with self.assertRaises(AssertionError):
            to_aeronet_api(cql2_filter)

    def test_data_type(self):
        cql2_filter = {
            "op": "and",
            "args": [{"op": "eq", "args": [{"property": "data_type"}, "AOD10"]}],
        }

        expected = "AOD10=1"
        current, _ = to_aeronet_api(cql2_filter)

        self.assertEqual(expected, current)

    def test_site_datatype(self):
        cql2_filter = {
            "op": "and",
            "args": [
                {"op": "eq", "args": [{"property": "site"}, "Cart_Site"]},
                {"op": "eq", "args": [{"property": "data_type"}, "AOD10"]},
            ],
        }

        expected = "site=Cart_Site&AOD10=1"
        current, _ = to_aeronet_api(cql2_filter)

        self.assertEqual(expected, current)

    def test_format_csv(self):
        cql2_filter = {
            "op": "and",
            "args": [{"op": "eq", "args": [{"property": "format"}, "csv"]}],
        }

        expected = "if_no_html=1"
        current, _ = to_aeronet_api(cql2_filter)

        self.assertEqual(expected, current)

    def test_format_html(self):
        cql2_filter = {
            "op": "and",
            "args": [{"op": "eq", "args": [{"property": "format"}, "html"]}],
        }

        expected = "if_no_html=0"
        current, _ = to_aeronet_api(cql2_filter)

        self.assertEqual(expected, current)

    def test_bbox(self):
        cql2_filter = {
            "op": "s_intersects",
            "args": [
                {"property": "geometry"},
                {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [10.0, 45.0],
                            [12.0, 45.0],
                            [12.0, 47.0],
                            [10.0, 47.0],
                            [10.0, 45.0],
                        ]
                    ],
                },
            ],
        }

        expected = "lon1=10.0&lat1=45.0&lon2=12.0&lat2=47.0"
        current, _ = to_aeronet_api(cql2_filter)

        self.assertEqual(
            expected,
            current,
        )

    def test_date_interval(self):

        cql2_filter = {
            "op": "and",
            "args": [
                {
                    "op": "t_after",
                    "args": [
                        {"property": "time"},
                        {"timestamp": "2023-01-01T00:00:00Z"},
                    ],
                },
                {
                    "op": "t_before",
                    "args": [
                        {"property": "time"},
                        {"timestamp": "2023-01-31T23:59:59Z"},
                    ],
                },
            ],
        }

        expected = "year=2023&month=1&day=1&hour=0&year2=2023&month2=1&day2=31&hour2=23"
        current, _ = to_aeronet_api(cql2_filter)

        self.assertEqual(
            expected,
            current,
        )

    def test_real_case(self):
        cql2_filter = {
            "op": "and",
            "args": [
                {"op": "eq", "args": [{"property": "site"}, "Cart_Site"]},
                {"op": "eq", "args": [{"property": "data_type"}, "AOD10"]},
                {"op": "eq", "args": [{"property": "format"}, "csv"]},
                {
                    "op": "t_after",
                    "args": [
                        {"property": "time"},
                        {"timestamp": "2023-02-01T00:00:00Z"},
                    ],
                },
                {
                    "op": "t_before",
                    "args": [
                        {"property": "time"},
                        {"timestamp": "2023-02-28T23:59:59Z"},
                    ],
                },
            ],
        }

        expected = "site=Cart_Site&AOD10=1&if_no_html=1&year=2023&month=2&day=1&hour=0&year2=2023&month2=2&day2=28&hour2=23"
        current, _ = to_aeronet_api(cql2_filter)

        self.assertEqual(
            expected,
            current,
        )
