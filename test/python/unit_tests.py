#!/usr/bin/env python3
#
# Since: January, 2019
# Author: gvenzl
# Name: unit_tests.py
# Description: Unit tests file for csv2db
#
# Copyright 2019 Gerald Venzl
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
#

import functions as f
import config as cfg
import unittest


class CSV2DBTestCase(unittest.TestCase):

    def setUp(self):
        # Set the default column separator for all tests
        cfg.column_separator = ","

    def test_open_csv_file(self):
        with f.open_file("../resources/201811-citibike-tripdata.csv") as file:
            self.assertIsNotNone(file.read())

    def test_open_zip_file(self):
        with f.open_file("../resources/201811-citibike-tripdata.csv.zip") as file:
            self.assertIsNotNone(file.read())

    def test_open_gzip_file(self):
        with f.open_file("../resources/201811-citibike-tripdata.csv.gz") as file:
            self.assertIsNotNone(file.read())

    def test_raw_input_to_lit(self):
        with f.open_file("../resources/201811-citibike-tripdata.csv") as file:
            self.assertEqual(f.raw_input_to_list(file.readline()),
                             ["tripduration", "starttime", "stoptime", "start station id", "start station name",
                              "start station latitude", "start station longitude", "end station id",
                              "end station name", "end station latitude", "end station longitude",
                              "bikeid", "usertype", "birth year", "gender"])

    def test_read_header(self):
        with f.open_file("../resources/201811-citibike-tripdata.csv.gz") as file:
            self.assertEqual(f.read_header(file),
                             {"BIKEID", "BIRTH_YEAR", "END_STATION_ID", "END_STATION_LATITUDE",
                              "END_STATION_LONGITUDE", "END_STATION_NAME", "GENDER", "STARTTIME",
                              "START_STATION_ID", "START_STATION_LATITUDE", "START_STATION_LONGITUDE",
                              "START_STATION_NAME", "STOPTIME", "TRIPDURATION", "USERTYPE"})

    def test_tab_separated_file(self):
        cfg.column_separator = "\t"
        with f.open_file("../resources/201812-citibike-tripdata.tsv") as file:
            content = [f.raw_input_to_list(file.readline(), True)]
            for raw_line in file:
                content.append(f.raw_input_to_list(raw_line))
            self.assertEqual(len(content), 11)

    def test_pipe_separated_file(self):
        cfg.column_separator = "|"
        with f.open_file("../resources/201812-citibike-tripdata.psv") as file:
            content = [f.raw_input_to_list(file.readline(), True)]
            for raw_line in file:
                content.append(f.raw_input_to_list(raw_line))
            self.assertEqual(len(content), 11)


if __name__ == '__main__':
    unittest.main()
