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
import csv2db

login = {
    "user": "test",
    "db2_user": "db2inst1",
    "password": "LetsDocker1",
    "database": "test",
    "table": "STAGING"
}


class CSV2DBTestCase(unittest.TestCase):

    def setUp(self):
        # Set the default column separator for all tests
        cfg.column_separator = ","
        cfg.quote_char = '"'
        cfg.data_loading_error = False
        cfg.debug = False

    def test_open_csv_file(self):
        print("test_open_csv_file")
        with f.open_file("../resources/201811-citibike-tripdata.csv") as file:
            self.assertIsNotNone(file.read())

    def test_open_zip_file(self):
        print("test_open_zip_file")
        with f.open_file("../resources/201811-citibike-tripdata.csv.zip") as file:
            self.assertIsNotNone(file.read())

    def test_open_gzip_file(self):
        print("test_open_gzip_file")
        with f.open_file("../resources/201811-citibike-tripdata.csv.gz") as file:
            self.assertIsNotNone(file.read())

    def test_read_header(self):
        print("test_read_header")
        with f.open_file("../resources/201811-citibike-tripdata.csv.gz") as file:
            reader = f.get_csv_reader(file)
            expected = ["BIKEID", "BIRTH_YEAR", "END_STATION_ID", "END_STATION_LATITUDE",
                        "END_STATION_LONGITUDE", "END_STATION_NAME", "GENDER", "STARTTIME",
                        "START_STATION_ID", "START_STATION_LATITUDE", "START_STATION_LONGITUDE",
                        "START_STATION_NAME", "STOPTIME", "TRIPDURATION", "USERTYPE"]
            expected.sort()
            actual = f.read_header(reader)
            actual.sort()
            self.assertListEqual(expected, actual)

    def test_tab_separated_file(self):
        print("test_tab_separated_file")
        cfg.column_separator = "\t"
        with f.open_file("../resources/201812-citibike-tripdata.tsv") as file:
            reader = f.get_csv_reader(file)
            content = [f.read_header(reader)]
            for line in reader:
                content.append(line)
            self.assertEqual(11, len(content))

    def test_pipe_separated_file(self):
        print("test_pipe_separated_file")
        cfg.column_separator = "|"
        with f.open_file("../resources/201812-citibike-tripdata.psv") as file:
            reader = f.get_csv_reader(file)
            content = [f.read_header(reader)]
            for line in reader:
                content.append(line)
            self.assertEqual(11, len(content))

    def test_loading_MySQL(self):
        print("test_loading MySQL")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/201811-citibike-tripdata.csv",
                              "-o", "mysql",
                              "-u", login["user"],
                              "-p", login["password"],
                              "-d", login["database"],
                              "-t", login["table"]
                              ]
                            )
                         )

    def test_loading_Postgres(self):
        print("test_loading Postgres")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/201811-citibike-tripdata.csv",
                              "-o", "postgres",
                              "-u", login["user"],
                              "-p", login["password"],
                              "-d", login["database"],
                              "-t", login["table"]
                              ]
                            )
                         )

    def test_loading_Oracle(self):
        print("test_loading Oracle")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/201811-citibike-tripdata.csv",
                              "-o", "oracle",
                              "-u", login["user"],
                              "-p", login["password"],
                              "-d", login["database"],
                              "-t", login["table"]
                              ]
                            )
                         )

    def test_loading_SqlServer(self):
        print("test_loading SqlServer")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                              ["load",
                               "-f", "../resources/201811-citibike-tripdata.csv",
                               "-o", "sqlserver",
                               "-u", login["user"],
                               "-p", login["password"],
                               "-d", login["database"],
                               "-t", login["table"]
                               ]
                             )
                         )

    def test_loading_Db2(self):
        print("test_loading Db2")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                              ["load",
                               "-f", "../resources/201811-citibike-tripdata.csv",
                               "-o", "db2",
                               "-u", login["db2_user"],
                               "-p", login["password"],
                               "-d", login["database"],
                               "-t", login["table"]
                               ]
                             )
                         )

    def test_exit_code_SUCCESS(self):
        print("test_exit_code_SUCCESS")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(["gen", "-f", "../resources/201811-citibike-tripdata.csv.gz", "-t", "STAGING"]))

    def test_exit_code_GENERIC_ERROR(self):
        print("test_exit_code_GENERIC_ERROR")
        self.assertEqual(f.ExitCodes.GENERIC_ERROR.value,
                         csv2db.run(["gen", "-f", "../resources/bad/201811-citibike-tripdata-invalid.csv.zip"]))

    def test_exit_code_ARGUMENT_ERROR(self):
        print("test_exit_code_ARGUMENT_ERROR")
        # Test that command raises SystemExit exception
        with self.assertRaises(SystemExit) as cm:
            csv2db.run(["load", "-f", "../resources/201811-citibike-tripdata.csv.gz", "-t", "STAGING"])
        # Test that command threw SystemExit with status code 2
        self.assertEqual(cm.exception.code, 2)

    def test_load_file_with_insufficient_columns(self):
        print("test_load_file_with_insufficient_columns")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                              ["load",
                               "-f", "../resources/bad/201811-citibike-tripdata-not-enough-columns.csv",
                               "-u", login["user"],
                               "-p", login["password"],
                               "-d", login["database"],
                               "-t", login["table"],
                               "--debug"
                               ]
                             )
                         )

    def test_exit_code_DATABASE_ERROR(self):
        print("test_exit_code_DATABASE_ERROR")
        self.assertEqual(f.ExitCodes.DATABASE_ERROR.value,
                         csv2db.run(
                              ["load",
                               "-f", "../resources/201811-citibike-tripdata.csv",
                               "-u", "INVALIDUSER",
                               "-p", "test",
                               "-t", "STAGING"]
                              )
                         )

    def test_exit_code_DATA_LOADING_ERROR(self):
        print("test_exit_code_DATA_LOADING_ERROR")
        self.assertEqual(f.ExitCodes.DATA_LOADING_ERROR.value,
                         csv2db.run(
                              ["load",
                               "-f", "../resources/201811-citibike-tripdata.csv",
                               "-u", login["user"],
                               "-p", login["password"],
                               "-d", login["database"],
                               "-t", "DOES_NOT_EXIST"
                               ]
                             )
                         )

    def test_empty_file(self):
        print("test_empty_file")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/bad/201811-citibike-tripdata-empty.csv",
                              "-u", login["user"],
                              "-p", login["password"],
                              "-d", login["database"],
                              "-t", login["table"]
                              ]
                            )
                         )

    def test_unicode_file_Oracle(self):
        print("test_unicode_file")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/allCountries.1000.txt.gz",
                              "-u", login["user"],
                              "-p", login["password"],
                              "-d", login["database"],
                              "-t", "LOCATIONS",
                              "-s", "	"]
                             )
                         )

    def test_unicode_file_MySQL(self):
        print("test_unicode_file")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/allCountries.1000.txt.gz",
                              "-o", "mysql",
                              "-u", login["user"],
                              "-p", login["password"],
                              "-d", login["database"],
                              "-t", "LOCATIONS",
                              "-s", "	"]
                             )
                         )

    def test_unicode_file_Postgres(self):
        print("test_unicode_file")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/allCountries.1000.txt.gz",
                              "-o", "postgres",
                              "-u", login["user"],
                              "-p", login["password"],
                              "-d", login["database"],
                              "-t", "LOCATIONS",
                              "-s", "	"]
                             )
                         )

    def test_unicode_file_SqlServer(self):
        print("test_unicode_file")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/allCountries.1000.txt.gz",
                              "-o", "sqlserver",
                              "-u", login["user"],
                              "-p", login["password"],
                              "-d", login["database"],
                              "-t", "LOCATIONS",
                              "-s", "	"]
                             )
                         )

    def test_unicode_file_Db2(self):
        print("test_unicode_file")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/allCountries.1000.txt.gz",
                              "-o", "db2",
                              "-u", login["db2_user"],
                              "-p", login["password"],
                              "-d", login["database"],
                              "-t", "LOCATIONS",
                              "-s", "	"]
                             )
                         )


if __name__ == '__main__':
    unittest.main()
