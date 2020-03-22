#!/usr/bin/env python3
#
# Since: January, 2019
# Author: gvenzl
# Name: loading_tests.py
# Description: Loading unit tests for csv2db
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


class LoadingTestCaseSuite(unittest.TestCase):

    def setUp(self):
        # Set the default column separator for all tests
        cfg.column_separator = ","
        cfg.quote_char = '"'
        cfg.data_loading_error = False
        cfg.debug = False

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
                               "-t", "DOES_NOT_EXIST",
                               "--debug"
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
