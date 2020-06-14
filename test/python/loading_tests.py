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
        # Set the defaults for all tests
        cfg.column_separator = ","
        cfg.quote_char = '"'
        cfg.data_loading_error = False
        cfg.debug = False
        cfg.truncate_before_load = False

    def test_loading_MySQL(self):
        print("test_loading_MySQL")
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
        print("test_loading_Postgres")
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
        print("test_loading_Oracle")
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
        print("test_loading_SqlServer")
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
        print("test_loading_Db2")
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
        print("test_unicode_file_Oracle")
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
        print("test_unicode_file_MySQL")
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
        print("test_unicode_file_Postgres")
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
        print("test_unicode_file_SqlServer")
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
        print("test_unicode_file_Db2")
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

    def test_truncate_table_before_load_Oracle(self):
        print("test_truncate_table_before_load_Oracle")
        truncate_table_before_load(self, f.DBType.ORACLE.value)

    def test_truncate_table_before_load_MySQL(self):
        print("test_truncate_table_before_load_MySQL")
        truncate_table_before_load(self, f.DBType.MYSQL.value)

    def test_truncate_table_before_load_Postgres(self):
        print("test_truncate_table_before_load_Postgres")
        truncate_table_before_load(self, f.DBType.POSTGRES.value)

    def test_truncate_table_before_load_SqlServer(self):
        print("test_truncate_table_before_load_SQLServer")
        truncate_table_before_load(self, f.DBType.SQLSERVER.value)

    def test_truncate_table_before_load_Db2(self):
        print("test_truncate_table_before_load_Db2")
        truncate_table_before_load(self, f.DBType.DB2.value, login["db2_user"])


def truncate_table_before_load(self, db_type, username=login["user"]):

    params = ["load",
              "-f", "../resources/201811-citibike-tripdata.csv*",
              "-o", db_type,
              "-u", username,
              "-p", login["password"],
              "-d", login["database"],
              "-t", login["table"],
              "--truncate"
              ]

    self.assertEqual(f.ExitCodes.SUCCESS.value, csv2db.run(params))
    # MySQL: a connection cannot hold the talbe still in cache
    # otherwise the TRUNCATE TABLE will hang (as it is a DROP/CREATE TABLE)
    count1 = get_count_from_table(db_type, username, login["table"])

    # Reset global truncate parameter
    cfg.truncate_before_load = False

    self.assertEqual(f.ExitCodes.SUCCESS.value, csv2db.run(params))
    count2 = get_count_from_table(db_type, username, login["table"])

    self.assertEqual(count1, count2)


def get_count_from_table(db_type, username, table):
    conn = f.get_db_connection(db_type,
                               username,
                               login["password"],
                               "localhost",
                               f.get_default_db_port(db_type),
                               login["database"])
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM {0}".format(login["table"]))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    return count


if __name__ == '__main__':
    unittest.main()
