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
import os
import unittest
import csv2db

test_parameters = {
    "user": "test",
    "db2_user": "db2inst1",
    "password": "LetsTest1",
    "database": "test",
    "table_staging": "STAGING",
    "table_locations": "LOCATIONS"
}


class LoadingTestCaseSuite(unittest.TestCase):

    def setUp(self):
        # Set the defaults for all tests
        cfg.column_separator = ","
        cfg.quote_char = '"'
        cfg.data_loading_error = False
        cfg.ignore_errors = False
        cfg.log_bad_records = False
        cfg.debug = False
        cfg.truncate_before_load = False

    @classmethod
    def tearDownClass(cls):
        # Truncate tables
        for db in f.DBType:
            conn = helper_get_db_conn(db,
                                      test_parameters["db2_user"] if db is f.DBType.DB2
                                      else test_parameters["user"])
            f.truncate_table(db, conn, test_parameters["table_staging"])
            f.truncate_table(db, conn, test_parameters["table_locations"])
            conn.close()

    def test_negative_load_file_with_insufficient_columns(self):
        print("test_negative_load_file_with_insufficient_columns")
        self.assertEqual(f.ExitCodes.DATA_LOADING_ERROR.value,
                         csv2db.run(
                              ["load",
                               "-f", "../resources/test_files/bad/201811-citibike-tripdata-not-enough-columns.csv",
                               "-u", test_parameters["user"],
                               "-p", test_parameters["password"],
                               "-d", test_parameters["database"],
                               "-t", test_parameters["table_staging"],
                               "--debug"
                               ]
                             )
                         )

    def test_load_file_with_insufficient_columns(self):
        print("test_load_file_with_insufficient_columns")
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                              ["load",
                               "-f", "../resources/bad/201811-citibike-tripdata-not-enough-columns.csv",
                               "-u", test_parameters["user"],
                               "-p", test_parameters["password"],
                               "-d", test_parameters["database"],
                               "-t", test_parameters["table_staging"],
                               "--ignore"
                               ]
                             )
                         )

    def test_exit_code_DATABASE_ERROR(self):
        print("test_exit_code_DATABASE_ERROR")
        self.assertEqual(f.ExitCodes.DATABASE_ERROR.value,
                         csv2db.run(
                              ["load",
                               "-f", "../resources/test_files/201811-citibike-tripdata.csv",
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
                               "-f", "../resources/test_files/201811-citibike-tripdata.csv",
                               "-u", test_parameters["user"],
                               "-p", test_parameters["password"],
                               "-d", test_parameters["database"],
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
                              "-u", test_parameters["user"],
                              "-p", test_parameters["password"],
                              "-d", test_parameters["database"],
                              "-t", test_parameters["table_staging"]
                              ]
                            )
                         )

    def test_loading_MySQL(self):
        print("test_loading_MySQL")
        helper_load_data(self, f.DBType.MYSQL, "../resources/test_files/201811-citibike-tripdata.csv")

    def test_loading_Postgres(self):
        print("test_loading_Postgres")
        helper_load_data(self, f.DBType.POSTGRES, "../resources/test_files/201811-citibike-tripdata.csv")

    def test_loading_Oracle(self):
        print("test_loading_Oracle")
        helper_load_data(self, f.DBType.ORACLE, "../resources/test_files/201811-citibike-tripdata.csv")

    def test_loading_SQLServer(self):
        print("test_loading_SQLServer")
        helper_load_data(self, f.DBType.SQLSERVER, "../resources/test_files/201811-citibike-tripdata.csv")

    def test_loading_Db2(self):
        print("test_loading_Db2")
        helper_load_data(self,
                         f.DBType.DB2,
                         "../resources/test_files/201811-citibike-tripdata.csv",
                         username=test_parameters["db2_user"])

    def test_unicode_file_Oracle(self):
        print("test_unicode_file_Oracle")
        helper_load_data(self,
                         f.DBType.ORACLE,
                         "../resources/test_files/allCountries.1000.txt.gz",
                         table=test_parameters["table_locations"],
                         separator="\t")

    def test_unicode_file_MySQL(self):
        print("test_unicode_file_MySQL")
        helper_load_data(self,
                         f.DBType.MYSQL,
                         "../resources/test_files/allCountries.1000.txt.gz",
                         table=test_parameters["table_locations"],
                         separator="\t")

    def test_unicode_file_Postgres(self):
        print("test_unicode_file_Postgres")
        helper_load_data(self,
                         f.DBType.POSTGRES,
                         "../resources/test_files/allCountries.1000.txt.gz",
                         table=test_parameters["table_locations"],
                         separator="\t")

    def test_unicode_file_SQLServer(self):
        print("test_unicode_file_SQLServer")
        helper_load_data(self,
                         f.DBType.SQLSERVER,
                         "../resources/test_files/allCountries.1000.txt.gz",
                         table=test_parameters["table_locations"],
                         separator="\t")

    def test_unicode_file_Db2(self):
        print("test_unicode_file_Db2")
        helper_load_data(self,
                         f.DBType.DB2,
                         "../resources/test_files/allCountries.1000.txt.gz",
                         table=test_parameters["table_locations"],
                         separator="\t",
                         username=test_parameters["db2_user"])

    def test_truncate_table_before_load_Oracle(self):
        print("test_truncate_table_before_load_Oracle")
        helper_truncate_table_before_load(self, f.DBType.ORACLE)

    def test_truncate_table_before_load_MySQL(self):
        print("test_truncate_table_before_load_MySQL")
        helper_truncate_table_before_load(self, f.DBType.MYSQL)

    def test_truncate_table_before_load_Postgres(self):
        print("test_truncate_table_before_load_Postgres")
        helper_truncate_table_before_load(self, f.DBType.POSTGRES)

    def test_truncate_table_before_load_SQLServer(self):
        print("test_truncate_table_before_load_SQLServer")
        helper_truncate_table_before_load(self, f.DBType.SQLSERVER)

    def test_truncate_table_before_load_Db2(self):
        print("test_truncate_table_before_load_Db2")
        helper_truncate_table_before_load(self, f.DBType.DB2, test_parameters["db2_user"])

    def test_negative_truncate_table_before_load_Oracle(self):
        print("test_negative_truncate_table_before_load_Oracle")
        helper_negative_truncate_table_before_load(self, f.DBType.ORACLE)

    def test_negative_truncate_table_before_load_MySQL(self):
        print("test_negative_truncate_table_before_load_MySQL")
        helper_negative_truncate_table_before_load(self, f.DBType.MYSQL)

    def test_negative_truncate_table_before_load_Postgres(self):
        print("test_negative_truncate_table_before_load_Postgres")
        helper_negative_truncate_table_before_load(self, f.DBType.POSTGRES)

    def test_negative_truncate_table_before_load_SQLServer(self):
        print("test_negative_truncate_table_before_load_SQLServer")
        helper_negative_truncate_table_before_load(self, f.DBType.SQLSERVER)

    def test_negative_truncate_table_before_load_Db2(self):
        print("test_negative_truncate_table_before_load_Db2")
        helper_negative_truncate_table_before_load(self, f.DBType.DB2, test_parameters["db2_user"])

    def test_negative_load_invalid_file_type(self):
        print("test_negative_load_invalid_file_type")
        self.assertEqual(f.ExitCodes.GENERIC_ERROR.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/test_files/bad/201811-citibike-tripdata-invalid.csv.zip",
                              "-u", test_parameters["user"],
                              "-p", test_parameters["password"],
                              "-d", test_parameters["database"],
                              "-t", test_parameters["table_staging"],
                              "--debug"
                              ])
                         )

    def test_negative_error_bad_data(self):
        print("test_negative_error_bad_data")
        self.assertEqual(f.ExitCodes.DATA_LOADING_ERROR.value,
                         csv2db.run(
                             ["load",
                              "-f", "../resources/test_files/bad/201811-citibike-tripdata-errors.csv",
                              "-u", test_parameters["user"],
                              "-p", test_parameters["password"],
                              "-d", test_parameters["database"],
                              "-t", test_parameters["table_staging"]
                              ])
                         )

    def test_ignore_bad_data(self):
        print("test_ignore_bad_data")
        good_records = 7
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-o", f.DBType.MYSQL.value,
                              "-f", "../resources/test_files/bad/201811-citibike-tripdata-errors.csv",
                              "-u", test_parameters["user"],
                              "-p", test_parameters["password"],
                              "-d", test_parameters["database"],
                              "-t", test_parameters["table_staging"],
                              "--ignore",
                              "--debug"
                              ])
                         )
        self.assertEqual(good_records,
                         helper_get_count_from_table(
                                                     f.DBType.MYSQL,
                                                     test_parameters["user"],
                                                     test_parameters["table_staging"])
                         )

    def test_log_bad_rows(self):
        print("test_ignore_bad_data")
        bad_rows = 3
        bad_rows_found = 0
        self.assertEqual(f.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-o", f.DBType.MYSQL.value,
                              "-f", "../resources/test_files/bad/201811-citibike-tripdata-errors.csv",
                              "-u", test_parameters["user"],
                              "-p", test_parameters["password"],
                              "-d", test_parameters["database"],
                              "-t", test_parameters["table_staging"],
                              "--log",
                              "--debug"
                              ])
                         )

        with f.open_file("../resources/test_files/bad/201811-citibike-tripdata-errors.csv.bad") as bad_file:
            bad_reader = f.get_csv_reader(bad_file)
            for bad_line in bad_reader:
                with f.open_file("../resources/test_files/bad/201811-citibike-tripdata-errors.csv") as file:
                    reader = f.get_csv_reader(file)
                    for line in reader:
                        if bad_line == line:
                            bad_rows_found += 1
                            break

        self.assertEqual(bad_rows, bad_rows_found)
        os.remove("../resources/test_files/bad/201811-citibike-tripdata-errors.csv.bad")


def helper_negative_truncate_table_before_load(self, db_type, username=test_parameters["user"]):
    conn = helper_get_db_conn(db_type, username)
    f.truncate_table(db_type, conn, test_parameters["table_staging"])
    conn.close()
    count1 = helper_load_data(self, db_type,
                              file="../resources/test_files/201811-citibike-tripdata.csv",
                              table=test_parameters["table_staging"],
                              username=username)
    count2 = helper_load_data(self, db_type,
                              file="../resources/test_files/201811-citibike-tripdata.csv",
                              table=test_parameters["table_staging"],
                              username=username)
    # Assert that double the amount of rows have been loaded (2 * first count == second count)
    self.assertEqual((count1*2), count2)


def helper_load_data(self, db_type, file,
                     table=test_parameters["table_staging"],
                     username=test_parameters["user"],
                     separator=","):
    self.assertEqual(f.ExitCodes.SUCCESS.value,
                     csv2db.run(
                         ["load",
                          "-f", file,
                          "-o", db_type.value,
                          "-u", username,
                          "-p", test_parameters["password"],
                          "-d", test_parameters["database"],
                          "-t", table,
                          "-s", separator
                          ]
                        )
                     )
    return helper_get_count_from_table(db_type, username, table)


def helper_truncate_table_before_load(self, db_type, username=test_parameters["user"]):
    params = ["load",
              "-f", "../resources/201811-citibike-tripdata.csv*",
              "-o", db_type.value,
              "-u", username,
              "-p", test_parameters["password"],
              "-d", test_parameters["database"],
              "-t", test_parameters["table_staging"],
              "--truncate"
              ]

    self.assertEqual(f.ExitCodes.SUCCESS.value, csv2db.run(params))
    # MySQL: a connection cannot hold the table still in cache
    # otherwise the TRUNCATE TABLE will hang (as it is a DROP/CREATE TABLE)
    count1 = helper_get_count_from_table(db_type, username, test_parameters["table_staging"])

    # Reset global truncate parameter
    cfg.truncate_before_load = False

    self.assertEqual(f.ExitCodes.SUCCESS.value, csv2db.run(params))
    count2 = helper_get_count_from_table(db_type, username, test_parameters["table_staging"])

    self.assertEqual(count1, count2)


def helper_get_count_from_table(db_type, username, table):
    conn = helper_get_db_conn(db_type, username)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM {0}".format(table))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    return count


def helper_get_db_conn(db_type, username):
    return f.get_db_connection(db_type,
                               username,
                               test_parameters["password"],
                               "localhost",
                               f.get_default_db_port(db_type),
                               test_parameters["database"])


if __name__ == '__main__':
    unittest.main()
