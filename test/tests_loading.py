#
#  Since: October 2023
#  Author: gvenzl
#  Name: tests_loading.py
#  Description:
#
#  Copyright 2023 Gerald Venzl
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#
#  Since: December 2022
#  Author: gvenzl
#  Name: tests_loading.py
#  Description: Loading tests with base class.
#
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import csv2db.constants as cons
import csv2db.functions as f
import csv2db.config as cfg
import os
import unittest
import main as csv2db


class LoadingTestsSuite(unittest.TestCase):

    params = {
        "db_type": "mysql",
        "user": "test",
        "password": "L3tsT3stTh1s++",
        "database": "test",
        "hostname": "localhost",
        "table_staging": "STAGING",
        "table_locations": "LOCATIONS"
    }

    def get_db_con(self):
        return f.get_db_connection(cons.DBType(self.params["db_type"]),
                                   self.params["user"],
                                   self.params["password"],
                                   self.params["hostname"],
                                   f.get_default_db_port(cons.DBType(self.params["db_type"])),
                                   self.params["database"])

    def table_count(self, table):
        conn = self.get_db_con()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) FROM {0}".format(table))
        count = cur.fetchone()[0]
        cur.close()
        conn.close()

        return count

    def load_data(self, file, table, separator=","):
        self.assertEqual(cons.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-f", file,
                              "-o", self.params["db_type"],
                              "-u", self.params["user"],
                              "-p", self.params["password"],
                              "-d", self.params["database"],
                              "-t", table,
                              "-s", separator,
                              "--debug"
                              ]
                            )
                         )
        return self.table_count(table)

    def load_with_truncated_table(self):
        params = ["load",
                  "-f", "resources/test_files/201811-citibike-tripdata.csv*",
                  "-o", self.params["db_type"],
                  "-u", self.params["user"],
                  "-p", self.params["password"],
                  "-d", self.params["database"],
                  "-t", self.params["table_staging"],
                  "--truncate"
                  ]

        self.assertEqual(cons.ExitCodes.SUCCESS.value, csv2db.run(params))
        # MySQL: a connection cannot hold the table still in cache
        # otherwise the TRUNCATE TABLE will hang (as it is a DROP/CREATE TABLE)
        count1 = self.table_count(self.params["table_staging"])

        # Reset global truncate parameter
        cfg.truncate_before_load = False

        self.assertEqual(cons.ExitCodes.SUCCESS.value, csv2db.run(params))
        count2 = self.table_count(self.params["table_staging"])

        self.assertEqual(count1, count2)

    def load_with_truncated_table_negative(self):
        conn = self.get_db_con()
        f.truncate_table(cons.DBType(self.params["db_type"]), conn, self.params["table_staging"])
        conn.close()
        count1 = self.load_data("resources/test_files/201811-citibike-tripdata.csv",
                                self.params["table_staging"])
        count2 = self.load_data("resources/test_files/201811-citibike-tripdata.csv",
                                self.params["table_staging"])
        # Assert that double the amount of rows has been loaded (2 * first count == second count)
        self.assertEqual((count1*2), count2)

    def setUp(self):
        # Set the defaults for all tests
        cfg.column_separator = ","
        cfg.quote_char = '"'
        cfg.data_loading_error = False
        cfg.ignore_errors = False
        cfg.log_bad_records = False
        cfg.debug = False
        cfg.truncate_before_load = False

    def tearDown(self):
        # Truncate tables
        conn = self.get_db_con()
        f.truncate_table(cons.DBType(self.params["db_type"]), conn, self.params["table_staging"])
        f.truncate_table(cons.DBType(self.params["db_type"]), conn, self.params["table_locations"])
        conn.close()

    def test_negative_load_file_with_insufficient_columns(self):
        print("test_negative_load_file_with_insufficient_columns")
        self.assertEqual(cons.ExitCodes.DATA_LOADING_ERROR.value,
                         csv2db.run(
                              ["load",
                               "-o", self.params["db_type"],
                               "-f", "resources/test_files/bad/201811-citibike-tripdata-not-enough-columns.csv",
                               "-u", self.params["user"],
                               "-p", self.params["password"],
                               "-d", self.params["database"],
                               "-t", self.params["table_staging"],
                               "--debug"
                               ]
                             )
                         )

    def test_load_file_with_insufficient_columns_and_ignore(self):
        print("test_load_file_with_insufficient_columns_and_ignore")
        self.assertEqual(cons.ExitCodes.SUCCESS.value,
                         csv2db.run(
                              ["load",
                               "-o", self.params["db_type"],
                               "-f", "resources/test_files/bad/201811-citibike-tripdata-not-enough-columns.csv",
                               "-u", self.params["user"],
                               "-p", self.params["password"],
                               "-d", self.params["database"],
                               "-t", self.params["table_staging"],
                               "--ignore"
                               ]
                             )
                         )

    def test_exit_code_DATABASE_ERROR(self):
        print("test_exit_code_DATABASE_ERROR")
        self.assertEqual(cons.ExitCodes.DATABASE_ERROR.value,
                         csv2db.run(
                              ["load",
                               "-o", self.params["db_type"],
                               "-f", "resources/test_files/201811-citibike-tripdata.csv",
                               "-u", "INVALIDUSER",
                               "-p", self.params["password"],
                               "-d", self.params["database"],
                               "-t", self.params["table_staging"]
                               ]
                              )
                         )

    def test_exit_code_DATA_LOADING_ERROR(self):
        print("test_exit_code_DATA_LOADING_ERROR")
        self.assertEqual(cons.ExitCodes.DATA_LOADING_ERROR.value,
                         csv2db.run(
                              ["load",
                               "-o", self.params["db_type"],
                               "-f", "resources/test_files/201811-citibike-tripdata.csv",
                               "-u", self.params["user"],
                               "-p", self.params["password"],
                               "-d", self.params["database"],
                               "-t", "DOES_NOT_EXIST",
                               "--debug"
                               ]
                             )
                         )

    def test_empty_file(self):
        print("test_empty_file")
        self.assertEqual(cons.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-o", self.params["db_type"],
                              "-f", "resources/test_files/bad/201811-citibike-tripdata-empty.csv",
                              "-u", self.params["user"],
                              "-p", self.params["password"],
                              "-d", self.params["database"],
                              "-t", self.params["table_staging"]
                              ]
                            )
                         )

    def test_loading(self):
        print("test_loading_" + self.params["db_type"])
        self.load_data("resources/test_files/201811-citibike-tripdata.csv", self.params["table_staging"])

    def test_unicode_file(self):
        print("test_unicode_file_" + self.params["db_type"])
        self.load_data("resources/test_files/allCountries.1000.txt.gz",
                       self.params["table_locations"], "\t")

    def test_truncate_table_before_load(self):
        print("test_truncate_table_before_load_" + self.params["db_type"])
        self.load_with_truncated_table()

    def test_negative_truncate_table_before_load(self):
        print("test_negative_truncate_table_before_load_" + self.params["db_type"])
        self.load_with_truncated_table_negative()

    def test_negative_load_invalid_file_type(self):
        print("test_negative_load_invalid_file_type")
        self.assertEqual(cons.ExitCodes.GENERIC_ERROR.value,
                         csv2db.run(
                             ["load",
                              "-o", self.params["db_type"],
                              "-f", "resources/test_files/bad/201811-citibike-tripdata-invalid.csv.zip",
                              "-u", self.params["user"],
                              "-p", self.params["password"],
                              "-d", self.params["database"],
                              "-t", self.params["table_staging"],
                              "--debug"
                              ])
                         )

    def test_negative_error_bad_data(self):
        print("test_negative_error_bad_data")
        self.assertEqual(cons.ExitCodes.DATA_LOADING_ERROR.value,
                         csv2db.run(
                             ["load",
                              "-o", self.params["db_type"],
                              "-f", "resources/test_files/bad/201811-citibike-tripdata-errors.csv",
                              "-u", self.params["user"],
                              "-p", self.params["password"],
                              "-d", self.params["database"],
                              "-t", self.params["table_staging"]
                              ])
                         )

    def test_ignore_bad_data(self):
        print("test_ignore_bad_data")
        good_records = 7
        self.assertEqual(cons.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-o", self.params["db_type"],
                              "-f", "resources/test_files/bad/201811-citibike-tripdata-errors.csv",
                              "-u", self.params["user"],
                              "-p", self.params["password"],
                              "-d", self.params["database"],
                              "-t", self.params["table_staging"],
                              "--ignore",
                              "--debug"
                              ])
                         )
        self.assertEqual(good_records,
                         self.table_count(self.params["table_staging"])
                         )

    def test_log_bad_rows(self):
        print("test_ignore_bad_data")
        bad_rows = 3
        bad_rows_found = 0
        self.assertEqual(cons.ExitCodes.SUCCESS.value,
                         csv2db.run(
                             ["load",
                              "-o", self.params["db_type"],
                              "-f", "resources/test_files/bad/201811-citibike-tripdata-errors.csv",
                              "-u", self.params["user"],
                              "-p", self.params["password"],
                              "-d", self.params["database"],
                              "-t", self.params["table_staging"],
                              "--log",
                              "--debug"
                              ])
                         )

        with f.open_file("resources/test_files/bad/201811-citibike-tripdata-errors.csv.bad") as bad_file:
            bad_reader = f.get_csv_reader(bad_file)
            for bad_line in bad_reader:
                with f.open_file("resources/test_files/bad/201811-citibike-tripdata-errors.csv") as file:
                    reader = f.get_csv_reader(file)
                    for line in reader:
                        if bad_line == line:
                            bad_rows_found += 1
                            break

        self.assertEqual(bad_rows, bad_rows_found)
        os.remove("resources/test_files/bad/201811-citibike-tripdata-errors.csv.bad")

    def test_load_utf_16_file(self):
        print("test_load_utf_16_file")
        self.assertEqual(cons.ExitCodes.SUCCESS.value,
                         csv2db.run(
                              ["load",
                               "-o", self.params["db_type"],
                               "-f", "resources/test_files/201811-citibike-tripdata-utf-16.csv",
                               "-u", self.params["user"],
                               "-p", self.params["password"],
                               "-d", self.params["database"],
                               "-t", self.params["table_staging"],
                               "--encoding", "utf-16"
                               ]
                             )
                         )

    def test_negative_load_utf_16_file(self):
        print("test_load_utf_16_file")
        self.assertEqual(cons.ExitCodes.DATA_LOADING_ERROR.value,
                         csv2db.run(
                              ["load",
                               "-o", self.params["db_type"],
                               "-f", "resources/test_files/201811-citibike-tripdata-utf-16.csv",
                               "-u", self.params["user"],
                               "-p", self.params["password"],
                               "-d", self.params["database"],
                               "-t", self.params["table_staging"]
                               ]
                             )
                         )


if __name__ == '__main__':
    unittest.main(verbosity=2)
