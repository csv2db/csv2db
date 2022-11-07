#
#  Since: November 2022
#  Author: gvenzl
#  Name: tests_loading_oracle.py
#  Description: loading tests for Oracle
#
#  Copyright 2022 Gerald Venzl
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

import config as cfg
import functions as f
import unittest
import tests_loading as t

p = {
    "db_type": "oracle",
    "user": "TEST",
    "pwd": "LetsTest1",
    "db_name": "TEST",
    "tab_stage": "STAGING",
    "tab_loc": "LOCATIONS"
}


class LoadingTestsOracleSuite(unittest.TestCase):

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
        conn = f.get_db_connection(f.DBType.ORACLE, p["user"], p["pwd"], "localhost", "1521", p["db_name"])
        f.truncate_table(f.DBType.ORACLE, conn, p["tab_stage"])
        f.truncate_table(f.DBType.ORACLE, conn, p["tab_loc"])
        conn.close()

    def test_negative_load_file_with_insufficient_columns(self):
        print("test_negative_Oracle_load_file_with_insufficient_columns")
        self.assertEqual(
            f.ExitCodes.DATA_LOADING_ERROR.value,
            t.negative_load_file_with_insufficient_columns(
                p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"]
            )
        )

    def test_load_file_with_insufficient_columns_and_ignore_flag(self):
        print("test_Oracle_load_file_with_insufficient_columns")
        self.assertEqual(
            f.ExitCodes.SUCCESS.value,
            t.load_file_with_insufficient_columns_and_ignore_flag(
                p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"]
            )
        )

    def test_exit_code_DATABASE_ERROR(self):
        print("test_Oracle_exit_code_DATABASE_ERROR")
        self.assertEqual(
            f.ExitCodes.DATABASE_ERROR.value,
            t.exit_code_DATABASE_ERROR(
                p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"]
            )
        )

    def test_exit_code_DATA_LOADING_ERROR(self):
        print("test_Oracle_exit_code_DATA_LOADING_ERROR")
        self.assertEqual(
            f.ExitCodes.DATA_LOADING_ERROR.value,
            t.exit_code_DATA_LOADING_ERROR(
                p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"]
            )
        )

    def test_empty_file(self):
        print("test_Oracle_empty_file")
        self.assertEqual(
            f.ExitCodes.SUCCESS.value,
            t.empty_file(
                p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"]
            )
        )

    def test_loading_Oracle(self):
        print("test_loading_Oracle")
        expected_rows = 16

        self.assertEqual(
            expected_rows,
            t.load_file(
                p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"],
                "../resources/test_files/201811-citibike-tripdata.csv"
            )
        )

    def test_unicode_file_Oracle(self):
        print("test_unicode_file_Oracle")
        expected_rows = 1000

        self.assertEqual(
            expected_rows,
            t.load_file(
                p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_loc"],
                "../resources/test_files/allCountries.1000.txt.gz", "\t"
            )
        )

    def test_truncate_table_before_load_Oracle(self):
        print("test_truncate_table_before_load_Oracle")
        count1, count2 = t.truncate_table_before_load(
            p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"],
            "../resources/test_files/201811-citibike-tripdata.csv*"
        )
        self.assertEqual(count1, count2)

    def test_negative_truncate_table_before_load_Oracle(self):
        print("test_negative_truncate_table_before_load_Oracle")
        count1, count2 = t.negative_truncate_table_before_load(
            p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"],
            "../resources/test_files/201811-citibike-tripdata.csv"
        )

        # Assert that double the amount of rows have been loaded (2 * first count == second count)
        self.assertEqual(count1*2, count2)

    def test_negative_load_invalid_file_type_Oracle(self):
        print("test_negative_load_invalid_file_type_Oracle")
        self.assertEqual(
            f.ExitCodes.GENERIC_ERROR.value,
            t.load_file_with_return_code(
                p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"],
                "../resources/test_files/bad/201811-citibike-tripdata-invalid.csv.zip"
            )
        )

    def test_negative_error_bad_data_Oracle(self):
        print("test_negative_error_bad_data_Oracle")
        self.assertEqual(
            f.ExitCodes.DATA_LOADING_ERROR.value,
            t.load_file_with_return_code(
                p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"],
                "../resources/test_files/bad/201811-citibike-tripdata-errors.csv"
            )
        )

    def test_ignore_bad_data(self):
        print("test_ignore_bad_data_Oracle")
        good_records = 7
        self.assertEqual(good_records,
                         t.load_file_with_return_code_ignore(
                             p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"],
                             "../resources/test_files/bad/201811-citibike-tripdata-errors.csv"
                            )
                         )

    def test_log_bad_rows(self):
        print("test_ignore_bad_data_Oracle")
        bad_rows = 3
        self.assertEqual(bad_rows,
                         t.log_bad_rows(
                             p["db_type"], p["user"], p["pwd"], p["db_name"], p["tab_stage"],
                             "../resources/test_files/bad/201811-citibike-tripdata-errors.csv"
                            )
                         )


if __name__ == '__main__':
    unittest.main(verbosity=2)
