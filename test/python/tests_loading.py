#
#  Since: November 2022
#  Author: gvenzl
#  Name: tests_loading.py
#  Description: loading tests function
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
import csv2db
import functions as f
import os


def negative_load_file_with_insufficient_columns(db_type, user, password, database, table):
    """Negative test that tries to load a file with not enough columns.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name

    Returns
    -------
    int
       The return code from csv2db
    """
    return csv2db.run(
              ["load",
               "-f", "../resources/test_files/bad/201811-citibike-tripdata-not-enough-columns.csv",
               "-o", db_type, "-u", user, "-p", password, "-d", database, "-t", table,
               "--debug"
               ]
             )


def load_file_with_insufficient_columns_and_ignore_flag(db_type, user, password, database, table):
    """Test that tries to load a file with not enough columns using the ignore errors flag.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name

    Returns
    -------
    int
       The return code from csv2db
    """
    return csv2db.run(
              ["load",
               "-f", "../resources/test_files/bad/201811-citibike-tripdata-not-enough-columns.csv",
               "-o", db_type, "-u", user, "-p", password, "-d", database, "-t", table,
               "--ignore"
               ]
             )


def exit_code_DATABASE_ERROR(db_type, user, password, database, table):
    """Test that raises a database error due to invalid username.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name

    Returns
    -------
    int
       The return code from csv2db
    """
    return csv2db.run(
              ["load",
               "-o", db_type,
               "-f", "../resources/test_files/201811-citibike-tripdata.csv",
               "-u", "INVALIDUSER",
               "-p", password,
               "-d", database,
               "-t", table]
              )


def exit_code_DATA_LOADING_ERROR(db_type, user, password, database, table):
    """Test that raises a data loading error due to an invalid table name.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name

    Returns
    -------
    int
       The return code from csv2db
    """
    return csv2db.run(
              ["load",
               "-f", "../resources/test_files/201811-citibike-tripdata.csv",
               "-o", db_type, "-u", user, "-p", password, "-d", database,
               "-t", "DOES_NOT_EXIST",
               "--debug"
               ]
             )


def empty_file(db_type, user, password, database, table):
    """Test that tries to load an empty file.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name

    Returns
    -------
    int
       The return code from csv2db
    """
    return csv2db.run(
             ["load",
              "-f", "../resources/test_files/bad/201811-citibike-tripdata-empty.csv",
              "-o", db_type, "-u", user, "-p", password, "-d", database, "-t", table
              ]
            )


def load_file(db_type, user, password, database, table, file, separator=","):
    """Loads a file and returns the loaded records.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name
    file : str
        The file to load
    separator : str
        The separator to use

    Returns
    -------
    int
       The number of rows loaded.
    """
    csv2db.run(
        ["load",
         "-f", file, "-s", separator,
         "-o", db_type, "-u", user, "-p", password, "-d", database, "-t", table
         ]
    )
    conn = f.get_db_connection(f.DBType(db_type), user, password, "localhost", "1521", database)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM {0}".format(table))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    return count


def truncate_table_before_load(db_type, user, password, database, table, file, separator=","):
    """Truncates the table before loading a file and returning the loaded records.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name
    file : str
        The file to load
    separator : str
        The separator to use

    Returns
    -------
    int, int
       The number of rows loaded the first and second time.
    """
    params = ["load", "-f", file,
              "-o", db_type, "-u", user, "-p", password, "-d", database,
              "-t", table, "-s", separator, "--truncate"
              ]

    csv2db.run(params)

    conn = f.get_db_connection(f.DBType(db_type), user, password, "localhost", "1521", database)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM {0}".format(table))
    count1 = cur.fetchone()[0]
    cur.close()
    conn.close()

    # Reset global truncate parameter
    cfg.truncate_before_load = False

    csv2db.run(params)

    conn = f.get_db_connection(f.DBType(db_type), user, password, "localhost", "1521", database)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM {0}".format(table))
    count2 = cur.fetchone()[0]
    cur.close()
    conn.close()

    return count1, count2


def negative_truncate_table_before_load(db_type, user, password, database, table, file, separator=","):
    """Negative test to load the same file twice without truncating the table beforehand.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name
    file : str
        The file to load
    separator : str
        The separator to use

    Returns
    -------
    int, int
       The number of rows loaded the first and second time.
    """
    conn = f.get_db_connection(f.DBType(db_type), user, password, "localhost", "1521", database)
    f.truncate_table(f.DBType(db_type), conn, table)
    conn.close()

    count1 = load_file(db_type, user, password, database, table,
                       "../resources/test_files/201811-citibike-tripdata.csv"
                       )

    count2 = load_file(db_type, user, password, database, table,
                       "../resources/test_files/201811-citibike-tripdata.csv"
                       )

    return count1, count2


def load_file_with_return_code(db_type, user, password, database, table, file, separator=","):
    """Loads file and returns csv2db return code.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name
    file : str
        The file to load
    separator : str
        The separator to use

    Returns
    -------
    int
       The return code from csv2db.
    """
    return csv2db.run(
             ["load",
              "-f", file, "-s", separator,
              "-o", db_type, "-u", user, "-p", password, "-d", database, "-t", table
              ]
            )


def load_file_with_return_code_ignore(db_type, user, password, database, table, file, separator=","):
    """Loads file and returns csv2db return code and passes on the ignore flag.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name
    file : str
        The file to load
    separator : str
        The separator to use

    Returns
    -------
    int
       The rows loaded into the table.
    """
    csv2db.run(
        ["load",
         "-f", file, "-s", separator, "--ignore",
         "-o", db_type, "-u", user, "-p", password, "-d", database, "-t", table
         ]
    )

    conn = f.get_db_connection(f.DBType(db_type), user, password, "localhost", "1521", database)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM {0}".format(table))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    return count


def log_bad_rows(db_type, user, password, database, table, file):
    """Loads file, records bad rows and returns the bad row count.

    Parameters
    ----------
    db_type : str
        The database type to load into
    user : str
        The database username
    password : str
        The database user password
    database : str
        The database name
    table : str
        The table name
    file : str
        The file to load

    Returns
    -------
    int
       The bad rows found.
    """
    csv2db.run(
        ["load", "-f", file, "--ignore", "--log",
         "-o", db_type, "-u", user, "-p", password, "-d", database, "-t", table
         ]
    )

    bad_rows_found = 0
    with f.open_file(file + ".bad") as bad_file:
        bad_reader = f.get_csv_reader(bad_file)
        with f.open_file(file) as source_file:
            for bad_line in bad_reader:
                reader = f.get_csv_reader(source_file)
                for line in reader:
                    if bad_line == line:
                        bad_rows_found += 1
                        break

    os.remove(file + ".bad")
    return bad_rows_found
