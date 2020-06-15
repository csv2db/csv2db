#!/usr/bin/env python3
#
# Since: January, 2019
# Author: gvenzl
# Name: functions.py
# Description: Common functions for csv2db
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

import datetime
import glob
import gzip
import os
import platform
import io
import zipfile
import sys
import traceback
from enum import Enum
import csv

import config as cfg


class DBType(Enum):
    """Database type enumeration."""
    ORACLE = "oracle"
    MYSQL = "mysql"
    POSTGRES = "postgres"
    SQLSERVER = "sqlserver"
    DB2 = "db2"


class ExitCodes(Enum):
    """Program return code enumeration."""
    SUCCESS = 0
    GENERIC_ERROR = 1
    DATABASE_ERROR = 3  # value 2 is reserved for wrong arguments passed via argparse
    DATA_LOADING_ERROR = 4


class TerminalColor(Enum):
    GREEN = "\x1b[32m"
    RED = "\x1b[31m"
    YELLOW = "\x1b[33m"
    RESET = "\x1b[0m"


def open_file(file):
    """Opens a CSV file.

    The file can either be in plain text (.csv), zipped (.csv.zip), or gzipped (.csv.gz)

    Parameters
    ----------
    file : str
        The file to open

    Returns
    -------
    file-object
        A file object
    """
    if file.endswith(".zip"):
        zip_file = zipfile.ZipFile(file, mode="r")
        file = zip_file.open(zip_file.infolist()[0], mode="r")
        return io.TextIOWrapper(file)
    elif file.endswith(".gz"):
        return gzip.open(file, mode="rt")
    else:
        return open(file, mode='r')


def read_header(reader):
    """Reads header and returns the column list.

    This function reads the first row of the CSV file and parses it for the column names.

    Parameters
    ----------
    reader : _csv.reader
        The CSV Reader object to read the header from

    Returns
    -------
    [str,]
        A list with all the column names.
    """
    header = []
    header.extend(col.replace(' ', '_',).upper() for col in next(reader))
    return header


def find_all_files(pattern):
    """Find all files of a given pattern.

    Parameters
    ----------
    pattern : str
        The pattern to search for

    Returns
    -------
    []
        List of files.
    """
    if os.path.isdir(pattern):
        # If path is directory find all CSV files, compressed or uncompressed
        pattern += "/*.csv*"
    return sorted(glob.glob(pattern))


def print_color(color, output):
    """Print colored output.

    If $NO_COLOR is set then no colored output will be printed.
    On Windows no colored output will be printed.
    
    Parameters
    ----------
    color : TerminalColor
        The color to be used.
    output : Any
        The output to be printed
    """
    if os.getenv('NO_COLOR') is None and platform.system() != "Windows":
        print(color.value, end='')
        print(output)
        print(TerminalColor.RESET.value, end='')
    else:
        print(output)


def verbose(output):
    """Print verbose output.

    Parameters
    ----------
    output : Any
        The output to print
    """
    if cfg.verbose:
        print(output)


def debug(output):
    """Print debug output.

    Parameters
    ----------
    output : Any
        The output to print
    """
    if cfg.debug:
        if isinstance(output, list):
            output = ", ".join(output)
        elif isinstance(output, dict):
            output = ", ".join(str(key) + ": " + str(value) for key, value in output.items())
        print_color(TerminalColor.YELLOW, "DEBUG: {0}: {1}".format(datetime.datetime.now(), output))


def error(output):
    """Print error output.

    Parameters
    ----------
    output : Any
        The output to be printed
    """
    print_color(TerminalColor.RED, output)


def get_exception_details():
    """Return usable exception string and its traceback as string.

    The string will be in the format "(Exception class name): (Exception message)"

    Returns
    -------
    (str, traceback)
        The string and traceback (as string) of the exception
    """
    exception_type, exception_message, tb = sys.exc_info()
    traceback_str = "Traceback:\n" + ''.join(traceback.format_tb(tb))
    return "{0}: {1}".format(exception_type.__name__, exception_message), traceback_str


def get_db_connection(db_type, user, password, host, port, db_name):
    """ Connects to the database.

    Parameters
    ----------
    db_type : str
        The database type
    user : str
        The database user
    password : str
        The database user password
    host : str
        The database host or ip address
    port : str
        The port to connect to
    db_name : str
        The database or service name

    Returns
    -------
    conn
        A database connection
    """

    try:
        if db_type == DBType.ORACLE.value:
            import cx_Oracle
            conn = cx_Oracle.connect(user,
                                     password,
                                     host + ":" + port + "/" + db_name,
                                     encoding="UTF-8", nencoding="UTF-8")
        elif db_type == DBType.MYSQL.value:
            import mysql.connector
            conn = mysql.connector.connect(
                                       user=user,
                                       password=password,
                                       host=host,
                                       port=int(port),
                                       database=db_name)
        elif db_type == DBType.POSTGRES.value:
            import psycopg2
            conn = psycopg2.connect("""user='{0}' 
                                       password='{1}' 
                                       host='{2}' 
                                       port='{3}' 
                                       dbname='{4}'""".format(user, password, host, port, db_name)
                                    )
        elif db_type == DBType.DB2.value:
            import ibm_db
            import ibm_db_dbi
            conn = ibm_db.connect("PROTOCOL=TCPIP;AUTHENTICATION=SERVER;"
                                  "UID={0};PWD={1};HOSTNAME={2};PORT={3};DATABASE={4};"
                                  .format(user, password, host, port, db_name), "", "")
            # Set autocommit explicitly off
            ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
            return ibm_db_dbi.Connection(conn)
        elif db_type == DBType.SQLSERVER.value:
            import pymssql
            conn = pymssql.connect(server=host, user=user, password=password, database=db_name)
            # 'pymssql.Connection' object attribute 'autocommit' is read-only
            conn.autocommit(False)
            return conn
        else:
            raise ValueError("Database type '{0}' is not supported.".format(db_type))

        # Set autocommit explicitly off for all database types
        conn.autocommit = False

        return conn

    except ModuleNotFoundError as err:
        raise ConnectionError("Database driver module is not installed: {0}. Please install it first.".format(str(err)))


def get_default_db_port(db_type):
    """Returns the default port for a database.

    Parameters
    ----------
    db_type : str
        The database type

    Returns
    -------
    str
        The default port
    """
    if db_type == DBType.ORACLE.value:
        return "1521"
    elif db_type == DBType.MYSQL.value:
        return "3306"
    elif db_type == DBType.POSTGRES.value:
        return "5432"
    elif db_type == DBType.DB2.value:
        return "50000"
    elif db_type == DBType.SQLSERVER.value:
        return "1433"


def get_csv_reader(file):
    """Returns a csv reader.

    Parameters
    ----------
    file : file-object
        A file object
    """
    return csv.reader(file, delimiter=cfg.column_separator, quotechar=cfg.quote_char)


def executemany(cur, stmt):
    """Runs executemany on the value set with the provided cursor.

    This function is a wrapper around the Python Database API 'executemany'
    to accommodate for psycopg2 slow 'executemany' implementation.

    Parameters
    ----------
    cur : cursor
        The cursor to run the statement with
    stmt : str
        The SQL statement to execute on
    """
    if cur is not None:
        if cfg.db_type != DBType.POSTGRES.value:
            cur.executemany(stmt, cfg.input_data)
        else:
            import psycopg2.extras as p
            p.execute_batch(cur, stmt, cfg.input_data)


def truncate_table(db_type, conn, table_name):
    """Truncates a database table.

    This function executes a TRUNCATE TABLE on a database table.
    The database user needs to have the right permissions to execute that statement.

    Parameters
    ----------
    db_type : str
        The database type connected to
    conn
        The database connection to use
    table_name : str
        The table name to be truncated
    """
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE {0}"
                .format(table_name + " IMMEDIATE"
                        if db_type == DBType.DB2.value
                        else table_name))
    cur.close()

    # Postgres and SQLServer handle TRUNCATE TABLE transactional
    # Db2 doesn't allow two TRUNCATE TABLE IMMEDIATE in one transaction, although it cannot be rolled back
    if db_type in (DBType.POSTGRES.value, DBType.SQLSERVER.value, DBType.DB2.value):
        conn.commit()
