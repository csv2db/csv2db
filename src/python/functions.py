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

    Raises
    ------
    UnicodeDecodeError
        If the file cannot be read in UTF-8 or the encoding provided
    """
    return_file = None

    if file.endswith(".zip"):
        zip_file = zipfile.ZipFile(file, mode="r")
        zfile = zip_file.open(zip_file.infolist()[0], mode="r")
        return_file = io.TextIOWrapper(zfile, encoding=cfg.file_encoding)
    elif file.endswith(".gz"):
        return_file = gzip.open(file, mode="rt", encoding=cfg.file_encoding)
    else:
        return_file = open(file, mode='r', encoding=cfg.file_encoding)

    # Test whether file can be read
    # If not, this will throw UnicodeDecodeError
    return_file.read(1)
    # Reset file position to the beginning of the file
    return_file.seek(0, 0)

    return return_file


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
    for idx, col in enumerate(next(reader), start=1):
        # Bug #56: if a file contains an emtpy column name (i.e id,,name,date,...), raise an error
        if col == "":
            raise NameError("The header column name is empty for column at position {0}.".format(idx))
        header.append(get_identifier(col.replace(' ', '_')))
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
    db_type : DBType
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

    Raises
    ------
    ValueError
        If the database type is not supported
    ConnectionError
        If the database driver is not found/installed
    """

    try:
        if db_type is DBType.ORACLE:
            import oracledb
            conn = oracledb.connect(user=user,
                                    password=password,
                                    dsn=host + ":" + port + "/" + db_name,
                                    encoding="UTF-8", nencoding="UTF-8")
        elif db_type is DBType.MYSQL:
            import mysql.connector
            conn = mysql.connector.connect(
                                       user=user,
                                       password=password,
                                       host=host,
                                       port=int(port),
                                       database=db_name)
        elif db_type is DBType.POSTGRES:
            import psycopg
            conn = psycopg.connect("""user='{0}' 
                                      password='{1}' 
                                      host='{2}' 
                                      port='{3}' 
                                      dbname='{4}'""".format(user, password, host, port, db_name)
                                   )
        elif db_type is DBType.DB2:
            import ibm_db
            import ibm_db_dbi
            conn = ibm_db.connect("PROTOCOL=TCPIP;AUTHENTICATION=SERVER;"
                                  "UID={0};PWD={1};HOSTNAME={2};PORT={3};DATABASE={4};"
                                  .format(user, password, host, port, db_name), "", "")
            # Set autocommit explicitly off
            ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
            return ibm_db_dbi.Connection(conn)
        elif db_type is DBType.SQLSERVER:
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
    db_type : DBType
        The database type

    Returns
    -------
    str
        The default port
    """
    if db_type is DBType.ORACLE:
        return "1521"
    elif db_type is DBType.MYSQL:
        return "3306"
    elif db_type is DBType.POSTGRES:
        return "5432"
    elif db_type is DBType.DB2:
        return "50000"
    elif db_type is DBType.SQLSERVER:
        return "1433"


def get_csv_reader(file):
    """Returns a csv reader.

    Parameters
    ----------
    file : file-object
        A file object

    Returns
    -------
    object
        The csv reader object
    """
    return csv.reader(file, delimiter=cfg.column_separator, quotechar=cfg.quote_char)


def truncate_table(db_type, conn, table_name):
    """Truncates a database table.

    This function executes a TRUNCATE TABLE on a database table.
    The database user needs to have the right permissions to execute that statement.

    Parameters
    ----------
    db_type : DBType
        The database type connected to
    conn
        The database connection to use
    table_name : str
        The table name to be truncated
    """
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE {0}"
                .format(table_name + " IMMEDIATE"
                        if db_type is DBType.DB2
                        else table_name))
    cur.close()

    # Postgres and SQLServer handle TRUNCATE TABLE transactional
    # Db2 doesn't allow two TRUNCATE TABLE IMMEDIATE in one transaction, although it cannot be rolled back
    if db_type in (DBType.POSTGRES, DBType.SQLSERVER, DBType.DB2):
        conn.commit()


class BadRecordLogger:
    """This class logs bad records into a file."""

    def __init__(self, file_name):
        """Initializes a BadRecordLogger object.

        Parameters
        ----------
        file_name : str
            The file name to log bad records to.
        """
        self.file_name = file_name
        self.file = None

    def write_bad_record(self, record):
        """Writes a bad record.

        Parameters
        ----------
        record : tuple
            The record to write. A new line will be appended by this method.
        """
        if self.file is None:
            self.file = open(self.file_name, mode="w", encoding="utf-8")
        self.file.write(cfg.column_separator.join(record) + '\n')

    def close(self):
        """Close file."""
        self.file.close()

    def __enter__(self):
        """Create context manager."""
        return self

    def __exit__(self, exc_type, exc_value, trace_back):
        """Destroy context manager."""
        self.__del__()

    def __del__(self):
        """Close file."""
        if self.file is not None:
            self.close()


def get_identifier(identifier):
    """Returns an identifier name considering global identifier settings.

    Parameters
    ----------
    identifier : str
        An identifier string

    Returns
    -------
    str
        A transformed identifier string based on the global identifier settings.
    """
    if cfg.case_insensitive_identifiers:
        identifier = identifier.upper()
    return identifier
