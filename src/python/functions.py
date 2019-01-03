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

import zipfile
import gzip
import glob
import os
from enum import Enum


def open_file(file):
    """Opens a CSV file.

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
        return zip_file.open(zip_file.infolist()[0], mode="r")
    elif file.endswith(".gz"):
        return gzip.open(file, mode="r")
    else:
        return open(file, mode='r')


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


def verbose(output):
    """Print verbose output.

    Parameters
    ----------
    output : str
        The output to print
    """
    if verbose:
        print(output)


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
        if db_type is DBType.ORACLE.value:
            import cx_Oracle
            return cx_Oracle.connect(user,
                                     password,
                                     host + ":" + port + "/" + db_name)
        elif db_type is DBType.MYSQL.value:
            import mysqlx
            return mysqlx.get_session({
                                       'user': user,
                                       'password': password,
                                       'host': host,
                                       'port': int(port),
                                      }).get_schema(db_name)
        elif db_type is DBType.POSTGRES.value:
            import psycopg2
            return psycopg2.connect("""user='{0}' 
                                       password='{1}' 
                                       host='{2}' 
                                       port='{3}' 
                                       dbname='{4}'""".format(user, password, host, port, db_name)
                                    )
        elif db_type is DBType.SQLSERVER.value:
            import pypyodbc
            return pypyodbc.connect("Driver={SQL Server};" +
                                    """uid={0};
                                       pwd={1};
                                       Server={2};
                                       Port={3};
                                       Database={4};""".format(user, password, host, port, db_name)
                                    )
    except Exception as err:
        raise ConnectionError(err)


def raw_input_to_list(raw_line, header=False):
    ret = raw_line
    if isinstance(raw_line, (bytes, bytearray)):
        ret = raw_line.decode()
    ret = ret.splitlines()[0]
    # If empty string return None, i.e. skip empty lines
    if not ret:
        return None
    ret = ret.split(",")
    for n in range(len(ret)):
        val = ret[n].replace('"', '').strip()
        # If line is a header line, i.e. column number, replace spaces with '_' and make names UPPER
        if header:
            val = val.replace(' ', '_',).upper()
        ret[n] = val
    return ret


def raw_input_to_set(raw_line, header=False):
    return set(raw_input_to_list(raw_line, header))


class DBType(Enum):
    ORACLE = "oracle"
    MYSQL = "mysql"
    POSTGRES = "postgres"
    SQLSERVER = "sqlserver"
