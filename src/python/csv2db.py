#!/usr/bin/env python3
#
# Since: January, 2019
# Author: gvenzl
# Name: csv2db.py
# Description: CSV 2 (to) DB main file
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

import sys
import argparse
import functions as f

conn = None
batchsize = 0
table_name = ""
input_data = []


def run(cmd):
    """Runs csv2db.

    This function is the main entry point for csv2db.

    Parameters
    ----------
    cmd : str array
        The arguments passed

    Returns
    -------
    int
        The exit code.
    """
    args = parse_arguments(cmd)

    # Set batch size
    global batchsize
    batchsize = int(args.batch)

    # Set verbose
    global verbose
    verbose = args.verbose

    # Set table name
    global table_name
    table_name = args.table

    # Find all files
    file_names = f.find_all_files(args.file)

    if args.generate:
        generate_table_sql(file_names, args.column_type)
    else:
        global conn
        conn = f.get_db_connection(args.dbtype, args.user, args.password, args.host, args.port, args.dbname)
        load_files(file_names)
        conn.close()


def generate_table_sql(file_names, column_data_type):
    """Generates SQL for the table to load data.

    Parameters
    ----------
    file_names : str
        The file_names to scan for columns
    column_data_type : str
        The column data type to use
    """
    col_set = set()
    for file_name in file_names:
        file = f.open_file(file_name)
        columns_to_add = f.read_header(file)
        col_set = add_to_col_set(col_set, columns_to_add)
        file.close()
    print_table_and_col_set(col_set, column_data_type)


def add_to_col_set(col_set, columns_to_add):
    """Adds a column set to another one, without duplicates.

    Parameters
    ----------
    col_set : set()
        The column set to add to
    columns_to_add : set()
        The columns to add to the first set

    Returns
    -------
    set()
        A new set with the two sets combined.
    """
    if col_set is None:
        return columns_to_add
    else:
        return col_set.union(columns_to_add)


def print_table_and_col_set(col_set, column_data_type):
    """Prints the SQL CREATE TABLE statement to stdout.

    Parameters
    ----------
    col_set : set(str)
        The column set for the table
    column_data_type : str
        The data type to use for all columns
    """
    global table_name
    if table_name is None:
        table_name = "<TABLE NAME>"
    print("CREATE TABLE {0}".format(table_name))
    print("(")
    cols = ""
    for col in col_set:
        cols += "  " + col + " " + column_data_type + ",\n"
    cols = cols[:-2]
    print(cols)
    print(");")
    print()


def load_files(file_names):
    """Loads all files into the database.

    file_names : str
        All the file names to load into the database
    """
    for file_name in file_names:
        print()
        print("Loading file {0}".format(file_name))
        print()
        file = f.open_file(file_name)
        read_and_load_file(file, table_name)
        file.close()


def read_and_load_file(file):
    """Reads and loads file.

    Parameters
    ----------
    file : file_object
        The file to load
    """
    col_map = f.raw_input_to_list(file.readline(), True)
    try:
        for raw_line in file:
            load_data(col_map, raw_line)
        load_data(col_map, None)
    except Exception as err:
        print("Error in file: {0}".format(file.name), err)


def load_data(col_map, data):
    """Loads the data into the database.

    Parameters
    ----------
    col_map : [str,]
        The columns to load the data into
    data : bytes, bytearray, str
        The data to load
    """
    global input_data

    if data is not None:
        values = f.raw_input_to_list(data)
        if values:
            # If the data has more values than the header provided, ignore the end (green data set has that)
            while len(values) > len(col_map):
                values.pop()
            input_data.append(values)

    if (len(input_data) == batchsize) or (data is None):
        global conn
        cur = conn.cursor()
        cur.executemany(generate_statement(col_map), input_data)
        conn.commit()
        input_data.clear()


def generate_statement(col_map):
    """Generates the INSERT statement

    Parameters
    ----------
    col_map : [str,]
        The columns to load the data into
    """
    return "INSERT INTO {0} ({1}) VALUES (:{2})".format(
                        table_name,
                        ", ".join(col_map),
                        ", :".join(col_map))


def parse_arguments(cmd):
    """Parses the arguments.

    Parameters
    ----------
    cmd : str array
        The arguments passed

    Returns
    -------
    arg
        Argparse object
    """
    parser = argparse.ArgumentParser(prog="csv2db", description="A loader for CSV files.")
    parser.add_argument("-f", "--file", default="*.csv.zip",
                        help="The file to load, by default all *.csv.zip files")
    parser.add_argument("-o", "--dbtype", default="oracle",
                        help="The database type. Choose one of {0}, ".format([e.value for e in f.DBType]))
    parser.add_argument("-u", "--user",
                        help="The database user to load data into")
    parser.add_argument("-p", "--password",
                        help="The database schema password")
    parser.add_argument("-m", "--host", default="localhost",
                        help="The host name on which the database is running on")
    parser.add_argument("-n", "--port", default="1521",
                        help="The port on which the database is listening")
    parser.add_argument("-d", "--dbname", default="ORCLPDB1",
                        help="The name of the database")
    parser.add_argument("-b", "--batch", default="10000",
                        help="How many rows should be loaded at once.")
    parser.add_argument("-t", "--table",
                        help="The table to load data into.")
    parser.add_argument("-g", "--generate", action="store_true", default=False,
                        help="Generates the table and columns based on the header row of the CSV file.")
    parser.add_argument("-c", "--column-type", default="VARCHAR2(4000)",
                        help="The column type to use for the table generation")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="Verbose output.")
    return parser.parse_args(cmd)


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
