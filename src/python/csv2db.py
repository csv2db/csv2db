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
import config as cfg


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

    # Set verbose and debug output flags
    cfg.verbose = args.verbose
    if args.debug:
        cfg.verbose = True
        cfg.debug = True

    # Set table name
    cfg.table_name = args.table

    # Find all files
    f.verbose("Finding file(s).")
    file_names = f.find_all_files(args.file)
    f.debug("Found {0} files.".format(len(file_names)))
    f.debug(file_names)

    if args.command.startswith("gen"):
        f.verbose("Generating CREATE TABLE statement.")
        generate_table_sql(file_names, args.column_type)
    else:
        # Set DB type
        cfg.db_type = args.dbtype

        if args.port is None:
            if cfg.db_type == f.DBType.ORACLE.value:
                args.port = "1521"
            elif cfg.db_type == f.DBType.MYSQL.value:
                args.port = "3306"
            elif cfg.db_type == f.DBType.POSTGRES.value:
                args.port = "5432"

        # Set batch size
        f.debug("Batch size: {0}".format(args.batch))
        cfg.batch_size = int(args.batch)

        f.verbose("Establishing database connection.")
        f.debug("Database details:")
        f.debug({"dbtype": args.dbtype, "user": args.user, "host": args.host, "port": args.port, "dbname": args.dbname})
        try:
            cfg.conn = f.get_db_connection(cfg.db_type, args.user, args.password, args.host, args.port, args.dbname)
            load_files(file_names)
            f.verbose("Closing database connection.")
            cfg.conn.close()
        except Exception as err:
            print("Error connecting to the database: {0}".format(err))
        except KeyboardInterrupt:
            print("Exiting program")
            cfg.conn.close()


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
    if cfg.table_name is not None:
        print("CREATE TABLE {0}".format(cfg.table_name))
    else:
        print("CREATE TABLE <TABLE NAME>")
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
        f.debug("Opening file handler for '{0}'".format(file_name))
        with f.open_file(file_name) as file:
            read_and_load_file(file)
        print("Done")
        print()


def read_and_load_file(file):
    """Reads and loads file.

    Parameters
    ----------
    file : file_object
        The file to load
    """
    col_map = f.raw_input_to_list(file.readline(), True)
    f.debug("Column map: {0}".format(col_map))
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
    if data is not None:
        values = f.raw_input_to_list(data)
        if values:
            # If the data has more values than the header provided, ignore the end (green data set has that)
            while len(values) > len(col_map):
                f.debug("Removing extra row value entry not present in the header.")
                values.pop()
            cfg.input_data.append(values)

    if (len(cfg.input_data) == cfg.batch_size) or (data is None):
        f.debug("Executing statement:")
        stmt = generate_statement(col_map)
        f.debug(stmt)
        cur = cfg.conn.cursor()
        cur.executemany(stmt, cfg.input_data)
        f.debug("Commit")
        cfg.conn.commit()
        cur.close()
        cfg.input_data.clear()
        f.verbose("{0} rows loaded".format(len(cfg.input_data)))


def generate_statement(col_map):
    """Generates the INSERT statement

    Parameters
    ----------
    col_map : [str,]
        The columns to load the data into
    """
    if cfg.db_type == f.DBType.ORACLE.value:
        values = ":" + ", :".join(col_map)
    else:
        values = ("%s, " * len(col_map))[:-2]
    return "INSERT INTO {0} ({1}) VALUES ({2})".format(cfg.table_name, ", ".join(col_map), values)


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

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    # Sub Parser generate
    parser_generate = subparsers.add_parser("generate", aliases=["gen"],
                                            help="Prints a CREATE TABLE SQL statement to create the table " +
                                                 "and columns based on the header row of the CSV file(s).")
    parser_generate.add_argument("-f", "--file", default="*.csv.zip",
                                 help="The file to load, by default all *.csv.zip files")
    parser_generate.add_argument("-v", "--verbose", action="store_true", default=False,
                                 help="Verbose output.")
    parser_generate.add_argument("--debug", action="store_true", default=False,
                                 help="Debug output.")
    parser_generate.add_argument("-t", "--table",
                                 help="The table name to use.")
    parser_generate.add_argument("-c", "--column-type", default="VARCHAR2(4000)",
                                 help="The column type to use for the table generation.")

    # Sub Parser load
    parser_load = subparsers.add_parser("load", aliases=["lo"],
                                        help="Loads the data from the CSV file(s) into the database.")
    parser_load.add_argument("-f", "--file", default="*.csv.zip",
                             help="The file to load, by default all *.csv.zip files")
    parser_load.add_argument("-v", "--verbose", action="store_true", default=False,
                             help="Verbose output.")
    parser_load.add_argument("--debug", action="store_true", default=False,
                             help="Debug output.")
    parser_load.add_argument("-t", "--table",
                             help="The table name to use.")
    parser_load.add_argument("-o", "--dbtype", default="oracle",
                             help="The database type. Choose one of {0}.".format([e.value for e in f.DBType]))
    parser_load.add_argument("-u", "--user",
                             help="The database user to load data into.")
    parser_load.add_argument("-p", "--password",
                             help="The database schema password.")
    parser_load.add_argument("-m", "--host", default="localhost",
                             help="The host name on which the database is running on.")
    parser_load.add_argument("-n", "--port",
                             help="The port on which the database is listening. " +
                                  "If not passed on the default port will be used " +
                                  "(Oracle: 1521, MySQL: 3306, PostgreSQL: 5432).")
    parser_load.add_argument("-d", "--dbname", default="ORCLPDB1",
                             help="The name of the database.")
    parser_load.add_argument("-b", "--batch", default="10000",
                             help="How many rows should be loaded at once.")

    return parser.parse_args(cmd)


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
