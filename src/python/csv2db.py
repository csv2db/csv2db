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

import argparse
import getpass
import sys

import config as cfg
import constants as cons
import functions as f


def set_global_config(args):
    """Sets csv2db global configuration.

    Parameters
    ----------
    args : argparse.Namespace
        The populated argparse namespace.
    """
    # Set verbose and debug output flags
    cfg.verbose = args.verbose
    # Verbose is implicit for debug output
    if args.debug:
        cfg.verbose = True
        cfg.debug = True

    # Set case insensitive identifiers
    cfg.case_insensitive_identifiers = args.case_insensitive_identifiers
    f.debug("Case insensitive identifiers: {0}".format(cfg.case_insensitive_identifiers))

    # Set quoted identifiers
    cfg.quote_identifiers = args.quote_identifiers
    f.debug("Quoted identifiers: {0}".format(cfg.quote_identifiers))

    # Set DB type
    cfg.db_type = cons.DBType(args.dbtype)
    f.debug("DB type: {0}".format(cfg.db_type))

    # Set table name
    cfg.table_name = f.get_identifier(args.table)
    f.debug("Table name: {0}".format(cfg.table_name))

    # Set column separator characters(s)
    cfg.column_separator = args.separator
    f.debug("Column separator: {0}".format(cfg.column_separator))

    # Set quote character(s)
    cfg.quote_char = args.quote
    f.debug("Column escape character: {0}".format(cfg.quote_char))

    # Set file encoding
    cfg.file_encoding = args.encoding
    f.debug("File encoding: {0}".format(cfg.file_encoding))


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

    set_global_config(args)

    # Find all files
    f.verbose("Finding file(s).")
    file_names = f.find_all_files(args.file)
    f.verbose("Found {0} file(s).".format(len(file_names)))
    # Exit program if no files found.
    if len(file_names) == 0:
        return cons.ExitCodes.SUCCESS.value
    f.debug(file_names)

    # Generate CREATE TABLE SQL
    if args.command.startswith("gen"):
        f.verbose("Generating CREATE TABLE statement.")
        try:
            generate_table_sql(file_names, f.get_identifier(args.column_type, True))
            return cons.ExitCodes.SUCCESS.value
        except Exception:
            exception, tb_str = f.get_exception_details()
            f.error("Error generating statement: {0}".format(exception))
            f.debug(tb_str)
            return cons.ExitCodes.GENERIC_ERROR.value

    # Load data
    else:
        if args.directpath:
            cfg.direct_path = args.directpath
            f.debug("'DIRECT PATH' loading option set by user")

        if args.truncate:
            cfg.truncate_before_load = args.truncate
            f.debug("'TRUNCATE TABLE' option set by user")

        # Set DB default port, if needed
        if args.port is None:
            args.port = f.get_default_db_port(cfg.db_type)
            f.debug("Using default port {0}".format(args.port))

        # Set batch size
        cfg.batch_size = int(args.batch)
        f.debug("Batch size: {0}".format(cfg.batch_size))

        # If direct path has been specified and batch size is lower than 10k, overwrite batch size to 10k.
        if cfg.direct_path and cfg.batch_size < 10000:
            f.debug("Direct path was specified but batch size is less than 10000.")
            f.debug("Overwriting the batch size to 10000 for direct-path load to make sense.")
            cfg.batch_size = 10000

        # If password hasn't been specified via parameter, prompt for it
        if args.password is None:
            f.debug("Password has not been provided via parameter, prompting for it.")
            args.password = getpass.getpass(prompt='DB user password: ')

        # Set logging errors flag
        cfg.log_bad_records = args.log
        # Set ignore error flag (log_errors implies ignore errors)
        cfg.ignore_errors = (args.ignore or cfg.log_bad_records)
        f.debug("Ignore errors: {0}".format(cfg.ignore_errors))
        f.debug("Log errors: {0}".format(cfg.log_bad_records))

        f.verbose("Establishing database connection.")
        f.debug("Database details:")
        f.debug({"dbtype": args.dbtype, "user": args.user, "host": args.host, "port": args.port, "dbname": args.dbname})

        try:
            cfg.conn = f.get_db_connection(cfg.db_type, args.user, args.password, args.host, args.port, args.dbname)
        except Exception:
            exception, tb_str = f.get_exception_details()
            f.error("Error connecting to the database: {0}".format(exception))
            f.debug(tb_str)
            return cons.ExitCodes.DATABASE_ERROR.value

        try:
            if cfg.truncate_before_load:
                f.verbose("Truncating table before load.")
                f.truncate_table(cfg.db_type, cfg.conn, cfg.table_name)

            load_files(file_names)

            f.verbose("Closing database connection.")
            cfg.conn.close()
            return cons.ExitCodes.SUCCESS.value if not cfg.data_loading_error else cons.ExitCodes.DATA_LOADING_ERROR.value
        except KeyboardInterrupt:
            print("Exiting program")
            cfg.conn.close()
            return cons.ExitCodes.GENERIC_ERROR.value
        except Exception:
            exception, tb_str = f.get_exception_details()
            f.error("Error loading file(s): {0}".format(exception))
            f.debug(tb_str)
            cfg.conn.close()
            return cons.ExitCodes.GENERIC_ERROR.value


def generate_table_sql(file_names, column_data_type):
    """Generates SQL for the table to load data.

    Parameters
    ----------
    file_names : str
        The file_names to scan for columns
    column_data_type : str
        The column data type to use
    """
    col_list = []
    for file_name in file_names:
        f.debug("Reading file {0}".format(file_name))
        with f.open_file(file_name) as file:
            reader = f.get_csv_reader(file)
            columns_to_add = f.read_header(reader)
            f.debug("Columns to add {0}".format(columns_to_add))
            # Add columns to list implicitly removing duplicates for when going over multiple files
            col_list.extend(col for col in columns_to_add if col not in col_list)
    print_table_and_columns(col_list, column_data_type)


def print_table_and_columns(col_list, column_data_type):
    """Prints the SQL CREATE TABLE statement to stdout.

    Parameters
    ----------
    col_list : [str,]
        The column list for the table
    column_data_type : str
        The data type to use for all columns
    """
    if cfg.table_name is not None:
        print("CREATE TABLE {0}".format(cfg.table_name))
    else:
        print("CREATE TABLE <TABLE NAME>")
    print("(")
    cols = ""
    for col in col_list:
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
        try:
            # Open file (will check whether file can be read)
            with f.open_file(file_name) as file:
                try:
                    read_and_load_file(file)
                    print("File loaded.")
                except StopIteration:
                    print("File is empty: {0}".format(file_name))
                # Catch any unanticipated exceptions and report stack trace
                except Exception:
                    f.error("Error while loading file into table: {0}".format(file.name))
                    exception, traceback = f.get_exception_details()
                    f.error(exception)
                    f.debug(traceback)
                    cfg.data_loading_error = True
                    print("Skipping file.")
        except UnicodeDecodeError:
            f.error("File is not UTF-8 encoded or in a UTF-8 compatible encoding: {0}".format(file_name))
            f.error("Please specify the encoding that should be used via the '--encoding' parameter.")
            cfg.data_loading_error = True
            print("Skipping file.")
        print()


def read_and_load_file(file):
    """Reads and loads file.

    Parameters
    ----------
    file : file_object
        The file to load
    """
    reader = f.get_csv_reader(file)
    col_map = f.read_header(reader)
    f.debug("Column map: {0}".format(col_map))
    if cfg.log_bad_records:
        cfg.bad_records_logger = f.BadRecordLogger(file.name + ".bad")
    for line in reader:
        load_data(col_map, line)
    load_data(col_map, None)
    if cfg.log_bad_records:
        cfg.bad_records_logger.close()


def load_data(col_map, data):
    """Loads the data into the database.

    Parameters
    ----------
    col_map : [str,]
        The columns to load the data into
    data : [str,]
        The data to load. If data is None the array will be loaded and flushed.
    """
    if data is not None and len(data) > 0:
        # If ignore errors is set and log bad records is not
        # check whether the row has more values than the header
        # If just ignore errors is set, ignore the additional records
        # If log errors is set, leave the additional values so that the row will be logged as an invalid one
        if cfg.ignore_errors and not cfg.log_bad_records:
            while len(data) > len(col_map):
                f.debug("Removing extra row value entry not present in the header.")
                data.pop()
        # tuple or dictionary only for SQL Server
        cfg.input_data.append(tuple(data))

    # If batch size has been reached or input array should be flushed
    if (len(cfg.input_data) == cfg.batch_size) or (data is None and len(cfg.input_data) > 0):
        f.debug("Executing statement:")
        stmt = generate_statement(col_map)
        f.debug(stmt)
        cur = cfg.conn.cursor()
        errors = False
        try:
            cur.executemany(stmt, cfg.input_data)
            cur.close()
        # Catch batch execution exception
        except Exception as err:
            f.verbose("Error executing batch.")
            errors = True
            # Rollback old batch (needed for at least Postgres to finish transaction)
            # Previous successful batches would have already been committed.
            f.debug("Rollback current batch.")
            cfg.conn.rollback()
            cur.close()
            # If neither ignore nor debug output is enabled, raise error
            if not cfg.ignore_errors and not cfg.debug:
                cfg.input_data.clear()
                raise err
            # If ignore errors or debug output is enabled, find failing record
            # Avoid "else" for future maintainability
            if cfg.ignore_errors or cfg.debug:
                f.verbose("Executing batch row by row.")
                records_loaded = 0
                records_ignored = 0
                for record in cfg.input_data:
                    try:
                        # Get new cursor for every row to avoid previous row variables name/number caching.
                        cur_err = cfg.conn.cursor()
                        cur_err.execute(stmt, record)
                        # Postgres doesn't support errors within transaction boundaries
                        # Once there is an error in a transaction, that transaction needs to be ended
                        # Hence, to get all the other successful rows into Postgres before an error
                        # we commit here every successful row.
                        # Likewise, it seems that SQL Server aborts any erroneous transaction implicitly,
                        # so we want to commit every row that was successful.
                        if cfg.db_type is cons.DBType.POSTGRES or cfg.db_type is cons.DBType.SQLSERVER:
                            f.debug("Commit")
                            cfg.conn.commit()
                        cur_err.close()
                        records_loaded += 1
                    except Exception as err:
                        cur_err.close()
                        f.debug("Error with record: {0}".format(record))
                        f.debug("Error: {0}".format(err))
                        # If only DEBUG output is set, we are done.
                        # We found the bad record, told the user, time to clear the batch and raise the error
                        if not cfg.ignore_errors:
                            cfg.input_data.clear()
                            raise err
                        else:
                            f.verbose("Ignoring invalid record.")
                            records_ignored += 1
                            # Rollback the broken transaction for Postgres
                            if cfg.db_type is cons.DBType.POSTGRES or cfg.db_type is cons.DBType.SQLSERVER:
                                f.debug("Rollback")
                                cfg.conn.rollback()
                            # Ignore errors is implied with log bad errors
                            # (there is no point logging bad errors if the program is about
                            #  to abort on a bad error because ignore errors isn't set)
                            if cfg.log_bad_records:
                                f.verbose("Logging invalid record.")
                                cfg.bad_records_logger.write_bad_record(record)
                f.debug("Commit")
                cfg.conn.commit()
                f.verbose("{0} rows loaded.".format(records_loaded))
                f.verbose("{0} rows ignored.".format(records_ignored))
        # If errors occurred for Postgres or SQL Server, do not commit at end as all rows have already been
        # committed one by one. SQL Server will throw an error when issuing a commit and no transaction is running
        if not errors or (errors and cfg.db_type is not cons.DBType.POSTGRES and cfg.db_type is not cons.DBType.SQLSERVER):
            f.debug("Commit")
            cfg.conn.commit()
        # In the error case, we already printed how many rows were loaded and ignored
        if not errors:
            f.verbose("{0} rows loaded.".format(len(cfg.input_data)))
        # Always clear input array when errors or success
        cfg.input_data.clear()


def generate_statement(col_map):
    """Generates the INSERT statement

    Parameters
    ----------
    col_map : [str,]
        The columns to load the data into
    """
    append_hint = ""
    if cfg.db_type is cons.DBType.ORACLE:
        values = ":" + ", :".join(col_map)
        if cfg.direct_path:
            append_hint = " /*+ APPEND_VALUES */"
    elif cfg.db_type is cons.DBType.DB2:
        values = ("?," * len(col_map))[:-1]
    else:
        values = ("%s, " * len(col_map))[:-2]
    return "INSERT{0} INTO {1} ({2}) VALUES ({3})".format(append_hint,
                                                          cfg.table_name,
                                                          ", ".join(col_map),
                                                          values)


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
    parser = argparse.ArgumentParser(
        prog="csv2db",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="The CSV to database command line loader.\nVersion: {0}\n(c) Gerald Venzl".format(cfg.version)
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    # Sub Parser generate
    parser_generate = subparsers.add_parser("generate", aliases=["gen"],
                                            help="Prints a CREATE TABLE SQL statement to create the table " +
                                                 "and columns based on the header row of the CSV file(s).")
    parser_generate.add_argument("-f", "--file", default="*.csv.zip",
                                 help="The file to read the header from, by default all *.csv.zip files")
    parser_generate.add_argument("-e", "--encoding", default="utf-8",
                                 help="The file encoding to be used to read the file, " +
                                      "see https://docs.python.org/3/library/codecs.html#standard-encodings " +
                                      "for a list of all allowed encodings.")
    parser_generate.add_argument("-v", "--verbose", action="store_true", default=False,
                                 help="Verbose output.")
    parser_generate.add_argument("--debug", action="store_true", default=False,
                                 help="Debug output.")
    parser_generate.add_argument("-o", "--dbtype", default="oracle", choices=[e.value for e in cons.DBType],
                                 help="The database type.")
    parser_generate.add_argument("-t", "--table",
                                 help="The table name to use.")
    parser_generate.add_argument("-c", "--column-type", default="varchar(1000)",
                                 help="The column type to use for the table generation.")
    parser_generate.add_argument("-s", "--separator", default=",",
                                 help="The columns separator character(s).")
    parser_generate.add_argument("-q", "--quote", default='"',
                                 help="The quote character on which a string won't be split.")
    parser_generate.add_argument("--case-insensitive-identifiers", action="store_true", default=False,
                                 help="If set, all identifiers will be upper-cased.")
    parser_generate.add_argument("--quote-identifiers", action="store_true", default=False,
                                 help="If set, all table and column identifiers will be quoted.")

    # Sub Parser load
    parser_load = subparsers.add_parser("load", aliases=["lo"],
                                        help="Loads the data from the CSV file(s) into the database.")
    parser_load.add_argument("-f", "--file", default="*.csv.zip",
                             help="The file to load, by default all *.csv.zip files")
    parser_load.add_argument("-e", "--encoding", default="utf-8",
                             help="The file encoding to be used to read the file, " +
                                  "see https://docs.python.org/3/library/codecs.html#standard-encodings " +
                                  "for a list of all allowed encodings.")
    parser_load.add_argument("-v", "--verbose", action="store_true", default=False,
                             help="Verbose output.")
    parser_load.add_argument("--debug", action="store_true", default=False,
                             help="Debug output.")
    parser_load.add_argument("-t", "--table", required=True,
                             help="The table name to use.")
    parser_load.add_argument("-o", "--dbtype", default="oracle", choices=[e.value for e in cons.DBType],
                             help="The database type.")
    parser_load.add_argument("-u", "--user", required=True,
                             help="The database user to load data into.")
    parser_load.add_argument("-p", "--password",
                             help="The database schema password. csv2db will prompt for the password " +
                                  "if the parameter is missing which is a more secure method of providing a password.")
    parser_load.add_argument("-m", "--host", default="localhost",
                             help="The host name on which the database is running on.")
    parser_load.add_argument("-n", "--port",
                             help="The port on which the database is listening. " +
                                  "If not passed on the default port will be used " +
                                  "(Oracle: 1521, MySQL: 3306, PostgreSQL: 5432, SQL Server: 1433, DB2: 50000).")
    parser_load.add_argument("-d", "--dbname", default="ORCLPDB1",
                             help="The name of the database.")
    parser_load.add_argument("-b", "--batch", default="10000",
                             help="How many rows should be loaded at once.")
    parser_load.add_argument("-s", "--separator", default=",",
                             help="The columns separator character(s).")
    parser_load.add_argument("-q", "--quote", default='"',
                             help="The quote character on which a string won't be split.")
    parser_load.add_argument("-a", "--directpath", action="store_true", default=False,
                             help="Execute a direct path INSERT load operation (Oracle only).")
    parser_load.add_argument("--truncate", action="store_true", default=False,
                             help="Truncate/empty table before loading.")
    parser_load.add_argument("-i", "--ignore", action="store_true", default=False,
                             help="Ignore erroneous/invalid lines in files and continue the load.")
    parser_load.add_argument("-l", "--log", action="store_true", default=False,
                             help="Log erroneous/invalid lines in *.bad file of the same name as the input file " +
                                  "(this implies the --ignore option).")
    parser_load.add_argument("--case-insensitive-identifiers", action="store_true", default=False,
                             help="If set, all identifiers will be upper-cased.")
    parser_load.add_argument("--quote-identifiers", action="store_true", default=False,
                             help="If set, all table and column identifiers will be quoted.")

    return parser.parse_args(cmd)


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
