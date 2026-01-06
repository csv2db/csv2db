```
                  ___       ____  
  ___________   _|__ \ ____/ / /_ 
 / ___/ ___/ | / /_/ // __  / __ \
/ /__(__  )| |/ / __// /_/ / /_/ /
\___/____/ |___/____/\____/_____/ 
                                  
```


The CSV to database command line loader.

`csv2db` reads CSV files and loads them into a database.
Rather than having to go through the CSV data first and find out what columns and data types are present in the CSV files,
`csv2db` will read the header in each CSV file and automatically load data into the columns of the same name into the target table.
The case of the header column names will be preserved as present in the CSV file.
Spaces in the header column names are automatically replaced with `_` characters, for example, the column `station id`
in the CSV file will be interpreted as `station_id` column in the table.

This approach allows you to get data into the database first and worry about the data cleansing part later,
which is usually much easier once the data is in the database rather than in the CSV files.

`csv2db` is capable of scanning all CSV file headers at once and generate a `CREATE TABLE` statement with all the column names present.
This is particularly useful if the format of the CSV files has changed over time or because you want to load different CSV file types into the same database table.

# Usage

```bash
$ ./csv2db -h
usage: csv2db [-h] {generate,gen,load,lo} ...

The CSV to database command line loader.
Version: 1.6.1
(c) Gerald Venzl

positional arguments:
  {generate,gen,load,lo}
    generate (gen)      Prints a CREATE TABLE SQL statement to create the
                        table and columns based on the header row of the CSV
                        file(s).
    load (lo)           Loads the data from the CSV file(s) into the database.

options:
  -h, --help            show this help message and exit
```

```bash
$ ./csv2db generate -h
usage: csv2db generate [-h] [-f FILE] [-e ENCODING] [-v] [--debug]
                       [-o {oracle,mysql,postgres,sqlserver,db2}] [-t TABLE]
                       [-c COLUMN_TYPE] [-s SEPARATOR] [-q QUOTE]
                       [--case-insensitive-identifiers] [--quote-identifiers]

options:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  The file to read the header from, by default all
                        *.csv.zip files
  -e ENCODING, --encoding ENCODING
                        The file encoding to be used to read the file, see htt
                        ps://docs.python.org/3/library/codecs.html#standard-
                        encodings for a list of all allowed encodings.
  -v, --verbose         Verbose output.
  --debug               Debug output.
  -o {oracle,mysql,postgres,sqlserver,db2}, --dbtype {oracle,mysql,postgres,sqlserver,db2}
                        The database type.
  -t TABLE, --table TABLE
                        The table name to use.
  -c COLUMN_TYPE, --column-type COLUMN_TYPE
                        The column type to use for the table generation.
  -s SEPARATOR, --separator SEPARATOR
                        The columns separator character(s).
  -q QUOTE, --quote QUOTE
                        The quote character on which a string won't be split.
  --case-insensitive-identifiers
                        If set, all identifiers will be upper-cased.
  --quote-identifiers   If set, all table and column identifiers will be
                        quoted.
```

```bash
$ ./csv2db load -h
usage: csv2db load [-h] [-f FILE] [-e ENCODING] [-v] [--debug] -t TABLE
                   [-o {oracle,mysql,postgres,sqlserver,db2}] -u USER
                   [-p PASSWORD] [-m HOST] [-n PORT] [-d DBNAME] [-b BATCH]
                   [-s SEPARATOR] [-q QUOTE] [-a] [--truncate] [-i] [-l]
                   [--case-insensitive-identifiers] [--quote-identifiers]

options:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  The file to load, by default all *.csv.zip files
  -e ENCODING, --encoding ENCODING
                        The file encoding to be used to read the file, see htt
                        ps://docs.python.org/3/library/codecs.html#standard-
                        encodings for a list of all allowed encodings.
  -v, --verbose         Verbose output.
  --debug               Debug output.
  -t TABLE, --table TABLE
                        The table name to use.
  -o {oracle,mysql,postgres,sqlserver,db2}, --dbtype {oracle,mysql,postgres,sqlserver,db2}
                        The database type.
  -u USER, --user USER  The database user to load data into.
  -p PASSWORD, --password PASSWORD
                        The database schema password. csv2db will prompt for
                        the password if the parameter is missing which is a
                        more secure method of providing a password.
  -m HOST, --host HOST  The host name on which the database is running on.
  -n PORT, --port PORT  The port on which the database is listening. If not
                        passed on the default port will be used (Oracle: 1521,
                        MySQL: 3306, PostgreSQL: 5432, SQL Server: 1433, DB2:
                        50000).
  -d DBNAME, --dbname DBNAME
                        The name of the database.
  -b BATCH, --batch BATCH
                        How many rows should be loaded at once.
  -s SEPARATOR, --separator SEPARATOR
                        The columns separator character(s).
  -q QUOTE, --quote QUOTE
                        The quote character on which a string won't be split.
  -a, --directpath      Execute a direct path INSERT load operation (Oracle
                        only).
  --truncate            Truncate/empty table before loading.
  -i, --ignore          Ignore erroneous/invalid lines in files and continue
                        the load.
  -l, --log             Log erroneous/invalid lines in *.bad file of the same
                        name as the input file (this implies the --ignore
                        option).
  --case-insensitive-identifiers
                        If set, all identifiers will be upper-cased.
  --quote-identifiers   If set, all table and column identifiers will be
                        quoted.
```

# How to use csv2db

## Loading CSV files into the database

`csv2db` can load plain text csv files as well as compressed csv files in `.zip` or `.gz` format without having to uncompress them first.

```bash
$ ./csv2db load -f test/resources/201811-citibike-tripdata.csv -t citibikes -u csv_data -p csv_data -d ORCLPDB1

Loading file test/resources/201811-citibike-tripdata.csv
File loaded.

$ ./csv2db load -f test/resources/201811-citibike-tripdata.csv.gz -t citibikes -u csv_data -p csv_data -d ORCLPDB1

Loading file test/resources/201811-citibike-tripdata.csv.gz
File loaded.

```

`csv2db` `--verbose` option will provide verbose output.

```bash
$ ./csv2db load -f test/resources/201811-citibike-tripdata.csv -t citibikes -u csv_data -p csv_data -d ORCLPDB1 --verbose
Finding file(s).
Found 1 file(s).
Establishing database connection.

Loading file test/resources/201811-citibike-tripdata.csv
16 rows loaded.
File loaded.

Closing database connection.
```

`csv2db` can load multiple files at once, using either wildcard characters (e.g. data*.csv.zip) or by passing on the folder containing CSV files.

***Note:** String including wildcard characters have to be enclosed in `""`*

```bash
$ ./csv2db load -f "test/resources/201811-citibike-tripdata.*" -t citibikes -u csv_data -p csv_data -d ORCLPDB1 --verbose
Finding file(s).
Found 3 file(s).
Establishing database connection.

Loading file test/resources/201811-citibike-tripdata.csv
16 rows loaded.
File loaded.


Loading file test/resources/201811-citibike-tripdata.csv.gz
10 rows loaded.
File loaded.


Loading file test/resources/201811-citibike-tripdata.csv.zip
10 rows loaded.
File loaded.

Closing database connection.
```

```bash
$ ./csv2db load -f test/resources -t citibikes -u csv_data -p csv_data -d ORCLPDB1 --verbose
Finding file(s).
Found 3 file(s).
Establishing database connection.

Loading file test/resources/201811-citibike-tripdata.csv
16 rows loaded.
File loaded.


Loading file test/resources/201811-citibike-tripdata.csv.gz
10 rows loaded.
File loaded.


Loading file test/resources/201811-citibike-tripdata.csv.zip
10 rows loaded.
File loaded.

Closing database connection.
```

`csv2db` will load all values as strings. You can either load all data into a staging table with all columns being strings as well, or rely on implicit data type conversion on the database side.

## Create a staging table

`csv2db` can generate the SQL statement for a staging table for your data using the `generate` command:

```sql
$ ./csv2db generate -f test/resources/201811-citibike-tripdata.csv
CREATE TABLE <TABLE NAME>
(
  TRIPDURATION VARCHAR(1000),
  STARTTIME VARCHAR(1000),
  STOPTIME VARCHAR(1000),
  START_STATION_ID VARCHAR(1000),
  START_STATION_NAME VARCHAR(1000),
  START_STATION_LATITUDE VARCHAR(1000),
  START_STATION_LONGITUDE VARCHAR(1000),
  END_STATION_ID VARCHAR(1000),
  END_STATION_NAME VARCHAR(1000),
  END_STATION_LATITUDE VARCHAR(1000),
  END_STATION_LONGITUDE VARCHAR(1000),
  BIKEID VARCHAR(1000),
  USERTYPE VARCHAR(1000),
  BIRTH_YEAR VARCHAR(1000),
  GENDER VARCHAR(1000)
);
```

By default, you will have to fill in the table name. You can also specify the table name via the `-t` option:

```sql
$ ./csv2db generate -f test/resources/201811-citibike-tripdata.csv -t STAGING
CREATE TABLE STAGING
(
  TRIPDURATION VARCHAR(1000),
  STARTTIME VARCHAR(1000),
  STOPTIME VARCHAR(1000),
  START_STATION_ID VARCHAR(1000),
  START_STATION_NAME VARCHAR(1000),
  START_STATION_LATITUDE VARCHAR(1000),
  START_STATION_LONGITUDE VARCHAR(1000),
  END_STATION_ID VARCHAR(1000),
  END_STATION_NAME VARCHAR(1000),
  END_STATION_LATITUDE VARCHAR(1000),
  END_STATION_LONGITUDE VARCHAR(1000),
  BIKEID VARCHAR(1000),
  USERTYPE VARCHAR(1000),
  BIRTH_YEAR VARCHAR(1000),
  GENDER VARCHAR(1000)
);
```

`csv2db` will use `VARCHAR(1000)` as default data type for all columns for the staging table. If you wish to use a different data type, you can specify it via the `-c` option:

```sql
$ ./csv2db generate -f test/resources/201811-citibike-tripdata.csv -t STAGING -c CLOB
CREATE TABLE STAGING
(
  TRIPDURATION CLOB,
  STARTTIME CLOB,
  STOPTIME CLOB,
  START_STATION_ID CLOB,
  START_STATION_NAME CLOB,
  START_STATION_LATITUDE CLOB,
  START_STATION_LONGITUDE CLOB,
  END_STATION_ID CLOB,
  END_STATION_NAME CLOB,
  END_STATION_LATITUDE CLOB,
  END_STATION_LONGITUDE CLOB,
  BIKEID CLOB,
  USERTYPE CLOB,
  BIRTH_YEAR CLOB,
  GENDER CLOB
);
```

The idea is to have a staging table that data can be loaded into first and then figure out the correct data types for each column.

# Installation

You can install `csv2db` either by installing it as a Python package,
which will automatically install all dependencies except the Db2 driver (as this one is still in Beta status)

```bash
$ python3 -m pip install csv2db
$ csv2db
```

or cloning this Git repository

```bash
$ git clone https://github.com/csv2db/csv2db
```

or by downloading one of the releases

```bash
$ LOCATION=$(curl -s https://api.github.com/repos/csv2db/csv2db/releases/latest | grep "tag_name" | awk '{print "https://github.com/csv2db/csv2db/archive/" substr($2, 2, length($2)-3) ".zip"}') ; curl -L -o csv2db.zip $LOCATION
$ unzip csv2db.zip
$ cd csv2db*
$ ./csv2db
```
    
In order for `csv2db` to work the appropriate database driver or drivers need to be installed.
This installation is done automatically when installing `csv2db` as a Python package (`pip install csv2db`).
The following drivers are being used, and are all available on [pypi.org](https://pypi.org/):

* Oracle: [oracledb](https://pypi.org/project/oracledb/) version 2.0.0+
* MySQL: [mysql-connector-python](https://pypi.org/project/mysql-connector-python/) version 8.0.13+
* PostgreSQL: [psycopg[binary]](https://pypi.org/project/psycopg-binary/) version 3.1.9+
* SQL Server: [pymssql](https://pypi.org/project/pymssql/) version 2.1.4+
* DB2: [ibm-db](https://pypi.org/project/ibm-db/) version 2.0.9+

You can install any of these drivers via `pip`:

```bash
$ python3 -m pip install oracledb
$ python3 -m pip install mysql-connector-python
$ python3 -m pip install psycopg[binary]
$ python3 -m pip install pymssql
$ python3 -m pip install ibm-db
```

For more instruction on how to install the driver(s) on your environment,
please see the documentation of the individual driver or refer to the
[csv2db Installation Guide](https://github.com/csv2db/csv2db/wiki/Installation-Guide).

**NOTE:** You only have to install the driver for the database(s) that you want to load data into.

# Miscellaneous

## What `csv2db` is and, more importantly, what it is not!
Since the very inception of `csv2db`, it has been a core principle for it not to become an ETL tool with all the bells and whistles.
There are already many very good ETL tools out there and the world doesn't need yet another one.
Instead, `csv2db` should aid users as a simple command-line tool to get rows from a delimited file into a database table, and not more!
Following that core design goal, `csv2db` will most likely never provide many database-specific options or parameters for the end-user to set,
it will not deal with explicit data type or character set conversion or globalization support that some databases offer.
If a user requires any of these features or more, he or she should look for one of the already existing ETL tools out there.

Simply put, `csv2db` does not do much more than taking rows from a delimited file and execute `INSERT INTO` statements with the values of these rows.
It is there to help users to get the contents of a file into a database table quickly where the data can then be further processed.

## Using the `csv2db` Docker image
`csv2db` is also offered as a Docker image, making the usage of `csv2db` quick and easy without requiring any install.

To run `csv2db`, simply use the `gvenzl/csv2db` image invoking the `docker|podman run` command, for example:

```bash
# quick test the image
podman run --rm gvenzl/csv2db --help
```

To load data, bind the folder containing the input files as a Docker volume:
```bash
podman run --rm -v <input files folder>:/input/ gvenzl/csv2db load -f /input/<input file(s)> ...
```

For example:
```bash
podman run --rm -v $HOME/input_files:/input gvenzl/csv2db \
  load -f /input/201811-citibike-tripdata.csv -t citibikes \
  -u db_user -p db_pass -d my_db
```

## Exit codes
`csv2db` returns following exit codes:  

| Exit code             | Value | Meaning                                                                                          |
|-----------------------|:-----:|--------------------------------------------------------------------------------------------------|
| `SUCCESS`             |   0   | Successful execution of the program.                                                             |
| `GENERIC_ERROR`       |   1   | A generic error occurred.                                                                        |
| `ARGUMENT_ERROR`      |   2   | An argument is either missing or incorrect.                                                      |
| `DATABASE_ERROR`      |   3   | A database error occurred.                                                                       |
| `DATA_LOADING_ERROR`  |   4   | An error occurred during loading of data. `csv2db` will continue to process other files, if any. |

## `$NO_COLOR` support
`csv2db` is capable of color coded output and will do so by default (except on Windows).  
<span style="color:yellow">Debug output is yellow.</span>  
<span style="color:red">Error output is red.</span>  
This can be deactivated by setting the `$NO_COLOR` environment variable. For more details on `$NO_COLOR` see https://no-color.org/

# LICENSE

	Copyright 2024 Gerald Venzl
	
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
