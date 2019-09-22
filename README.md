```
                  ___       ____  
  ___________   _|__ \ ____/ / /_ 
 / ___/ ___/ | / /_/ // __  / __ \
/ /__(__  )| |/ / __// /_/ / /_/ /
\___/____/ |___/____/\____/_____/ 
                                  
```


The CSV command line loader.

`csv2db` takes CSV files and loads them into a database.
Rather than having to go through the CSV data first and find out what columns and data types are present in the CSV files,
`csv2db` will read the header in each CSV file and automatically load data into the columns of the same name into the target table.
Spaces in the header column names are automatically replaced with `_` characters,
for example the column `station id` in the CSV file will be interpreted as `station_id` column in the table.

This approach allows you to get data into the database first and worry about the data cleansing part later,
which is usually much easier once the data is in the database rather than in the CSV files.

`csv2db` is capable of scanning all CSV file headers at once and generate a `CREATE TABLE` statement with all the column names present.
This is particularly useful if the format of the CSV files has changed over time or because you want to load different CSV file types into the same database table.

# Usage

```console
$ ./csv2db -h
usage: csv2db [-h] {generate,gen,load,lo} ...

The CSV command line loader.
Version: 1.4.0
(c) Gerald Venzl

positional arguments:
  {generate,gen,load,lo}
    generate (gen)      Prints a CREATE TABLE SQL statement to create the
                        table and columns based on the header row of the CSV
                        file(s).
    load (lo)           Loads the data from the CSV file(s) into the database.

optional arguments:
  -h, --help            show this help message and exit
```

```console
$ ./csv2db generate -h
usage: csv2db generate [-h] [-f FILE] [-v] [--debug] [-t TABLE]
                       [-c COLUMN_TYPE] [-s SEPARATOR] [-q QUOTE]

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  The file to load, by default all *.csv.zip files
  -v, --verbose         Verbose output.
  --debug               Debug output.
  -t TABLE, --table TABLE
                        The table name to use.
  -c COLUMN_TYPE, --column-type COLUMN_TYPE
                        The column type to use for the table generation.
  -s SEPARATOR, --separator SEPARATOR
                        The columns separator character(s).
  -q QUOTE, --quote QUOTE
                        The quote character on which a string won't be split.
```

```console
$ ./csv2db load -h
usage: csv2db load [-h] [-f FILE] [-v] [--debug] -t TABLE
                   [-o {oracle,mysql,postgres,sqlserver,db2}] -u USER -p
                   PASSWORD [-m HOST] [-n PORT] [-d DBNAME] [-b BATCH]
                   [-s SEPARATOR] [-q QUOTE] [-a]

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  The file to load, by default all *.csv.zip files
  -v, --verbose         Verbose output.
  --debug               Debug output.
  -t TABLE, --table TABLE
                        The table name to use.
  -o {oracle,mysql,postgres,sqlserver,db2}, --dbtype {oracle,mysql,postgres,sqlserver,db2}
                        The database type.
  -u USER, --user USER  The database user to load data into.
  -p PASSWORD, --password PASSWORD
                        The database schema password.
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
```

# How to use csv2db

## Loading CSV files into the database

`csv2db` can load plain text csv files as well as compressed csv files in `.zip` or `.gz` format without having to uncompress them first.

```console
$ ./csv2db load -f test/resources/201811-citibike-tripdata.csv -t citibikes -u csv_data -p csv_data -d ORCLPDB1

Loading file test/resources/201811-citibike-tripdata.csv
File loaded.

$ ./csv2db load -f test/resources/201811-citibike-tripdata.csv.gz -t citibikes -u csv_data -p csv_data -d ORCLPDB1

Loading file test/resources/201811-citibike-tripdata.csv.gz
File loaded.

```

`csv2db` `--verbose` option will provide verbose output.

```console
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

```console
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

```console
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

By default you will have to fill in the table name. You can also specify the table name via the `-t` option:

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

You can install `csv2db` either by cloning this Git repository

```console
$ git clone https://github.com/csv2db/csv2db
```

or by downloading one of the releases

```console
$ LOCATION=$(curl -s https://api.github.com/repos/csv2db/csv2db/releases/latest | grep "tag_name" | awk '{print "https://github.com/csv2db/csv2db/archive/" substr($2, 2, length($2)-3) ".zip"}') ; curl -L -o csv2db.zip $LOCATION
$ unzip csv2db.zip
$ cd csv2db*
$ ./csv2db
```
    
In order for `csv2db` to work you will have to install the appropriate database driver(s).
The following drivers are being used, all available on [pypi.org](https://pypi.org/):

* Oracle: [cx_Oracle](https://pypi.org/project/cx_Oracle/) version 7.0.0+
* MySQL: [mysql-connector-python](https://pypi.org/project/mysql-connector-python/) version 8.0.13+
* PostgreSQL: [psycopg2-binary](https://pypi.org/project/psycopg2-binary/) version 2.7.6.1+
* SQL Server: [pymssql](https://pypi.org/project/pymssql/) version 2.1.4+
* DB2: [ibm_db](https://pypi.org/project/ibm_db/) version 2.0.9+

You can install any of these drivers via `pip`:

```console
$ python3 -m pip install cx_Oracle
$ python3 -m pip install mysql-connector-python
$ python3 -m pip install psycopg2-binary
$ python3 -m pip install pymssql
$ python3 -m pip install ibm_db
```

For more instruction on how to install the driver(s) on your environment,
please see the documentation of the individual driver or refer to the
[csv2db Installation Guide](https://github.com/csv2db/csv2db/wiki/Installation-Guide).

**NOTE:** You only have to install the driver for the database(s) that you want to load data into.

# Miscellaneous

## Exit codes
`csv2db` returns following exit codes:  

Exit code          | Value | Meaning
------------------ | ----- | -------
SUCCESS            |     0 | Successful execution of the program.
GENERIC_ERROR      |     1 | A generic error occurred.
ARGUMENT_ERROR     |     2 | An argument is either missing or incorrect.
DATABASE_ERROR     |     3 | A database error occurred.
DATA_LOADING_ERROR |     4 | An error occurred during loading of data. `csv2db` will continue to process other files, if any.

## `$NO_COLOR` support
`csv2db` is capable of color coded output and will do so by default.  
<span style="color:yellow">Debug output is yellow.</span>  
<span style="color:red">Error output is red.</span>  
This can be deactivated by setting the `$NO_COLOR` environment variable. For more details on `$NO_COLOR` see https://no-color.org/

# LICENSE

	Copyright 2019 Gerald Venzl
	
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
