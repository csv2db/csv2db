# csv2db
A loader for CSV files.

`csv2db` takes CSV files and loads them into a database.
Rather than having to potentially go through TBs of CSV data to find out what columns and data types are present in the CSV files,
`csv2db` will read the header in each CSV file and automatically load data into the columns of the same name into the target table.
Spaces in the header column names are automatically replaced with `_` characters,
for example the column `station id` in the CSV file will be interpreted as `station_id` column in the table.

This approach allows you to get data into the database first and worry about the data cleansing part later,
which is usually much easier once the data is in the database than in the CSV files.

`csv2db` is capable of scanning all CSV file headers at once and derive a `CREATE TABLE` statement with all the column names from.
This is particularly useful if the format of the CSV files has changed over time or because you want to load different CSV file types into the same database table.

## Usage

	$ ./csv2db -h
	usage: csv2db [-h] {generate,gen,load,lo} ...
	
	A loader for CSV files.
	
	positional arguments:
	  {generate,gen,load,lo}
	    generate (gen)      Prints a CREATE TABLE SQL statement to create the
	                        table and columns based on the header row of the CSV
	                        file(s).
	    load (lo)           Loads the data from the CSV file(s) into the database.
	
	optional arguments:
	  -h, --help            show this help message and exit


	$ ./csv2db generate -h
	usage: csv2db generate [-h] [-f FILE] [-v] [--debug] [-t TABLE]
	                       [-c COLUMN_TYPE]
	
	optional arguments:
	  -h, --help            show this help message and exit
	  -f FILE, --file FILE  The file to load, by default all *.csv.zip files
	  -v, --verbose         Verbose output.
	  --debug               Debug output.
	  -t TABLE, --table TABLE
	                        The table name to use.
	  -c COLUMN_TYPE, --column-type COLUMN_TYPE
	                        The column type to use for the table generation.


	$ ./csv2db load -h
	usage: csv2db load [-h] [-f FILE] [-v] [--debug] [-t TABLE] [-o DBTYPE]
	                   [-u USER] [-p PASSWORD] [-m HOST] [-n PORT] [-d DBNAME]
	                   [-b BATCH]
	
	optional arguments:
	  -h, --help            show this help message and exit
	  -f FILE, --file FILE  The file to load, by default all *.csv.zip files
	  -v, --verbose         Verbose output.
	  --debug               Debug output.
	  -t TABLE, --table TABLE
	                        The table name to use.
	  -o DBTYPE, --dbtype DBTYPE
	                        The database type. Choose one of ['oracle', 'mysql', 'postgres'].
	  -u USER, --user USER  The database user to load data into.
	  -p PASSWORD, --password PASSWORD
	                        The database schema password.
	  -m HOST, --host HOST  The host name on which the database is running on.
	  -n PORT, --port PORT  The port on which the database is listening.
	  -d DBNAME, --dbname DBNAME
	                        The name of the database.
	  -b BATCH, --batch BATCH
	                        How many rows should be loaded at once.
	$

## How to use csv2db

### Loading CSV files into the database

`csv2db` can load uncompressed and compressed csv files in `.zip` or `.gz` format without having to uncompress them first.

```bash
$ ./csv2db load -f test/resources/201811-citibike-tripdata.csv -t citibikes -u csv_data -p csv_data -d ORCLPDB1

Loading file test/resources/201811-citibike-tripdata.csv
Done

$ ./csv2db load -f test/resources/201811-citibike-tripdata.csv.gz -t citibikes -u csv_data -p csv_data -d ORCLPDB1

Loading file test/resources/201811-citibike-tripdata.csv.gz
Done
```

`csv2db` `--verbose` option will provide verbose output.

```bash
$ ./csv2db load -f test/resources/201811-citibike-tripdata.csv -t citibikes -u csv_data -p csv_data -d ORCLPDB1 --verbose
Finding file(s).
Establishing database connection.

Loading file test/resources/201811-citibike-tripdata.csv
10 rows loaded
Done

Closing database connection.
```

`csv2db` can load multiple files at once, using either wildcard characters (e.g. data*.csv.zip) or by passing on the folder containing CSV files.

**Note:** Wildcart charaters will have to be enclosed in `""`

```bash
$ ./csv2db load -f "test/resources/2018*" -t citibikes -u csv_data -p csv_data -d ORCLPDB1 --verbose
Finding file(s).
Establishing database connection.

Loading file test/resources/201811-citibike-tripdata.csv
10 rows loaded
Done


Loading file test/resources/201811-citibike-tripdata.csv.gz
10 rows loaded
Done


Loading file test/resources/201811-citibike-tripdata.csv.zip
10 rows loaded
Done

Closing database connection.
```

```bash
$ ./csv2db load -f test/resources -t citibikes -u csv_data -p csv_data -d ORCLPDB1 --verbose
Finding file(s).
Establishing database connection.

Loading file test/resources/201811-citibike-tripdata.csv
10 rows loaded
Done


Loading file test/resources/201811-citibike-tripdata.csv.gz
10 rows loaded
Done


Loading file test/resources/201811-citibike-tripdata.csv.zip
10 rows loaded
Done

Closing database connection.
```

`csv2db` will load all values as strings. You can either load all data into a staging table with all columns being strings as well, or rely on implicit data type converion on the database side.

### Create a staging table

`csv2db` can generate the SQL statement for a staging table for your data using the `generate` command:

```sql
$ ./csv2db generate -f test/resources/201811-citibike-tripdata.csv
CREATE TABLE <TABLE NAME>
(
  END_STATION_ID VARCHAR2(4000),
  STOPTIME VARCHAR2(4000),
  START_STATION_LATITUDE VARCHAR2(4000),
  GENDER VARCHAR2(4000),
  END_STATION_LATITUDE VARCHAR2(4000),
  BIRTH_YEAR VARCHAR2(4000),
  START_STATION_ID VARCHAR2(4000),
  START_STATION_NAME VARCHAR2(4000),
  STARTTIME VARCHAR2(4000),
  USERTYPE VARCHAR2(4000),
  END_STATION_LONGITUDE VARCHAR2(4000),
  END_STATION_NAME VARCHAR2(4000),
  BIKEID VARCHAR2(4000),
  TRIPDURATION VARCHAR2(4000),
  START_STATION_LONGITUDE VARCHAR2(4000)
);
```

By default you will have to fill in the table name. You can also specify the table name via the `-t` option:

```sql
$ ./csv2db generate -f test/resources/201811-citibike-tripdata.csv -t STAGING
CREATE TABLE STAGING
(
  END_STATION_LATITUDE VARCHAR2(4000),
  GENDER VARCHAR2(4000),
  START_STATION_LATITUDE VARCHAR2(4000),
  END_STATION_NAME VARCHAR2(4000),
  BIRTH_YEAR VARCHAR2(4000),
  START_STATION_NAME VARCHAR2(4000),
  STOPTIME VARCHAR2(4000),
  END_STATION_ID VARCHAR2(4000),
  STARTTIME VARCHAR2(4000),
  START_STATION_LONGITUDE VARCHAR2(4000),
  START_STATION_ID VARCHAR2(4000),
  BIKEID VARCHAR2(4000),
  USERTYPE VARCHAR2(4000),
  TRIPDURATION VARCHAR2(4000),
  END_STATION_LONGITUDE VARCHAR2(4000)
);
```

`csv2db` will use `VARCHAR2(4000)` as default data type for all columns for the staging table. If you wish to use a different data type, you can specify it via the `-c` option:

```sql
$ ./csv2db generate -f test/resources/201811-citibike-tripdata.csv -t STAGING -c CLOB
CREATE TABLE STAGING
(
  START_STATION_ID CLOB,
  END_STATION_ID CLOB,
  START_STATION_NAME CLOB,
  STARTTIME CLOB,
  START_STATION_LONGITUDE CLOB,
  END_STATION_LONGITUDE CLOB,
  GENDER CLOB,
  BIKEID CLOB,
  USERTYPE CLOB,
  TRIPDURATION CLOB,
  START_STATION_LATITUDE CLOB,
  BIRTH_YEAR CLOB,
  END_STATION_NAME CLOB,
  STOPTIME CLOB,
  END_STATION_LATITUDE CLOB
);
```

The idea is that you have a staging table that you can load data into and then figure out the correct data types for each column.

## Installation

You can install `csv2db` either by cloning this Git repository

    git clone https://github.com/gvenzl/csv2db

or by downloading one of the releases

    wget https://github.com/gvenzl/csv2db/releases/latest
    unzip csv2db.zip
    cd csv2db
    
In order for `csv2db` to work you will have to install the appropriate database driver(s).
The following drivers are being used, all available on [pypi.org](https://pypi.org/):

* Oracle: [cx_Oracle](https://pypi.org/project/cx_Oracle/) version 7.0.0+
* MySQL: [mysql-connector-python](https://pypi.org/project/mysql-connector-python/) version 8.0.13+
* PostgreSQL: [psycopg2-binary](https://pypi.org/project/psycopg2-binary/) version 2.7.6.1+

You can install any of these drivers via `pip`:

    pip install cx_Oracle
    pip install mysql-connector-python
    pip install psycopg2-binary

**NOTE:** You only have to install the driver for the database(s) that you want to load the data into.

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