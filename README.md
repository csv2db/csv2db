# csv2db
A loader for CSV files.

`csv2db` takes CSV files and loads them into a database.
Rather than having to potentially go through TBs of CSV data to find out what columns and data types are present in the CSV files,
`csv2db` will read the header in each CSV file and automatically load data into the columns of the same name into the target table.
Spaces will automatically be replaced with `_` characters,
for example the column `station id` in the CSV file will be interpreted as `station_id` column in the table.
This approach allows you to get data into the database first and worry about the data cleansing part later,
which is usually much easier once the data is in the database than in the CSV files.

`csv2db` is capable of scanning all CSV file headers at once and derive a `CREATE TABLE` statement with all the column names from.
This is particularly useful if the format of the CSV files has changed over time,
either because of historical reasons or because you want to load different CSV file types into the same database table.

## Usage
    $ ./csv2db.py -h
    usage: csv2db [-h] [-f FILE] [-v] [-t TABLE] {generate,gen,load,lo} ...
    
    A loader for CSV files.
    
    positional arguments:
      {generate,gen,load,lo}
        generate (gen)      Prints a CREATE TABLE SQL statement to create the
                            table and columns based on the header row of the CSV
                            file(s).
        load (lo)           Loads the data from the CSV file(s) into the database.
    
    optional arguments:
      -h, --help            show this help message and exit
      -f FILE, --file FILE  The file to load, by default all *.csv.zip files
      -v, --verbose         Verbose output.
      -t TABLE, --table TABLE
                            The table name to use.
    gvenzl-mac:python gvenzl$ ./csv2db.py generate -h
    usage: csv2db generate [-h] [-c COLUMN_TYPE]
    
    optional arguments:
      -h, --help            show this help message and exit
      -c COLUMN_TYPE, --column-type COLUMN_TYPE
                            The column type to use for the table generation.
    gvenzl-mac:python gvenzl$ ./csv2db.py load -h
    usage: csv2db load [-h] [-o DBTYPE] [-u USER] [-p PASSWORD] [-m HOST]
                       [-n PORT] [-d DBNAME] [-b BATCH]
    
    optional arguments:
      -h, --help            show this help message and exit
      -o DBTYPE, --dbtype DBTYPE
                            The database type. Choose one of ['oracle', 'mysql',
                            'postgres', 'sqlserver'].
      -u USER, --user USER  The database user to load data into.
      -p PASSWORD, --password PASSWORD
                            The database schema password.
      -m HOST, --host HOST  The host name on which the database is running on.
      -n PORT, --port PORT  The port on which the database is listening.
      -d DBNAME, --dbname DBNAME
                            The name of the database.
      -b BATCH, --batch BATCH
                            How many rows should be loaded at once.

## How to use csv2db

### Create a staging table