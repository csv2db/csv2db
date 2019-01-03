# csv2db
A loader for CSV files.

## Usage
    usage: csv2db [-h] [-f FILE] [-o DBTYPE] [-u USER] [-p PASSWORD] [-m HOST]
                  [-n PORT] [-d DBNAME] [-b BATCH] [-t TABLE] [-g]
                  [-c COLUMN_TYPE] [-v]
    
    A loader for CSV files.
    
    optional arguments:
      -h, --help            show this help message and exit
      -f FILE, --file FILE  The file to load, by default all *.csv.zip files
      -o DBTYPE, --dbtype DBTYPE
                            The database type. Choose one of ['oracle', 'mysql',
                            'postgres', 'sqlserver'],
      -u USER, --user USER  The database user to load data into
      -p PASSWORD, --password PASSWORD
                            The database schema password
      -m HOST, --host HOST  The host name on which the database is running on
      -n PORT, --port PORT  The port on which the database is listening
      -d DBNAME, --dbname DBNAME
                            The name of the database
      -b BATCH, --batch BATCH
                            How many rows should be loaded at once.
      -t TABLE, --table TABLE
                            The table to load data into.
      -g, --generate        Generates the table and columns based on the header
                            row of the CSV file.
      -c COLUMN_TYPE, --column-type COLUMN_TYPE
                            The column type to use for the table generation
      -v, --verbose         Verbose output.
