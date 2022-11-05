# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- New option `--log` to log erroneous lines in a `*.bad` file of the same name (ER #46) 
- New option `--ignore` to ignore errors in file and continue loading (ER #46)
- New option `--truncate` to empty (truncate) the target table before the load (ER #45)

### Changed
- Read all input files with `UTF-8` encoding
- Use oracledb driver for Oracle Database

## [1.5.1] 2020-03-21

### Added
- Added loading tests for all databases
- Enhanced error reporting (ER #37)

### Changed
- Added `PROTOCOL=TCPIP` and `AUTHENTICATION=SERVER` to the Db2 connect string.
- Using `psycopg2.extras.execute_batch()` to speed up data load with Postgres (ER #39)

### Fixed
- Convert row to tuple on array append (bug #35)
- Use `UTF-8` connection encoding for Oracle (bug #36)

## [1.5.0] 2020-03-14

### Added
- Interactive password entry (PR #33)

## [1.4.2] 2020-01-26

### Changed
- Change csv2db description from "the CSV command line loader" to "the CSV to database command line loader"

## [1.4.1] 2020-01-25

### Added
- Introduce Windows .bat file wrapper

### Fixed
- Don't print color codes on Windows (bug #29)

## [1.4.0] - 2019-09-22

### Added
- Introduce csv2db icon
- Check for mandatory command line arguments (ER #14)
- Exit program immediately if no files were found (ER #17)
- More debug statements for file reading operations (ER #15)
- Color coded output (can be turned off via $NO_COLOR) (ER #20)
- Provide failing data record as debug output (ER #19)
- More precise status output (ER #24)
- Change default data type to VARCHAR(1000) (ER #26)
- Return code to indicate data loading issue (ER #25)
- Introduce SQL Server support (ER #11)

### Fixed
- Pass correct exit return codes (bug #18)
- Autocommit is explicitly turned off (bug #22)
- Record list cleared on error (bug #23)
- Preserve correct order of column list (bug #21)

## [1.3.1] - 2019-07-28

### Fixed
- Put `/*+ APPEND_VALUES */` hint at right position immediately after the `INSERT` keyword (bug #12)

## [1.3.0] - 2019-07-28

### Added
- Provide support for direct-path loading with Oracle DB (ER #8)
- Provide support for custom quote characters (ER #10)

## [1.2.0] - 2019-03-09

### Added
- Support for user-defined column separator

### Fixed
- Don't execute batch flush on empty data set (bug #6)

## [1.1.1] - 2019-02-09

### Fixed
- Get correct source path for shell wrapper (bug #4)

## [1.1.0] - 2019-01-16

### Added
- Db2 LUW support
- Version numbering in program help

### Changed
- Set default port for all database types (Oracle: 1521, MySQL: 3306, PostgreSQL: 5432, Db2: 50000).
