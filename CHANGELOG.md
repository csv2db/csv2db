# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Support for user-defined column separator

### Fixed
- Don't execute batch flush on emtpy data set (bug #6)

## [1.1.1] - 2019-02-09

### Fixed
- Get correct source path for shell wrapper (bug #4)

## [1.1.0] - 2019-01-16

### Added
- Db2 LUW support
- Version numbering in program help

### Changed
- Set default port for all database types (Oracle: 1521, MySQL: 3306, PostgreSQL: 5432, Db2: 50000).
