--
-- Since: March, 2020
-- Author: gvenzl
-- Name: create_test_infrastructure.sql
-- Description: SQL scripts for test infrastructure creation
--
-- Copyright 2020 Gerald Venzl
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

--
-- DATABASE AND USER
--

-- MySQL
CREATE DATABASE test;
CREATE USER 'test' IDENTIFIED BY 'LetsDocker1';
GRANT ALL PRIVILEGES ON test.* TO 'test';
FLUSH PRIVILEGES;

-- Postgres
CREATE DATABASE test;
CREATE USER test WITH ENCRYPTED PASSWORD 'LetsDocker1';
GRANT ALL PRIVILEGES ON DATABASE test TO test;

-- Oracle
CREATE PLUGGABLE DATABASE test ADMIN USER test IDENTIFIED BY LetsDocker1 FILE_NAME_CONVERT=('/opt/oracle/oradata/XE/pdbseed','/opt/oracle/oradata/XE/test');
ALTER PLUGGABLE DATABASE test OPEN;
ALTER PLUGGABLE DATABASE test SAVE STATE;
ALTER SESSION SET CONTAINER=test;
GRANT CONNECT, RESOURCE, CREATE VIEW, UNLIMITED TABLESPACE TO test;

-- SQL Server
sp_configure 'contained database authentication', 1;
RECONFIGURE;
go
CREATE DATABASE test CONTAINMENT=PARTIAL;
go
USE test;
CREATE USER test WITH PASSWORD = 'LetsDocker1';
go
ALTER ROLE db_owner ADD MEMBER test;
go

-- Db2
CREATE DATABASE test

--
-- TABLES
--

-- STAGING table (for user TEST)

CREATE TABLE STAGING
(
  TRIPDURATION VARCHAR(255),
  STARTTIME VARCHAR(255),
  STOPTIME VARCHAR(255),
  START_STATION_ID VARCHAR(255),
  START_STATION_NAME VARCHAR(255),
  START_STATION_LATITUDE VARCHAR(255),
  START_STATION_LONGITUDE VARCHAR(255),
  END_STATION_ID VARCHAR(255),
  END_STATION_NAME VARCHAR(255),
  END_STATION_LATITUDE VARCHAR(255),
  END_STATION_LONGITUDE VARCHAR(255),
  BIKEID VARCHAR(255),
  USERTYPE VARCHAR(255),
  BIRTH_YEAR VARCHAR(255),
  GENDER VARCHAR(255)
);

-- LOCATIONS table (for user TEST)

CREATE TABLE LOCATIONS
(
  GEONAMEID VARCHAR(255),
  NAME VARCHAR(255),
  ASCIINAME VARCHAR(255),
  ALTERNATENAMES TEXT, --CLOB,
  LATITUDE VARCHAR(255),
  LONGITUDE VARCHAR(255),
  FEATURE_CLASS VARCHAR(255),
  FEATURE_CODE VARCHAR(255),
  COUNTRY_CODE VARCHAR(255),
  CC2 VARCHAR(255),
  ADMIN1_CODE VARCHAR(255),
  ADMIN2_CODE VARCHAR(255),
  ADMIN3_CODE VARCHAR(255),
  ADMIN4_CODE VARCHAR(255),
  POPULATION VARCHAR(255),
  ELEVATION VARCHAR(255),
  DEM VARCHAR(255),
  TIMEZONE VARCHAR(255),
  MODIFICATION_DATE VARCHAR(255)
);
