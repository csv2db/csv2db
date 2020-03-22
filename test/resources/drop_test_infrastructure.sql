--
-- Since: March, 2020
-- Author: gvenzl
-- Name: drop_test_infrastructure.sql
-- Description: SQL scripts for test infrastructure deletion
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
-- TABLES
--

-- STAGING table (for user TEST)

DROP TABLE STAGING;

-- LOCATIONS table (for user TEST)

DROP TABLE LOCATIONS;


--
-- DATABASE AND USER
--

-- MySQL
DROP DATABASE test;
DROP USER 'test';
FLUSH PRIVILEGES;

-- Postgres
DROP DATABASE test;
DROP USER test;

-- Oracle
ALTER PLUGGABLE DATABASE test CLOSE;
DROP PLUGGABLE DATABASE test INCLUDING DATAFILES;

-- SQL Server
DROP DATABASE test;

-- Db2
CONNECT TO test
QUIESCE DATABASE IMMEDIATE FORCE CONNECTIONS
UNQUIESCE DATABASE
CONNECT RESET
DEACTIVATE DATABASE test
DROP DATABASE test
