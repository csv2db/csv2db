--
-- Since: August 2021
-- Author: gvenzl
-- Name: create_db_sqlserver.sql
-- Description: SQL scripts for test infrastructure creation for SQL Server
--
-- Copyright 2021 Gerald Venzl
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

sp_configure 'contained database authentication', 1;
RECONFIGURE;
go
CREATE DATABASE test CONTAINMENT=PARTIAL;
go
USE test;
CREATE USER test WITH PASSWORD = 'LetsTest1';
go
ALTER ROLE db_owner ADD MEMBER test;
go
