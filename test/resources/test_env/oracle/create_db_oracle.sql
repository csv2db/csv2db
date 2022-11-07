--
-- Since: November 2022
-- Author: gvenzl
-- Name: create_db_oracle.sql
-- Description: SQL scripts for test infrastructure creation for Oracle
--
-- Copyright 2022 Gerald Venzl
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

CREATE PLUGGABLE DATABASE test ADMIN USER test IDENTIFIED BY LetsDocker1 FILE_NAME_CONVERT=('/opt/oracle/oradata/XE/pdbseed','/opt/oracle/oradata/XE/test');
ALTER PLUGGABLE DATABASE test OPEN;
ALTER PLUGGABLE DATABASE test SAVE STATE;
ALTER SESSION SET CONTAINER=test;
GRANT CONNECT, RESOURCE, CREATE VIEW, UNLIMITED TABLESPACE TO test;
