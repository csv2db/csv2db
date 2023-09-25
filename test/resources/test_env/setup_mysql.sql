--
-- Since: October 2022
-- Author: gvenzl
-- Name: setup_mysql.sql
-- Description: SQL scripts for test infrastructure creation for MySQL
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

CREATE DATABASE test;
CREATE USER 'test' IDENTIFIED BY 'L3tsT3stTh1s++';
GRANT ALL PRIVILEGES ON test.* TO 'test';
FLUSH PRIVILEGES;
exit;
