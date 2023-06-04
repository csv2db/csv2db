--
-- Since: October 2022
-- Author: gvenzl
-- Name: setup_schema.sql
-- Description: Table creation SQL scripts for load tests.
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

CREATE TABLE STAGING
(
  TRIPDURATION            INTEGER,
  STARTTIME               VARCHAR(255),
  STOPTIME                VARCHAR(255),
  START_STATION_ID        INTEGER,
  START_STATION_NAME      VARCHAR(255),
  START_STATION_LATITUDE  NUMERIC(18,14),
  START_STATION_LONGITUDE NUMERIC(18,14),
  END_STATION_ID          INTEGER,
  END_STATION_NAME        VARCHAR(255),
  END_STATION_LATITUDE    NUMERIC(18,14),
  END_STATION_LONGITUDE   NUMERIC(18,14),
  BIKEID                  INTEGER,
  USERTYPE                VARCHAR(255),
  BIRTH_YEAR              NUMERIC(4),
  GENDER                  NUMERIC(1)
);

CREATE TABLE LOCATIONS
(
  GEONAMEID         VARCHAR(255),
  NAME              VARCHAR(255),
  ASCIINAME         VARCHAR(255),
  ALTERNATENAMES    VARCHAR(1200),
  LATITUDE          VARCHAR(255),
  LONGITUDE         VARCHAR(255),
  FEATURE_CLASS     VARCHAR(255),
  FEATURE_CODE      VARCHAR(255),
  COUNTRY_CODE      VARCHAR(255),
  CC2               VARCHAR(255),
  ADMIN1_CODE       VARCHAR(255),
  ADMIN2_CODE       VARCHAR(255),
  ADMIN3_CODE       VARCHAR(255),
  ADMIN4_CODE       VARCHAR(255),
  POPULATION        VARCHAR(255),
  ELEVATION         VARCHAR(255),
  DEM               VARCHAR(255),
  TIMEZONE          VARCHAR(255),
  MODIFICATION_DATE VARCHAR(255)
);
