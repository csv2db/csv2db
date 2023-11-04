@ECHO OFF
REM
REM Since: November, 2019
REM Author: gvenzl
REM Name: csv2db
REM Description: csv2db command prompt wrapper
REM
REM Copyright 2019 Gerald Venzl
REM
REM Licensed under the Apache License, Version 2.0 (the "License");
REM you may not use this file except in compliance with the License.
REM You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.
REM

REM Check whether python 3 is available
WHERE python.exe >nul 2>nul
IF %ERRORLEVEL% == 0 (
   python.exe %~dp0.\src\main.py %*
) ELSE (
  ECHO Python is not installed, please install Python 3 first.
  ECHO For more information see https://www.python.org/downloads/
)
