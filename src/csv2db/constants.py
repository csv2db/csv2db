#
#  Since: October 2023
#  Author: gvenzl
#  Name: constants.py
#  Description: Global constants to be used.
#
#  Copyright 2023 Gerald Venzl
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from enum import Enum


class DBType(Enum):
    """Database type enumeration."""
    ORACLE = "oracle"
    MYSQL = "mysql"
    POSTGRES = "postgres"
    SQLSERVER = "sqlserver"
    DB2 = "db2"


class ExitCodes(Enum):
    """Program return code enumeration."""
    SUCCESS = 0
    GENERIC_ERROR = 1
    DATABASE_ERROR = 3  # value 2 is reserved for wrong arguments passed via argparse
    DATA_LOADING_ERROR = 4


class TerminalColor(Enum):
    GREEN = "\x1b[32m"
    RED = "\x1b[31m"
    YELLOW = "\x1b[33m"
    RESET = "\x1b[0m"


class DBConfigKeys(str, Enum):
    IDENTIFIER_QUOTE = "identifier_quote"


DBConfig = {
    DBConfigKeys.IDENTIFIER_QUOTE: {
        DBType.MYSQL:     "`",
        DBType.ORACLE:    '"',
        DBType.POSTGRES:  '"',
        DBType.SQLSERVER: '"',
        DBType.DB2:       '"'
    }
}
