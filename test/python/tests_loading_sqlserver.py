#
#  Since: December 2022
#  Author: gvenzl
#  Name: tests_loading_sqlserver.py
#  Description: loading tests for SQL Server
#
#  Copyright 2022 Gerald Venzl
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

import unittest
import tests_loading as base


class LoadingTestsOracleSuite(base.LoadingTestsSuite):
    def __init__(self, *args, **kwargs):
        super(LoadingTestsOracleSuite, self).__init__(*args, **kwargs)
        self.params["db_type"] = "sqlserver"


if __name__ == '__main__':
    unittest.main(verbosity=2)
