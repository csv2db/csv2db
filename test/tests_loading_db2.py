#
#  Since: June 2023
#  Author: gvenzl
#  Name: tests_loading_db2.py
#  Description:loading tests for DB2
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

import unittest
import tests_loading as base


class LoadingTestsDb2Suite(base.LoadingTestsSuite):
    def __init__(self, *args, **kwargs):
        super(LoadingTestsDb2Suite, self).__init__(*args, **kwargs)
        self.params["db_type"] = "db2"
        self.params["user"] = "db2inst1"


if __name__ == '__main__':
    unittest.main(verbosity=2)
