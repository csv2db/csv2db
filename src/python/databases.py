from abc import ABCMeta, abstractmethod

import functions as f

class DatabaseType:
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_colum_type(self, column_type: str) -> str: raise NotImplementedError

class MySQL(DatabaseType):
    def get_colum_type(self, column_type: str) -> str:
        if column_type == f.ColumnTypes.INTEGER.value:
            return "BIGINT"

        if column_type == f.ColumnTypes.FLOAT.value:
            # Attention: Precision is not 100% given here.
            # Read https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
            return "FLOAT"

        # column_type == f.ColumnTypes.STRING.value
        return "VARCHAR(1000)"

# TODO Implement Oracle database type
# TODO Implement Postgres database type
# TODO Implement SQL Server database type
# TODO Implement DB2 database type