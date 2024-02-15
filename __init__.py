from dmf.connectors.mssql import MSSQLConnector
from dmf.connectors.oracle import OracleConnector

# This dictionary stores database connectors for different database systems.

databases = {"oracle19c": OracleConnector, "mssql": MSSQLConnector}
