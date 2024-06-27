#!/usr/bin/env python

"""
Lists all the INSERT, UPDATE or DELETE transaction.
"""

import struct

import pyodbc

SERVER = '192.168.122.7'
DATABASE = 'TestDB'
USERNAME = 'sa'
PASSWORD = 'RedHat12345'
CONN_STRING = (f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};'
               f'DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=no')

conn = pyodbc.connect(CONN_STRING)
cursor = conn.cursor()

query = """
SELECT DISTINCT [Transaction ID] AS tx_id FROM fn_dblog(NULL, NULL) 
where Operation IN ('LOP_INSERT_ROWS', 'LOP_MODIFY_ROW', 'LOP_DELETE_ROWS')
"""

cursor.execute(query)
records = cursor.fetchall()
for r in records:
    print(r.tx_id)
