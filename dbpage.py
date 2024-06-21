#!/usr/bin/env python

"""
Dumps pages of given table using DBCC utility.
"""

import logging
import struct

import pyodbc

SERVER = '192.168.122.7'
DATABASE = 'TestDB'
USERNAME = 'sa'
PASSWORD = 'RedHat12345'
CONN_STRING = (f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};'
               f'DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=no')

log = logging.getLogger("dbpage")

def first_page_numbers(table_name):
    """
    Obtains page number and file number of the first page of given
    table.
    """
    query = f"""
    SELECT first_page
    FROM sys.partitions partitions
        JOIN sys.system_internals_allocation_units allocations
        ON partitions.partition_id = allocations.container_id
    WHERE object_id=object_id(N'{table_name}');
    """
    cursor.execute(query)
    record = cursor.fetchone()
    page = record.first_page
    file_no, page_no = struct.unpack("<Ih", page)
    log.info(f"Page number and file number: {page_no}:{file_no}")
    return page_no, file_no

def db_page(page_no, file_no, print_opt=1):
    """
    Get the content of the page specified by page_no and file_no parameters.
    """
    # No need to run DBCC TRACEON(3604) when run WITH TABLERESULTS.
    query = (f"DBCC PAGE (TestDB, {page_no}, {file_no}, {print_opt}) "
             f"WITH TABLERESULTS;")
    cursor.execute(query)
    return cursor.fetchall()

def first_page_of(table_name):
    """
    Get the content of the first page of table table_name.
    """
    page_no, file_no = first_page_numbers(table_name)
    return db_page(page_no, file_no)


try:
    conn = pyodbc.connect(CONN_STRING)
    cursor = conn.cursor()
    page_table = first_page_of("Inventory")
    for p in page_table:
        if p.Object.startswith("Memory Dump"):
            print(f"result: {p.VALUE}")
finally:
    if conn:
        cursor.close()
        conn.close()
