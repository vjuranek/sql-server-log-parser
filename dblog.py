#!/usr/bin/env python

"""
Obtains a log record for insert operation done in provided transaction and
decodes the record. to obtain raw data, `fn_dblog()` function is used.
"""

import math
import struct

import pyodbc

SERVER = '192.168.122.7'
DATABASE = 'TestDB'
USERNAME = 'sa'
PASSWORD = 'RedHat12345'
CONN_STRING = (f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};'
               f'DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=no')

RAW_INSERT_COLUMN = "RowLog Contents 0"
RAW_UPDATE_COLUMN = "RowLog Contents 1"

INSERT_TX_ID = "0000:00000675"
UPDATE_TX_ID = "0000:000006a5"


def decode_records(row):
    """
    Decodes one change record and returns a dict with decoded values.
    """
    bit_A, bit_B, fix_data_len = struct.unpack("<bbH", row[0:4])
    fixed_data = struct.unpack(
        "<" + "c" * (fix_data_len - 4), row[4:fix_data_len])
    num_cols = struct.unpack("<" + "H", row[fix_data_len:(fix_data_len + 2)])
    null_array_size = math.ceil(num_cols[0] / 8)
    low = fix_data_len + 2
    high = low + null_array_size
    null_array = struct.unpack("<" + "B" * null_array_size, row[low: high])
    low = high
    high = high + 2
    var_data_len = struct.unpack("<H", row[low:high])[0]
    low = high
    high = high + 2 * var_data_len
    var_col_offset = struct.unpack(
        "<" + "c" * (var_data_len * 2), row[low:high])
    remaining = len(row) - high
    low = high
    var_col_data = struct.unpack("<" + "c" * remaining, row[low:])
    return {
        "bit_A": bit_A,
        "bit_B": bit_B,
        "fixed_data": fixed_data,
        "num_cols": num_cols,
        "null_array": null_array,
        "var_data_len": var_data_len,
        "var_col_offset": var_col_offset,
        "var_col_data": var_col_data,
    }


insert_query = f"""
SELECT [{RAW_INSERT_COLUMN}] FROM fn_dblog(NULL, NULL) db 
WHERE db.[Transaction ID] = N'{INSERT_TX_ID}' 
AND db.Operation = 'LOP_INSERT_ROWS';
"""

# TODO: update doesn't work
# update_query = f"""
# SELECT [{RAW_UPDATE_COLUMN}] FROM fn_dblog(NULL, NULL) db
# WHERE db.[Transaction ID] = N'{UPDATE_TX_ID}'
# AND db.Operation = 'LOP_MODIFY_ROW';
# """

conn = pyodbc.connect(CONN_STRING)
cursor = conn.cursor()

cursor.execute(insert_query)
records = cursor.fetchall()
for r in records:
    raw_column = r.__getattribute__(RAW_INSERT_COLUMN)
    if raw_column is None:
        continue
    log_rec = decode_records(raw_column)
    var_col_data = ''.join(b.decode('utf-8') for b in log_rec["var_col_data"])
    print(var_col_data)

cursor.close()
conn.close()
