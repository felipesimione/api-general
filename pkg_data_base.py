# -*- coding: UTF-8 -*-

#Functions to manage database connections

import sys
import pyodbc
import pandas as pd
import psycopg2
from io import StringIO


def bulk_loader_insert(schema, table, df):
    """Insert data with bulk loader.

    Args:
        schema: Name of the schema of the referenced table.
        table: Table name that will be used to inserted data.
        df: Data Frame that will be inserted.
    """
    if df.empty:
        print('DataFrame is empty!')
    else:
        try:
            conn = psycopg2.connect(database="bi", host="111.111.111.11", user="administrator", password="xxxxxxxxxx")
            table = schema + '.' + table

            # Initialize a string buffer
            sio = StringIO()
            sio.write(df.to_csv(index=None, header=None, sep=','))  # Write the Pandas DataFrame as a csv to the buffer
            sio.seek(0)  # Be sure to reset the position to the start of the stream

            # Copy the string buffer to the database, as if it were an actual file
            with conn.cursor() as c:
                c.copy_from(sio, table, columns=df.columns, sep=',')
                conn.commit()

            print('Bulk Loader Success!')
        except:
            db_connect_close(conn, c)
            print('Bulk Loader Error')
            raise
