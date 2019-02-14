#Functions to treat incorrect or missing values of a data frame.

import pandas as pd
import numpy as np
import re

def treat_string(df):
    """"
    Function to treat string-type columns, replaces commas and reserved characters.
    Arguments:
        df: data frame column (df[y])
    Return:
        df: data frame column (df[y])
    """
    df = df.str.replace(',' , '.')
    df = df.str.replace(r'\r' , 'r')
    df = df.str.replace(r'\n' , 'n')
    df = df.str.replace(r'\\' , 'n')
    df = df.fillna('')
    return df

def treat_num(df):
    """"
    Function to treat num-type columns, fills NULL and NaN with default value.
    Arguments:
        df: data frame column (df[y])
    Return:
        df: data frame column (df[y])
    """
    df = df.fillna(-1)
    return df

def treat_date(df):
    """"
    Function to treat date-type columns, fills NULL and NaN with default value.
    Arguments:
        df: data frame column (df[y])
    Return:
        df: data frame column (df[y])
    """
    df = df.fillna('1969-12-31 21:00:00')
    return df


def treat_columns(df):
    """"
    Function to treat columns, calls type-specific functions.
    Arguments:
        df: data frame
    Return:
        None
    """
    for y in df.columns:
        if(df[y].dtype == np.dtype('int64') or df[y].dtype == np.dtype('float64')):
            df[y] = treat_num(df[y])
            #print('it is a number')
        elif(df[y].dtype == np.dtype('datetime64[ns]') or df[y].dtype == np.dtype('<M8[ns]')):
            df[y] = treat_date(df[y])
            #print('it is a date')
        elif(df[y].dtype == np.dtype(object)):
            df[y] = treat_string(df[y])
            #print('it is an object')
        else:
            #print('idk what it is, maybe it is a boolean')
            pass

def adjust_columns_name(df):
    """"
    Function to change columns name to the Netfarma BI standard.
    Arguments:
        df: data frame
    Return:
        None
    """
    for i in range(len(df.columns)):
        # Create regex
        rx = re.compile(r'(?<=[a-z])(?=[A-Z])')
        # Split string upper
        column_name = rx.sub(' ',str(df.columns[i]))
        # Change string with space to "_" and lower
        column_name = re.sub('\s+', '_', column_name).lower()
        new_name = {str(df.columns[i]) : column_name}
        # Rename columns
        df.rename(index=str, columns=new_name, inplace=True)
