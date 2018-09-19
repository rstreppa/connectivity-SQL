#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 24 10:06:28 2018

@author: rstreppa
"""

import pyodbc as db
import numpy as np
import pandas as pd


def saquri04_conn():
    """Establish a connection to DB server saquri04.
    Args:
    
    Returns:
        object: database connection object    
    """
    # get connection
    conn_string =   "DRIVER={SQL Server Native Client 11.0};"+\
                    "SERVER=SAQURI04.corp.dtcc.com;"+\
                    "DATABASE=dwa_nscc_2017;"+\
                    "UID=quant_user;"+\
                    "PWD=Temp2017"
    conn = db.connect(conn_string)
    return conn

def query(sql):
    """
    Retrieves DB tables from a SQL query as a pandas DataFrame
    
    Args:
        sql (string): the SQL code that queries the database
    
    Returns:
        DataFrame: the rows retrieved as a pandas DataFrame (nrows x ncols)
    """
    conn = saquri04_conn()
    data = pd.read_sql(sql,conn)
    return data

def multiquery(*sqls):
    """
    Retrieves DB tables from a SQL multi query (temp tables) as a pandas DataFrame
    
    Args:
        *sqls (string) the SQL code that queries the database
    
    Returns:
        DataFrame: the rows retrieved as a pandas DataFrame (nrows x ncols)
    """
    conn = saquri04_conn()
    cursor = conn.cursor()
    for sql in sqls:
        cursor.execute(sql)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    for i in range(0,len(rows)):
        rows[i]=tuple(rows[i])
    df = pd.DataFrame(rows, columns=columns)
    return df