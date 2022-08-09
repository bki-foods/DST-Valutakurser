#!/usr/bin/env python3

import PyDST
import datetime
import urllib
import pandas as pd
from sqlalchemy import create_engine

# Create variables
now = datetime.datetime.now()
scriptName = 'QM_Tea.py'
executionId = int(now.timestamp())
script_name = 'dst valutakurser.py'
timestamp = datetime.datetime.now()

server_04 = "sqlsrv04"
db_ds = "BKI_Datastore"
params_ds = f"DRIVER={{SQL Server Native Client 11.0}};SERVER={server_04};DATABASE={db_ds};trusted_connection=yes"
con_ds = create_engine('mssql+pyodbc:///?odbc_connect=%s' % urllib.parse.quote_plus(params_ds))

# Function for insert into SQL database
def insert_sql(dataframe, table_name, schema):
    dataframe.to_sql(table_name, con=con_ds, schema=schema, if_exists='append', index=False)

# =============================================================================
# Get data from danmarks statistik. One query per currency. Time = 2000 - 2099
# One dst query per currency. If multiple currencies alter log dataframe for sql insert per currency code
# =============================================================================
data = PyDST.get_data(table_id = 'DNVALD', variables = {'KURTYP':'KBH'
                                                        ,'VALUTA':'USD'
                                                        ,'Tid':'202*'})

# Read dst query into dataframe and add relevant columns with info
df = PyDST.utils.to_dataframe(data)
df['Valutakode'] = 'USD'
df['ExecutionId'] = executionId
df['Timestamp'] = timestamp
# Remove non-numeric values (.. and 0 values)
df = df[~df['INDHOLD'].isin(['..','0.0000'])]
# Create dataframe for log insert    
df_log = pd.DataFrame(data= {'Date':now, 'Event':script_name, 'Note': 'USD from dst - execution id: ' + str(executionId)}, index=[0])    
    
# =============================================================================
# Insert into currency table as well as dbo.log
# =============================================================================
        
insert_sql(df ,'Valutakurser' ,'fin')
insert_sql(df_log, 'Log', 'dbo')
