#!/usr/bin/env python3


import PyDST

resp = PyDST.get_subjects() 

# print(resp.json())

tables = PyDST.get_tables(subjects = '16').json()

# print(tables)

info = PyDST.get_tableinfo(table_id = 'DNVALD').json()

print(info)

data = PyDST.get_data(table_id = 'DNVALD', variables = {'KURTYP':'KBH'
                                                        ,'VALUTA':'USD'
                                                        ,'Tid':'20*'})

df = PyDST.utils.to_dataframe(data)

print(df)
