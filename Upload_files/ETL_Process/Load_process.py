# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 16:51:36 2022

@author: atp50
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 12:22:40 2022

@author: atp50
"""
import pandas as pd
import sqlalchemy as db

class Load_process:
    
    def __init__(self,ruta,Nombre_Archivo,Extension):
        self.ruta = ruta,
        self.Nombre_Archivo= Nombre_Archivo,
        self.Extension = Extension
        
    def LoadPostgres(df,TableName):
        df_=df
        engine = db.create_engine('')
        conn = engine.connect()
        df_.to_sql(TableName,engine,if_exists='replace',index=False)
        
        return print("-- Dataframe Cargado "+TableName+" ----")
    