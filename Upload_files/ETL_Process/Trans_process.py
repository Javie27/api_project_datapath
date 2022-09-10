# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 13:36:55 2022

@author: atp50
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class Trans_process:
    

    def __init__(self,ruta,Nombre_Archivo,Extension):
        self.ruta = ruta,
        self.Nombre_Archivo= Nombre_Archivo,
        self.Extension = Extension
        self.Process = self.Extraction(ruta,Nombre_Archivo,Extension)
        
    def Trans_columns(df,columns):
        columns=columns.split(',')
        df_=df
        df_.columns=columns
        return(df_)
    
    def Trans_columnstype(df,columns,typecolumn):
        columns=columns.split(',')
        df_=df
        if typecolumn=="int":            
            for column in columns:
                    df_[column]=df_[column].astype(int)
            
        if typecolumn=="float":
             for column in columns:
                     df_[column]=df_[column].astype(float)           
        
        if typecolumn=="str":
            for column in columns:
                    df_[column]=df_[column].astype(str)
                    
        if typecolumn=="datetime":
            for column in columns:
                    df_[column]=pd.to_datetime(df_[column])
        
        return df_
    
    def Trans_join(df1_left,df2_right,column_left,column_right,typejoin):
        df1_=df1_left
        df2_=df2_right
        
        if typejoin=="left":
            df_ = pd.merge(df1_ , df2_, how="left", left_on=column_left, right_on=column_right)
        
        if typejoin=="right":
            df_ = pd.merge(df1_ , df2_, how="right", left_on=column_left, right_on=column_right)
        
        if typejoin=="inner":
            df_ = pd.merge(df1_ , df2_, how="inner", left_on=column_left, right_on=column_right)
            
        return df_
    
    