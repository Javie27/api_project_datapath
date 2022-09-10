# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 12:22:40 2022

@author: atp50
"""
import pandas as pd

class Ext_process:
    

    def __init__(self,ruta,Nombre_Archivo,Extension):
        self.ruta = ruta,
        self.Nombre_Archivo= Nombre_Archivo,
        self.Extension = Extension
        self.Process = self.Extraction(ruta,Nombre_Archivo,Extension)
        
    def Extraction(ruta,Nombre_Archivo,Extension):
        #return 1
        df_=pd.DataFrame()
        File=Nombre_Archivo+Extension
        df_=pd.read_csv(ruta+''+File, header=None, sep="|")
        return df_

