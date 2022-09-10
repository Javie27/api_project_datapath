# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 23:22:32 2022

@author: atp50
"""

import psycopg2


class Database:
    def connect(self):
        return psycopg2.connect(database = "Datapath", user = "postgres", password = "postgress", host = "localhost", port = "5432")
    
    def readdepartment(self, id):
        con = Database.connect(self)
        cursor = con.cursor()
        try:
            cursor.execute('select * from public."tr_Ingresos_por_Departamento" where department_id= %s',(id))
            return cursor.fetchall()    
        except:
            return("Error en Conexión a Postgres")
        finally:
            con.close

    def readdepartmentall(self):
        con = Database.connect(self)
        cursor = con.cursor()
        try:
            cursor.execute('select * from public."tr_Ingresos_por_Departamento"')
            return cursor.fetchall()    
        except:
            return("Error en Conexión a Postgres")
        finally:
            con.close
            
    def readcategories(self):
            con = Database.connect(self)
            cursor = con.cursor()
            try:
                cursor.execute('select * from public."tr_CantidadVendida_por_Categoria" order by order_item_quantity desc limit 1')
                return cursor.fetchall()    
            except:
                return("Error en Conexión a Postgres")
            finally:
                con.close
                
    def readcategoriesall(self):
                con = Database.connect(self)
                cursor = con.cursor()
                try:
                    cursor.execute('select * from public."tr_CantidadVendida_por_Categoria" order by order_item_quantity')
                    return cursor.fetchall()    
                except:
                    return("Error en Conexión a Postgres")
                finally:
                    con.close  
                    
    def readcustomer(self):
                con = Database.connect(self)
                cursor = con.cursor()
                try:
                    cursor.execute('select * from public."tr_CantidadVendida_por_Cliente_top10"')
                    return cursor.fetchall()    
                except:
                    return("Error en Conexión a Postgres")
                finally:
                    con.close                      
                    

    def readstatusall(self):
        con = Database.connect(self)
        cursor = con.cursor()
        try:
            cursor.execute('select * from public."tr_cantidadVendida_por_estado"')
            return cursor.fetchall()    
        except:
            return("Error en Conexión a Postgres")
        finally:
            con.close                
                    
                   
    