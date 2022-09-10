# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 12:14:27 2022

@author: atp50
"""

import pandas as pd
from ETL_Process.Ext_process import Ext_process
from ETL_Process.Trans_process import Trans_process
from ETL_Process.Load_process import Load_process

#### categories #################################################
df_categories= Ext_process.Extraction(
    ruta="C:/Users/atp50/OneDrive/Escritorio/CURSOS/Data Engineer/Ingenieria de Datos con Pyhton/Proyecto Python/Upload_files/data/",
    Nombre_Archivo="categories",
    Extension = ""
    )

df_categories_columns = Trans_process.Trans_columns(df_categories,
                                             "category_id,category_department_id,category_name")


Load_process.LoadPostgres(df_categories_columns,"t_Categories")

#### customer #################################################
df_customer= Ext_process.Extraction(
    ruta="C:/Users/atp50/OneDrive/Escritorio/CURSOS/Data Engineer/Ingenieria de Datos con Pyhton/Proyecto Python/Upload_files/data/",
    Nombre_Archivo="customer",
    Extension = ""
    )
df_customer_columns = Trans_process.Trans_columns(df_customer,
                                             "customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode")


Load_process.LoadPostgres(df_customer_columns,"t_Customer")

#### departaments #################################################

df_departments= Ext_process.Extraction(
    ruta="C:/Users/atp50/OneDrive/Escritorio/CURSOS/Data Engineer/Ingenieria de Datos con Pyhton/Proyecto Python/Upload_files/data/",
    Nombre_Archivo="departments",
    Extension = ""
    )

df_departments_columns = Trans_process.Trans_columns(df_departments,
                                             "department_id,department_name")

Load_process.LoadPostgres(df_departments_columns,"t_Departments")


#### order_item #################################################

df_order_items= Ext_process.Extraction(
    ruta="C:/Users/atp50/OneDrive/Escritorio/CURSOS/Data Engineer/Ingenieria de Datos con Pyhton/Proyecto Python/Upload_files/data/",
    Nombre_Archivo="order_items",
    Extension = ""
    )

df_order_items_columns = Trans_process.Trans_columns(df_order_items,
                                             "order_item_id,order_item_order_id,order_item_product_id,order_item_quantity,order_item_subtotal,order_item_product_price")


df_order_items_columns = Trans_process.Trans_columnstype(df_order_items_columns,"order_item_id,order_item_order_id,order_item_product_id,order_item_quantity","int")
df_order_items_columns = Trans_process.Trans_columnstype(df_order_items_columns,"order_item_subtotal,order_item_product_price","float")


Load_process.LoadPostgres(df_order_items_columns,"t_Order_items")


#### orders #################################################
df_orders= Ext_process.Extraction(
    ruta="C:/Users/atp50/OneDrive/Escritorio/CURSOS/Data Engineer/Ingenieria de Datos con Pyhton/Proyecto Python/Upload_files/data/",
    Nombre_Archivo="orders",
    Extension = ""
    )

df_orders_columns = Trans_process.Trans_columns(df_orders,
                                             "order_id,order_date,order_customer_id,order_status")


df_orders_columns = Trans_process.Trans_columnstype(df_orders_columns,"order_date","datetime")


Load_process.LoadPostgres(df_orders_columns,"t_Orders")

#### products ##################################################

df_products= Ext_process.Extraction(
    ruta="C:/Users/atp50/OneDrive/Escritorio/CURSOS/Data Engineer/Ingenieria de Datos con Pyhton/Proyecto Python/Upload_files/data/",
    Nombre_Archivo="products",
    Extension = ""
    )

df_products_columns = Trans_process.Trans_columns(df_products,
                                             "product_id,product_category_id,product_name,product_description,product_price,product_image")

df_products_columns = Trans_process.Trans_columnstype(df_products_columns,"product_description","str")

Load_process.LoadPostgres(df_products_columns,"t_Products")


##################################### SOLICITUD #################################################

df_join = Trans_process.Trans_join(df_order_items_columns,df_products_columns,"order_item_product_id","product_id","inner")
df_join = Trans_process.Trans_join(df_join,df_categories_columns,"product_category_id","category_id","inner")
df_join = Trans_process.Trans_join(df_join,df_departments_columns,"category_department_id","department_id","inner")
df_join = Trans_process.Trans_join(df_join,df_orders_columns,"order_item_order_id","order_id","inner")
df_join = Trans_process.Trans_join(df_join,df_customer_columns,"order_customer_id","customer_id","inner")



##### 1.¿Cuáles son los ingresos por departamento?

df_join_ingxdep = df_join.groupby(['department_id','department_name']).agg({'order_item_subtotal':'sum'}).reset_index()
print(df_join_ingxdep)

Load_process.LoadPostgres(df_join_ingxdep,"tr_Ingresos_por_Departamento")

##### 2.¿Cuáles son las categorías más compradas? (identificar el nombre de la categoría)

df_join_cantxcateg = df_join.groupby(['category_id','category_name']).agg({'order_item_quantity':'sum'}).reset_index()
print(df_join_cantxcateg)

Load_process.LoadPostgres(df_join_cantxcateg,"tr_CantidadVendida_por_Categoria")

##### 3.¿Quiénes son el top 10 de clientes que generan más compras para fidelizarlos?

df_join_cantxcli = df_join.groupby(['customer_id','customer_fname','customer_lname']).agg({'order_item_quantity':'sum'}).reset_index().head(10).sort_values('order_item_quantity',ascending=False)
print(df_join_cantxcli)

Load_process.LoadPostgres(df_join_cantxcli,"tr_CantidadVendida_por_Cliente_top10")

#### 4. Cantidad de Ventas e ingresos diarios #######################

df_join_ingrxFecha = df_join.groupby(['order_date']).agg({'order_item_quantity':'sum','order_item_subtotal':'sum'}).reset_index()
print(df_join_ingrxFecha)

Load_process.LoadPostgres(df_join_ingrxFecha,"tr_CantidadVendida_por_Fecha")

#### 5. cantidad de ventas por estado de la orden ###################

df_join_cantxEst = df_join.groupby(['order_status']).agg({'order_item_quantity':'sum','order_item_subtotal':'sum'}).reset_index()
print(df_join_cantxEst)

Load_process.LoadPostgres(df_join_cantxEst,"tr_cantidadVendida_por_estado")


################################### GRAFICO DE HISTOGRAMA ##################################

from matplotlib import pyplot as plt

import sqlalchemy as db
import pandas as pd

engine = db.create_engine('postgresql://postgres:postgress@localhost:5432/Datapath')
conn = engine.connect()

df_join= pd.read_sql('''select * from public."t_Order_items" oi
                             inner join public."t_Products" pr on oi.order_item_product_id=pr.product_id
                             inner join public."t_Categories" ca on pr.product_category_id=ca.category_id
                             inner join public."t_Departments" de on ca.category_department_id=de.department_id
                             inner join public."t_Orders" od on oi.order_item_order_id=od.order_id
                             inner join public."t_Customer" cu on od.order_customer_id=cu.customer_id
                        ''', con=conn)

histogram = df_join.plot.hist()
print(histogram)
plt.show()



