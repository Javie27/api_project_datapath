# -*- coding: utf-8 -*-
"""
Created on Sat Sep 10 03:11:31 2022

@author: atp50
"""

##### consumir datos del API ##################################################

import http.client
import pandas as pd   
import json


conn = http.client.HTTPSConnection("http://127.0.0.1:5000/department/all")


suscr_updates = []    

headers = {
        'content-type': "application/json",
        'authorization': "Token 5088cbc5ceb807c702b4e3487173ef792eb50be4",
        'cache-control': "no-cache",
        'postman-token': "1debf677-63d9-c876-7d91-26171dfa2a77"
        }
    
conn.request("GET", headers)




res = conn.getresponse()
data = res.read()
    
datos = data.decode("utf-8")
listas = json.loads(datos)
    
suscr_updates.append(listas) 
    
    
resultado = []

for y in suscr_updates:
    for z in y:
        resultado.append(z)
        
final = pd.DataFrame(resultado)  

final.head()
final.to_excel('archivo.xlsx')

