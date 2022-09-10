# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 23:18:53 2022

@author: atp50
"""

from flask import Flask, request, jsonify
from module.database import Database

app= Flask(__name__)
db= Database()  

@app.route('/')
def index():
    return "API REST"

### 1. ingreso por departamento con id
@app.route("/department", methods = ['GET'])
def department():
    if request.method == "GET" :
        try:
            result = db.readdepartment(request.json["id"])
        except Exception as e:
            return e
    return jsonify(result)

### 2. ingreso por departamento todo
@app.route("/department/all", methods = ['GET'])
def departmentall():
    if request.method == "GET" :
        try:
            result = db.readdepartmentall()
        except Exception as e:
            return e
    return jsonify(result)

### 3. Categoria mas comprada 
@app.route("/categories", methods = ['GET'])
def categories():
    if request.method == "GET" :
        try:
            result = db.readcategories()
        except Exception as e:
            return e
    return jsonify(result)

### 4. cantidad de ventas por categoria todo  
@app.route("/categories/all", methods = ['GET'])
def categoriesall():
    if request.method == "GET" :
        try:
            result = db.readcategoriesall()
        except Exception as e:
            return e
    return jsonify(result)

#### 5. TOP 10 CLIENTES CON MAS VENTAS 

@app.route("/customer", methods = ['GET'])
def customer():
    if request.method == "GET" :
        try:
            result = db.readcustomer()
        except Exception as e:
            return e
    return jsonify(result)

#### 6. cantidad de ventas por estado de la orden 

@app.route("/status", methods = ['GET'])
def estado():
    if request.method == "GET" :
        try:
            result = db.readstatus(request.json["status"])
        except Exception as e:
            return e
    return jsonify(result)


#### 7. cantidad de ventas por estado de la orden todo 

@app.route("/status/all", methods = ['GET'])
def estadoall():
    if request.method == "GET" :
        try:
            result = db.readstatusall()
        except Exception as e:
            return e
    return jsonify(result)


        
if __name__=="main":
    app.debug =True
    app.run()