from flask import Flask, request, json, Response
from pymongo import MongoClient
import logging as log
from flask import Flask, Response
from geopy.geocoders import Nominatim
import pandas as pd
import csv
from bson import json_util
from flask_pymongo import pymongo

#API para consultar datos almacenados desde mongoDB
app = Flask(__name__)
app.config

class MongoAPI:

    def __init__(self, data):
        self.client = MongoClient("mongodb://localhost:27017/")  
      
        database = data['database']
        collection = data['collection']
        cursor = self.client[database]
        self.collection = cursor[collection]
        self.data = data

    #funcion para consultar todos los datos de la bd
    def read(self):
            documents = self.collection.find()
            print('+++DOCUMENTS++')
            print(documents)
            output = [{item: data[item] for item in data if item != '_id'} for data in documents]
            print('+++output def READ++')
            print(output)
            return output
    #funcion para consultar solo un dato usando como filtro el id del vehiculo
    def readUnique(self):
            filt = self.data['Filter']
            documents = self.collection.find_one(filt,{'Ubicacion':1,'vehicle_id':1,'_id':0})
            output = documents
            print('+++output def readUnique++')
            print(output)
            return  json.loads(json_util.dumps(output))

    #funcion para consultar todos los vehiculos en status 1 o 2 
    def readUniqueDisponible(self):
            filt = self.data['Filter']
            documents = self.collection.find(filt,{'Ubicacion':1,'vehicle_id':1,'vehicle_current_status':1,'_id':0})
            output = documents
            print('+++output def readUnique++')
            print(output)
            return  json.loads(json_util.dumps(output))

    #funcion que retorna los vehiculos de una alcaldia en especifico
    def readDisponibleAlcaldia(self):
            filt = self.data['Filter']
            documents = self.collection.find(filt,{'Ubicacion':1,'Alcaldia':1,'vehicle_id':1,'vehicle_current_status':1,'_id':0})
            output = documents
            print('+++output def readUnique++')
            print(output)
            return  json.loads(json_util.dumps(output))      

    #funcion que retorna todas las alcaldias que aparecen en la bd sin duplicar los registros
    def readalcaldias(self):
            documents = self.collection.find()
            print('+++DOCUMENTS++')
            print(documents)
            output = [{item: data[item] for item in data if item != '_id'} for data in documents]
            print('+++output def READ++')
            print(output)
            df = pd.DataFrame(output)
            dfs=df['Alcaldia'].unique()
            return json.loads(json_util.dumps(dfs))  
         

    

@app.route('/')
def base():
  return Response(response=json.dumps({"Status": "UP"}),status=200,mimetype='application/json')

@app.route('/mongodb/todo', methods=['GET'])
def mongo_read():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400,
                        mimetype='application/json')
    obj1 = MongoAPI(data)
    response = obj1.read()
    return Response(response=json.dumps(response),
                    status=200,
                    mimetype='application/json')

@app.route('/mongodb/uniVehicle', methods=['GET'])
def mongo_readunique():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400,
                        mimetype='application/json')
    obj1 = MongoAPI(data)
    response = obj1.readUnique()
    return Response(response=json.dumps(response),
                    status=200,
                    mimetype='application/json')

@app.route('/mongodb/disponible', methods=['GET'])
def mongo_readdisponible():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400,
                        mimetype='application/json')
    obj1 = MongoAPI(data)
    response = obj1.readUniqueDisponible()
    return Response(response=json.dumps(response),
                    status=200,
                    mimetype='application/json')

@app.route('/mongodb/dAlcaldia', methods=['GET'])
def mongo_readdisponibleAlcaldia():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400,
                        mimetype='application/json')
    obj1 = MongoAPI(data)
    print(obj1)
    response = obj1.readDisponibleAlcaldia()
    return Response(response=json.dumps(response),
                    status=200,
                    mimetype='application/json')

@app.route('/mongodb/todasAlcaldias', methods=['GET'])
def mongo_readAlcaldias():
    data = request.json
    if data is None or data == {}:
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400,
                        mimetype='application/json')
    obj1 = MongoAPI(data)
    response = obj1.readalcaldias()
    return Response(response=json.dumps(response),
                    status=200,
                    mimetype='application/json')


if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')
    

