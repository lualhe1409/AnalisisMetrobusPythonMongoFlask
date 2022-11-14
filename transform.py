from geopy.geocoders import Nominatim
import pandas as pd
import csv
import pymongo
from pymongo import MongoClient 
from flask import Flask, request, json, Response
import json

#Inicialmente se borra cualquier BD de datos en Mongo db que tenga el mismo nombre
client = MongoClient("mongodb://localhost:27017/")  
mydb = client[ "MetroBusCDMX"]
mycol = mydb[ "Control"]
mycol.drop()
print('Base Eliminada Correctamente')

#def alcaldia=tomando como base las cordenadas indicadas en el csv y ocupando GEOpy se logra la localizacion de la 
#Direccion en general misma que se va filtrando y transformando hasta encontrar solo la alcaldia
def alcaldia(lan):
        try:
            geolocator = Nominatim(user_agent="lualhe14@gmail.com")
            scoord = (lan)
            location = geolocator.reverse(scoord)
            loct=location.raw
            loct=loct['address']
            loct=loct['neighbourhood']
            return loct
        except:		
            return 'N/a'

#la funcion address nos trae la direccion completa dadas solamente las cordenadas del archivo csv
#
def address(lan):
    try:
        geolocator = Nominatim(user_agent="lualhe14@gmail.com")
        scoord = (lan)
        location = geolocator.reverse(scoord)
        return location.address
    except:		
        return 'N/a'

#appendDictToDF Funcion que nos ayuda a escribir sobre un dataframe 

def appendDictToDF(df,dictToAppend):
    df = pd.concat([df, pd.DataFrame.from_records([dictToAppend])])
    return df


#En esta parte es donde se realiza la lectura de todo el csv y se extraen las coordenadas mismas que se mandan a las 
#funciones anteriormente definidas una vez que nos retornan la informacion estas se empiezas a cargar a un dataframe
#finalmente este dataframe se envia a la BD en MongoDB para que sea almacenada y empieze a ser consultada por el API
dfs=pd.DataFrame(columns=['id','geographic_point', 'vehicle_id', 'vehicle_current_status','Alcaldia','Ubicacion'])
geodir=[]
with open('./prueba_fetchdata_metrobus.csv', encoding="utf8") as csv_file:
    csv_data = csv.reader(csv_file, delimiter=',', quotechar='\'')
    for row in csv_data:
        longitud=0
        latitud=row[7]+' '+row[8]
        latitud=latitud.replace('"','')
        geodir = alcaldia(latitud)
        direccion=address(latitud)
        loct=geodir
        dfs=appendDictToDF(dfs,{'id':row[0],'geographic_point':latitud,'vehicle_id':row[2],'vehicle_current_status':row[4],'Alcaldia':loct,'Ubicacion':direccion})
print(dfs) 
data=dfs.to_dict(orient="records")
client = MongoClient("mongodb://localhost:27017/")  
db=client['MetroBusCDMX']
db.Control.insert_many(data)
print("Informacion guardada Correctamente")