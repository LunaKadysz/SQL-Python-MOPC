#!/usr/bin/env python
# coding: utf-8

# In[24]:
#Esteban hizo este código, si falla, avisarle y si ya no está solo dios podrá ayudar.
#Este código se hizo para obtener el valor del dolar para HSBC diariamente(está croneado para correr cada 2 hs de 7 am. a 15 pm.)
#!/bin/bash

import os
import datetime
#Se cambia el directorio de trabajo
os.chdir("/home/marcos-rago/Documents/")
#Se importan las funciones en util.functions para conexión a la base de datos
from utils.functions_ia import *
#Se utilizará BeautifulSoup para cambiar el formato a HTML 
from bs4 import BeautifulSoup
from selenium import webdriver
import requests
#Nombre de la tabla donde se pondrá la cotización del dolar
DOLAR_TABLE = "ACT_COTIZACION_DOLAR_HSBC"
#Conexión con la base de datos "Active"
cnxn = connect_database("active")

#URL del banco del cual se obtendrá la cotización
URL ="https://www.bna.com.ar/Personas"
page = requests.get(URL)
soup = BeautifulSoup(page.content, 'html.parser')
clase = "table cotizacion"
#En la posición [2] se encuentra el valor de venta del dolar, revisar si cambian el formato de tabla.
cotizacion = float(soup.find(class_ = clase).find_all('td')[2].text.replace(',', '.'))

print(cotizacion)

cursor = cnxn.cursor()
cursor.execute("INSERT INTO ACTIVE.DBO.{} (COTIZACION,FECHA_COTIZACION) values({},GETDATE())".format(DOLAR_TABLE,cotizacion)
cursor.commit()
cursor.close()


# In[ ]:




