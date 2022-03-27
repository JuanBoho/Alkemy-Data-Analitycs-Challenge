"""
Challenge: Data Analytics con Python - Alkemy.
Juan Bohórquez (2022)
https://github.com/JuanBoho/Alkemy-Data-Analitycs-Challenge
"""

from scrap import DataScraper
from data_processing import DataProcesser
from to_databse import TablasDB
import logging as log

# Datasets pedidos
categories = ['Museos', 'Salas de Cine', 'Bibliotecas Populares']

#fuente
url = "https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales"

#Scrap
scrap = DataScraper(url)
scrap.scrap(categories)

# Process
data_pro = DataProcesser(categories)
data_pro.readFiles(scrap.files_path)
data_pro.cleanFiles()
tablas_df = data_pro.process()

# Database

tablas = TablasDB()
tablas.exe()
tablas.actualizar_tablas(tablas_df)
