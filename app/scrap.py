
"""
Challenge: Data Analytics con Python - Alkemy.
Juan Bohórquez (2022)
https://github.com/JuanBoho/Alkemy-Data-Analitycs-Challenge
"""

import os
import requests
import datetime
from bs4 import BeautifulSoup
import logging as log


class DataScraper():
    
    """ Scrap y descarga de archivos .csv en
        las dependencias de la web: https://datos.gob.ar
        guarda rutas en diccionario files_path
    """

    def __init__(self, url_):
        self.url = url_
        self.files = {}
        self.files_path = {}

    def get_data_links(self):
        """ Busca todos los archivos csv dispoibles en url y crea
        un diccionario {'nombre_csv':'link_descarga'}"""

        r = requests.get(self.url)
        s = BeautifulSoup(r.text, "html.parser")

        # Archivos disponibles en pág
        csv_names = [i.string for i in s.find_all("h3")]

        download_btn = []
        for btn in s.find_all("button"):
            if btn.string == 'DESCARGAR':
                download_btn.append(btn.string)

        csv_links = []
        for child in download_btn:
            for parent in child.parents:
                if parent.has_attr("href"):
                    csv_links.append(parent["href"])


        # Diccionario
        self.files = dict(zip(csv_names, csv_links))
        self.files['Museos'] = self.files['Museo']  # Fix nombre categoría
        del self.files['Museo']

        return self.files

    def date_format(self):
        """ Formatea fecha en español:
            regresa lista: ['año-mes', 'día-mes-año']
        """
    
        date = datetime.datetime.now()
        months = ("enero", "febrero", "marzo", "abril",
                  "mayo", "junio", "julio", "agosto",
                  "septiembre", "octubre", "noviembre", "diciembre")

        month = months[date.month - 1]
        year = date.year
        sp_format = "{}-{}".format(year, month)
        ydm_format = date.strftime("%d-%m-%Y")

        return (sp_format,ydm_format)

    def create_path(self, cat, t):
        """ Crea y retorna ruta en directorio actual
            como: "../directorio_actual/data/categoría/año-mes/nombre-de-archivo.csv"
            cat: categoría, t: tuple ('año-mes', 'dia-mes-año').
        """
        #current_directory = os.path.dirname(os.path.abspath(__file__))  # directorio actual
        new_directory = "{}".format(t[0]) 
        parent_dir = "./data/{}".format(cat)
        path = os.path.join(parent_dir, new_directory) 

        try:
            os.makedirs(path, exist_ok = True)  # Crea directorio
        except OSError as error:
            print("Error en la ruta {}".format(error) )

        file_name = "{}-{}.csv".format(cat.lower().replace(" ", "-"), t[1])    
        file_path = "{}/{}".format(path, file_name)

        # Agrego ruta del último archivo al diccionario 
        self.files_path[cat] = "{}/{}".format(new_directory, file_name)

        return file_path

    def store_file(self, file_url, file_path):
        """ Guarda archivos en ruta dada.
                - file_url : url del archivo
                - file_path : ruta a directorio donde guardarlo
        """
        with requests.get(file_url, stream=True) as r:
            with open(file_path, 'wb') as fil:
                try:
                    for chunk in r.iter_content(chunk_size=1024):
                        fil.write(chunk)
                except Exception as e:
                    log.error('Error al guardar archivo: {}'.format(e))

    def scrap(self, cat_list):
        """ Busca archivos .csv disponibles para descargar en dependencia
            Crea un diccionario {nombre:link_descarga}
            Descarga archivos y les guarda de forma local.
        """
        files = self.get_data_links()
        t = self.date_format()

        # Descargo y guardo archivos 
        for cat in cat_list:
            file_path = self.create_path(cat,t)
            file_url = files[cat]
            log.info('Descargando archivo: {} ...'.format(cat))

            self.store_file(file_url,file_path)
    
    def files_path(self):
        return self.file_names
