"""
Challenge: Data Analytics con Python - Alkemy.
Juan Bohórquez (2022)
https://github.com/JuanBoho/Alkemy-Data-Analitycs-Challenge
"""

import os
import pandas as pd
import numpy as np
import logging as log


class DataProcesser():
    """ Procesa los datos """

    def __init__(self,categorias_):
        self.categories = categorias_
        self.df_list = []


    def readFiles(self, files_path_):
        """ Lee los últimos archivos descargados de cada categoría
            en df_prov y les agega a la lista de dataframes.
            files_path: diccionario {categoria: path de earchivo} """
        
        for cat in self.categories:
            csv_path = "./data/{}/{}".format(cat,files_path_[cat])
            df_prov = pd.read_csv(csv_path)
            self.df_list.append(df_prov)

            
    def cleanFiles(self):
        """ Normaliza dataframes """

        for i in self.df_list:
            fix_col_names(i)
            tel_concat(i)
            prov_fix(i)
    

    def datosConjuntos(self):
        """ Crea df de datos conjuntos """

        tabla_uno_df = pd.concat(self.df_list)
        
        # Columnas no pedidas
        to_drop = ['observaciones', 'subcategoria', 'piso', 'cod_area','latitud','longitud', 'tipolatitudlongitud', 'info_adicional','fuente',
                    'jurisdiccion', 'año_inauguracion', 'observacion',
                    'departamento', 'informacion adicional', 'tipo_gestion','año_inicio',
                    'año_actualizacion', 'pantallas', 'butacas','espacio_incaa', 'actualizacion']

        # nuevos nombres columnas
        new_columns = ["cod_localidad", "id_provincia", "id_departamento", "categoria",
                "provincia", "localidad", "nombre", "domicilio", "cod_postal",
                "num_tel", "mail", "web"]

        tabla_uno_df = tabla_uno_df.drop(to_drop, axis=1)
        tabla_uno_df.columns = new_columns

        return tabla_uno_df


    def totalesDatos(self, fuentes_, categoria_, provincia_):
        """ Crea df con registros totales"""

        # Agrego descrp de dict (str) para filtrarlo en tabla2        
        add_to_dict(categoria_, 'categoria')
        add_to_dict(provincia_, 'provincia')
        add_to_dict(fuentes_, 'fuente')

        registros_totales_dict = {**fuentes_, **categoria_, **provincia_} # Un solo diccionario
        tabla_dos_df = pd.DataFrame(registros_totales_dict).transpose() # values como cols
        tabla_dos_df.reset_index(inplace=True)
        tabla_dos_df = tabla_dos_df.rename(columns = {'index':'item', 0: 'total_registros', 1: 'tipo'})
        
        # Tabla agrupada
        table_dos_df = pd.pivot_table(tabla_dos_df, index =['tipo','item'], values= 'total_registros') 
        
        return table_dos_df


    def datos_cines(self):
        """ Crea df con info Cines"""

        cines = self.df_list[1]

        # Fix valores cols
        cines['espacio_incaa'] = cines['espacio_incaa'].replace({'SI': 1, 'si': 1,'0': 0,np.nan: 0,})
        tabla_tres_df = cines.groupby('provincia').sum()

        # fix final del df
        to_drop2 = ['cod_loc', 'idprovincia', 'iddepartamento', 'observaciones', 
                    'cp','informacion adicional', 'latitud', 'longitud', 'año_actualizacion']

        tabla_tres_df = tabla_tres_df.drop(to_drop2, axis=1)
        tabla_tres_df = tabla_tres_df.reset_index()
        
        return tabla_tres_df


    def process(self):
        """ Crea tres tablas según requerimientos del Challenge
            y les regresa en una lista
        """
        tabla_uno = self.datosConjuntos()

        # args para df2
        fuentes = totales_fuente_dict(self.df_list) 
        categoria = tabla_uno['categoria'].value_counts().to_dict() 
        provincia = tabla_uno['provincia'].value_counts().to_dict()

        
        tabla_dos = self.totalesDatos(fuentes, categoria, provincia)
        tabla_tres = self.datos_cines()

        return [tabla_uno, tabla_dos, tabla_tres]


# ------Utilidades------

# Normalización
def normalize_str(s):
    """ Cambia acentos en vocales de string """
    replace_lst= [("á", "a"),
                  ("é", "e"),
                  ("í", "i"),
                  ("ó", "o"),
                  ("ú", "u"),
                 ]

    for a, b in replace_lst:
        s = s.replace(a, b)
    return s


def fix_col_names(df):
    """ Cambia nombres en columnas del df"""
    cols = df.columns.to_list()
    norm_cols = []

    for col_name in cols:
        col_name = col_name.lower()
        col_name = normalize_str(col_name)
        norm_cols.append(col_name)

    df.columns = norm_cols
    # Mismo nombre en col domicilio
    df.rename(columns={'cod_tel':'cod_area', 'direccion':'domicilio'}, inplace=True)


def tel_concat(df):
    """ Cocatenate 'cod_area' and 'telefono' values in 'telefono' column """
    #fix_col_names(df)
    df['cod_area'] = df['cod_area'].replace([np.nan,'s/d',11],0).astype(int)
    df['cod_area'] = df['cod_area'].replace(0,'')
    df['cod_area'] = df['cod_area'].astype(str)
    
    df['telefono'] = df['cod_area'] + ' ' + df['telefono']
    df['telefono'] = df['telefono'].replace(['s/d', ' s/d'],np.nan)


def prov_fix(df):
    """ Unifica nombres provincias """
    
    df['provincia'] = df['provincia'].replace({'Santa Fe' : 'Santa Fé',
                                                'Neuquén\xa0': 'Neuquén',
                                                'Tierra del Fuego': 'Tierra del Fuego, Antártida e Islas del Atlántico Sur',
                                              })


# Totales tabla 2
def totales_fuente_dict(df_lst):
    """ Crea un diccionario con número total de registros de cada df """
    
    fuente_names = ['Museos', 'Bibliotecas', 'Cines']
    fuentes = {fuente_names[i]:df_lst[i].shape[0] for i in range(3)}
    
    return fuentes


def add_to_dict(dictionary, tipo_):
    """ Reemplaza valores en un diccionario por {'key': [val_anterior, otro_nuevo_val]} """
    
    for key, value in dictionary.items():    
        dictionary[key] = [value, tipo_]



