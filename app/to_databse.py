"""
Challenge: Data Analytics con Python - Alkemy.
Juan Bohórquez (2022)
https://github.com/JuanBoho/Alkemy-Data-Analitycs-Challenge
"""

from os import listdir
from os.path import isfile, join
from datetime import datetime
from decouple import config
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import logging as log



class TablasDB():
    """ Crea base de datos alkemy, crea y/o actualiza tablas
    """

    def __init__(self):
        self.config = config


    def db_config(self):

        try:
            db_config = {'user': self.config("USER"),
                        'password': self.config("PASSWORD"),
                        'host': self.config("HOST"),
                        'port': self.config("PORT")
            }
        except Exception as e:
            log.error("Error cofigurando db: {}".format(e))

        
        eng_temp = "postgresql://{user}:{password}@{host}:{port}/{db_name}"

        engine = (eng_temp.format(db_name='alkemy',**db_config))

        self.engine = create_engine(engine)


    def create_db(self):
        """ Crea base de datos si no existe.
        """

        try:
            if not database_exists(self.engine.url):
                create_database(self.engine.url)
                log.info('Creando base de datos...')
        except Exception as e:
            log.error('Error creando bd: {}'.format(e))


    def get_scripts(self, path):
        """ crea una lista con los archivos en path y agrega path a objeto"""
        
        self.script_path = path

        scr_names = [f for f in listdir(path) if isfile(join(path, f))]
        self.scripts_names = scr_names


    def create_tables(self):
        """ Crea tablas según archivos .sql
            scripts_lst: nombres de archivos .sql (list)
            path : ruta del directorio de los archivos (str)
        """

        for script in self.scripts_names:

            log.info('Creando tablas en bd...')

            try:
                with self.engine.connect() as c:
                    # f_name = "{}/{}".format(path,script) # ruta del archivo
                    file = open(self.script_path+script)
                    query = text(file.read())
                    c.execute(query)

            except Exception as e:
                log.error("Error creando tablas: {}".format(e))


    def actualizar_tablas(self, tables_):
        """ Atualiza tablas de la base de datos"""
        
        for ix in range(len(tables_)):
            # nombre sin ".sql" equivale a nombre de tabla
            tb_name = self.scripts_names[ix][:-4]
            tables_[ix]['Fecha_carga'] = datetime.now()
            try:
                tables_[ix].to_sql(tb_name, con=self.engine, if_exists="replace", index=False)
            except Exception as e:
                log.error("error al actulizar las tablas {}".format(e))


    def exe(self):
        self.db_config()
        self.create_db()
        self.get_scripts("./scripts/")
        self.create_tables()











