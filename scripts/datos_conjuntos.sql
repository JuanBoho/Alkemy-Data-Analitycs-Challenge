CREATE TABLE IF NOT EXISTS public.datos_conjuntos
(
    "cod_localidad" integer,
    "id_provincia" integer,
    "id_departamento" integer,
    "categoria" character varying(50),
    "provincia" character varying(50),
    "localidad" character varying(50),
    "nombre" character varying(50),
    "domicilio" character varying(50),
    "cod_postal" character varying(50),
    "num_telefono" character varying(50),
    "mail" character varying(50),
    "web" character varying(50),
    "Fecha de carga" date,
    PRIMARY KEY ("nombre")
);