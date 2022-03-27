CREATE TABLE IF NOT EXISTS public.totales_cines
(
    "Provincia" character varying(40),
    "pantallas" integer,
    "butacas" integer,
    "espacio_incaa" integer,
    "Fecha" date,
    PRIMARY KEY ("Provincia")
)