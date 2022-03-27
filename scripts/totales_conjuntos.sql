CREATE TABLE IF NOT EXISTS public.totales_conjuntos
(
    "tipo" character varying(40),
    "item" character varying(40),
    "total_registros" integer,
    "Fecha" date,
    PRIMARY KEY ("tipo")
)