-- BASE DE DATOS "bigdata"


-- DROP TABLE IF EXISTS public.temporal;
CREATE TABLE IF NOT EXISTS public.temporal(
    codigo_dep character varying(10) COLLATE pg_catalog."default",
    codigo_mun character varying(10) COLLATE pg_catalog."default",
    codigo_region integer,
    departamento text COLLATE pg_catalog."default",
    municipio text COLLATE pg_catalog."default",
    region text COLLATE pg_catalog."default"
);

-- Table: public.departamentos
-- DROP TABLE IF EXISTS public.departamentos;
CREATE TABLE IF NOT EXISTS public.departamentos(
    id_departamento integer NOT NULL,
    nombre character varying(70) COLLATE pg_catalog."default" NOT NULL,
    abb character varying(3) COLLATE pg_catalog."default",
    codigo_dane character varying(10) COLLATE pg_catalog."default" DEFAULT ''::character varying,
    codigo_region integer DEFAULT 0,
    poblacion integer DEFAULT 0,
    CONSTRAINT departamentos_pkey PRIMARY KEY (id_departamento)
);

-- DROP TABLE IF EXISTS public.municipios;
CREATE TABLE IF NOT EXISTS public.municipios(
    id_departamento integer NOT NULL,
    id_municipio integer NOT NULL,
    nombre character varying(70) COLLATE pg_catalog."default" NOT NULL,
    abb character varying(5) COLLATE pg_catalog."default",
    codigo_dane character varying(10) COLLATE pg_catalog."default" DEFAULT ''::character varying,
    poblacion integer DEFAULT 0,
    CONSTRAINT municipios_pkey PRIMARY KEY (id_municipio)
);

-- DROP TABLE IF EXISTS public.productos;
CREATE TABLE IF NOT EXISTS public.productos(
    id_producto integer NOT NULL,
    nombre character varying(20) COLLATE pg_catalog."default" NOT NULL,
    precio integer DEFAULT 0
);

-- DROP TABLE IF EXISTS public.operaciones;
CREATE TABLE IF NOT EXISTS public.operaciones(
    id_registro integer NOT NULL,
    id_departamento integer NOT NULL,
    id_municipio integer NOT NULL,
    id_producto integer NOT NULL,
    fecha character varying(10), 
    cantidad integer DEFAULT 0,
    estado character varying(1)
);


-- Productos 
INSERT INTO productos (id_producto,nombre,precio)
VALUES
    (1,'COLOMBIANITA',1000),
    (2,'MANZALOCA',    900),
    (3,'MANGOSON',     700),
    (4,'NARANJITA',    500);


-- Esta tabla se utiliza EXCLUSIVAMENTE PARA LOS CÁLCULOS DE TIEMPOS
-- NO SE UTILIZA PARA LAS CONSULTAS NI LOS GRÁFICOS
-- DROP TABLE IF EXISTS public.tamanio;
CREATE TABLE IF NOT EXISTS public.tamanio(
    id_registro integer NOT NULL,
    id_departamento integer NOT NULL,
    id_municipio integer NOT NULL,
    id_producto integer NOT NULL,
    fecha character varying(10), 
    cantidad integer DEFAULT 0,
    estado character varying(1)
);



-- INICIO TRABAJO ESTUDIANTE -- NO CORRER ETL.PY TODAVIA (PORQUE ETL ACTUALIZA operaciones.id_region, y no se ha creado)

-- 1. PASO DE EXTRACCION!!!!!

-- DROP TABLE IF EXISTS public.regiones;
CREATE TABLE IF NOT EXISTS public.regiones(
    id_region integer NOT NULL,
    nombre character varying(70) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT region_pkey PRIMARY KEY (id_region)
);


---INSERT REGIONES
TRUNCATE public.regiones;

INSERT INTO public.regiones (id_region, nombre) VALUES (571, 'Región Eje Cafetero - Antioquia');
INSERT INTO public.regiones (id_region, nombre) VALUES (572, 'Región Centro Oriente');
INSERT INTO public.regiones (id_region, nombre) VALUES (573, 'Región Centro Sur');
INSERT INTO public.regiones (id_region, nombre) VALUES (574, 'Región Caribe');
INSERT INTO public.regiones (id_region, nombre) VALUES (575, 'Región Llano');
INSERT INTO public.regiones (id_region, nombre) VALUES (576, 'Región Pacífico');

--ALTER de tabla operaciones

ALTER table if exists public.operaciones
add id_region integer;

-- 2. PASO DE LIMPIEZA DE DATOS!!!!! NO CORRER ETL.PY TODAVIA (PORQUE ETL ACTUALIZA operaciones.id_region, y ya se creó, pero algunos datos de departamento no están limpios)

-- LIMPIAR FECHAS EN REGISTROS DE OPERACIONES
-- DATOS A LIMPIAR SE ENCUENTRAN EN LAS SIGUIENTE CONSULTA SELECT

SELECT * FROM operaciones WHERE fecha !~ '^\d{4}-\d{2}-\d{2}$' 
or id_departamento = 0
or id_producto = 0

-- VISTA PREVIA SOLUCION DE FECHAS
SELECT fecha,
CASE
    WHEN fecha ~ '^\d{8}$' AND SUBSTRING(fecha FROM 5 FOR 4) = '2023' THEN 
        SUBSTRING(fecha FROM 5 FOR 4) || '-' || SUBSTRING(fecha FROM 3 FOR 2) || '-' || SUBSTRING(fecha FROM 1 FOR 2)
    WHEN fecha ~ '^\d{8}$' AND SUBSTRING(fecha FROM 1 FOR 4) = '2023' THEN 
        SUBSTRING(fecha FROM 1 FOR 4) || '-' || SUBSTRING(fecha FROM 5 FOR 2) || '-' || SUBSTRING(fecha FROM 7 FOR 2)
    WHEN fecha ~ '^\d{2}-\d{2}-\d{2}$' AND SUBSTRING(fecha FROM 1 FOR 2) = '23' THEN 
        '20' || SUBSTRING(fecha FROM 1 FOR 2) || '-' || SUBSTRING(fecha FROM 4 FOR 2) || '-' || SUBSTRING(fecha FROM 7 FOR 2)
    WHEN fecha ~ '^\d{2}-\d{2}-\d{2}$' AND SUBSTRING(fecha FROM 7 FOR 2) = '23' THEN 
        '20' || SUBSTRING(fecha FROM 7 FOR 2) || '-' || SUBSTRING(fecha FROM 4 FOR 2) || '-' || SUBSTRING(fecha FROM 1 FOR 2)
    WHEN fecha ~ '^\d{2}-\d{2}-\d{4}$' THEN 
        SUBSTRING(fecha FROM 7 FOR 4) || '-' || SUBSTRING(fecha FROM 4 FOR 2) || '-' || SUBSTRING(fecha FROM 1 FOR 2)
    WHEN fecha ~ '^\d{6}$' AND SUBSTRING(fecha FROM 1 FOR 2) = '23' THEN
        '20' || SUBSTRING(fecha FROM 1 FOR 2) || '-' || SUBSTRING(fecha FROM 3 FOR 2) || '-' || SUBSTRING(fecha FROM 5 FOR 2)
    WHEN fecha ~ '^\d{6}$' AND SUBSTRING(fecha FROM 5 FOR 2) = '23' THEN
        '20' || SUBSTRING(fecha FROM 5 FOR 2) || '-' || SUBSTRING(fecha FROM 3 FOR 2) || '-' || SUBSTRING(fecha FROM 1 FOR 2)
    ELSE fecha 
END as fecha_limpia from (SELECT * FROM operaciones WHERE fecha !~ '^\d{4}-\d{2}-\d{2}$');

--INSERTAR PRODUCTO con id_producto 0, para operaciones con producto indefinido
--UPDATE de las fechas con formato arreglado
--UPDATE del id_departamento donde este campo esté vacío

INSERT INTO productos (id_producto,nombre,precio) VALUES (0,'indefinido',0)

--INICIO QUERY LARGO

UPDATE operaciones
set fecha = CASE
    WHEN fecha ~ '^\d{8}$' AND SUBSTRING(fecha FROM 5 FOR 4) = '2023' THEN 
        SUBSTRING(fecha FROM 5 FOR 4) || '-' || SUBSTRING(fecha FROM 3 FOR 2) || '-' || SUBSTRING(fecha FROM 1 FOR 2)
    WHEN fecha ~ '^\d{8}$' AND SUBSTRING(fecha FROM 1 FOR 4) = '2023' THEN 
        SUBSTRING(fecha FROM 1 FOR 4) || '-' || SUBSTRING(fecha FROM 5 FOR 2) || '-' || SUBSTRING(fecha FROM 7 FOR 2)
    WHEN fecha ~ '^\d{2}-\d{2}-\d{2}$' AND SUBSTRING(fecha FROM 1 FOR 2) = '23' THEN 
        '20' || SUBSTRING(fecha FROM 1 FOR 2) || '-' || SUBSTRING(fecha FROM 4 FOR 2) || '-' || SUBSTRING(fecha FROM 7 FOR 2)
    WHEN fecha ~ '^\d{2}-\d{2}-\d{2}$' AND SUBSTRING(fecha FROM 7 FOR 2) = '23' THEN 
        '20' || SUBSTRING(fecha FROM 7 FOR 2) || '-' || SUBSTRING(fecha FROM 4 FOR 2) || '-' || SUBSTRING(fecha FROM 1 FOR 2)
    WHEN fecha ~ '^\d{2}-\d{2}-\d{4}$' THEN 
        SUBSTRING(fecha FROM 7 FOR 4) || '-' || SUBSTRING(fecha FROM 4 FOR 2) || '-' || SUBSTRING(fecha FROM 1 FOR 2)
    WHEN fecha ~ '^\d{6}$' AND SUBSTRING(fecha FROM 1 FOR 2) = '23' THEN
        '20' || SUBSTRING(fecha FROM 1 FOR 2) || '-' || SUBSTRING(fecha FROM 3 FOR 2) || '-' || SUBSTRING(fecha FROM 5 FOR 2)
    WHEN fecha ~ '^\d{6}$' AND SUBSTRING(fecha FROM 5 FOR 2) = '23' THEN
        '20' || SUBSTRING(fecha FROM 5 FOR 2) || '-' || SUBSTRING(fecha FROM 3 FOR 2) || '-' || SUBSTRING(fecha FROM 1 FOR 2)
    ELSE fecha 
END
WHERE fecha !~ '^\d{4}-\d{2}-\d{2}$' AND fecha IS NOT NULL;

--FIN QUERY LARGO

UPDATE operaciones SET id_departamento = CAST(SUBSTRING(CAST(id_municipio AS TEXT) FROM 1 FOR 4) AS INTEGER) WHERE id_departamento = 0


-- 3. PASO DE CARGA DE DATOS!!!!! #IMPORTANTE CORRER ARCHIVO ETL.PY EN ESTE PUNTO 

-- TABLA QUE MUESTRA VENTAS DIARIAS DE PRODUCTOS POR MUNICIPIO Y DEPARTAMENTO

--Crear tabla

CREATE TABLE IF NOT EXISTS public.ventas_diarias_productos(
    id_registro serial NOT NULL PRIMARY KEY,
	fecha varchar(10),
    nombre_producto character varying(70) COLLATE pg_catalog."default" NOT NULL,
	ventas_totales_dia INTEGER, 
	nombre_municipio character varying(70) COLLATE pg_catalog."default" NOT NULL,
	nombre_departamento character varying(70) COLLATE pg_catalog."default" NOT NULL
);

TRUNCATE ventas_diarias_productos;

INSERT INTO ventas_diarias_productos (fecha, nombre_producto, ventas_totales_dia, nombre_municipio, nombre_departamento)
SELECT 
    operaciones.fecha, 
    productos.nombre AS nombre_producto,
    SUM(cantidad) AS ventas_totales_dia,  -- Alineado con la columna del INSERT
    municipios.nombre AS nombre_municipio,
    departamentos.nombre AS nombre_departamento
FROM operaciones 
INNER JOIN municipios ON municipios.id_municipio = operaciones.id_municipio
INNER JOIN departamentos ON departamentos.id_departamento = operaciones.id_departamento
INNER JOIN productos ON productos.id_producto = operaciones.id_producto
GROUP BY productos.nombre, municipios.nombre, departamentos.nombre, operaciones.fecha
ORDER BY operaciones.fecha DESC, productos.nombre ASC, municipios.nombre ASC, departamentos.nombre ASC;

---------------------------------------------------
-- VISTA PREVIA TABLA
select * FROM ventas_diarias_productos


--FIN TRABAJO ESTUDIANTE


select SUM(cantidad), productos.nombre, regiones.nombre from operaciones
INNER JOIN productos ON productos.id_producto = operaciones.id_producto
INNER JOIN regiones ON operaciones.id_region = regiones.id_region
 group by regiones.id_region, productos.nombre


