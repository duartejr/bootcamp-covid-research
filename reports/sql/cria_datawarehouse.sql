-- -----------------------------------------------------
-- Database bootcamp
-- -----------------------------------------------------
--CREATE DATABASE bootcamp
--    WITH
--    OWNER = airflow
--    ENCODING = 'UTF8'
--    CONNECTION LIMIT = -1
--   IS_TEMPLATE = False;


-- -----------------------------------------------------
-- Schema datawarehouse
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema datawarehouse
-- -----------------------------------------------------
--CREATE SCHEMA IF NOT EXISTS datawarehouse
--    AUTHORIZATION airflow;

-- -----------------------------------------------------
-- Table `datawarehouse`.`PAISES`
-- -----------------------------------------------------
DROP TABLE IF EXISTS PAISES;

CREATE TABLE PAISES
(
    "ID" serial NOT NULL,
    pais character varying(20) NOT NULL,
    PRIMARY KEY ("ID")
);

ALTER TABLE IF EXISTS PAISES
    OWNER to airflow;

-- -----------------------------------------------------
-- Table `datawarehouse`.`CALENDARIO`
-- -----------------------------------------------------
DROP TABLE IF EXISTS CALENDARIO;

CREATE TABLE CALENDARIO
(
    data date NOT NULL,
    PRIMARY KEY (data)
);

ALTER TABLE IF EXISTS CALENDARIO
    OWNER to airflow;


-- -----------------------------------------------------
-- Table `datawarehouse`.`COVID_JHONS_HOPKINS`
-- -----------------------------------------------------
DROP TABLE IF EXISTS COVID_JHONS_HOPKINS;

CREATE TABLE COVID_JHONS_HOPKINS
(
    "ID" serial NOT NULL,
    pais_id integer NOT NULL,
    data date NOT NULL,
    confirmados integer,
    mortes integer,
    recuperados integer,
    novos_casos integer,
    novas_mortes integer,
    ativos integer,
    PRIMARY KEY ("ID"),
    CONSTRAINT fk_pais_id FOREIGN KEY (pais_id)
        REFERENCES PAISES ("ID") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT fk_data FOREIGN KEY (data)
        REFERENCES CALENDARIO (data) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);

ALTER TABLE IF EXISTS COVID_JHONS_HOPKINS
    OWNER to airflow;


-- -----------------------------------------------------
-- Table `datawarehouse`.`HANK_FELICIDADE`
-- -----------------------------------------------------
DROP TABLE IF EXISTS HANK_FELICIDADE;

CREATE TABLE HANK_FELICIDADE
(
    "ID" serial NOT NULL,
    pais_id integer NOT NULL,
    ano integer,
    periodo_referencia character varying(20),
    nota double precision,
    classificacao integer,
    PRIMARY KEY ("ID"),
    CONSTRAINT fk_pais_id FOREIGN KEY (pais_id)
        REFERENCES PAISES ("ID") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);

ALTER TABLE IF EXISTS HANK_FELICIDADE
    OWNER to airflow;


-- -----------------------------------------------------
-- Table `datawarehouse`.`LOCKDOWNS`
-- -----------------------------------------------------
-- Table: datawarehouse.LOCKDOWNS

DROP TABLE IF EXISTS LOCKDOWNS;

CREATE TABLE IF NOT EXISTS LOCKDOWNS
(
    "ID" serial NOT NULL,
    pais_id integer NOT NULL,
    data_inicio date,
    data_fim date,
    total_dias integer,
    CONSTRAINT "LOCKDOWNS_pkey" PRIMARY KEY ("ID"),
    CONSTRAINT fk_pais_id FOREIGN KEY (pais_id)
        REFERENCES PAISES ("ID") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS LOCKDOWNS
    OWNER to airflow;


-- -----------------------------------------------------
-- Table `datawarehouse`.`SENTIMENTOS`
-- -----------------------------------------------------
DROP TABLE IF EXISTS SENTIMENTOS;

CREATE TABLE SENTIMENTOS
(
    "ID" serial NOT NULL,
    pais_id integer NOT NULL,
    data date NOT NULL,
    tweet text,
    negativo double precision,
    positivo double precision,
    neutro double precision,
    composto double precision,
    PRIMARY KEY ("ID"),
    CONSTRAINT fk_pais_id FOREIGN KEY (pais_id)
        REFERENCES PAISES ("ID") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT fk_data FOREIGN KEY (data)
        REFERENCES CALENDARIO (data) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);

ALTER TABLE IF EXISTS SENTIMENTOS
    OWNER to airflow;


-- -----------------------------------------------------
-- Table `datawarehouse`.`DEMANDA_ENERGIA`
-- -----------------------------------------------------
DROP TABLE IF EXISTS DEMANDA_ENERGIA;

CREATE TABLE DEMANDA_ENERGIA
(
    "ID" serial NOT NULL,
    pais_id integer NOT NULL,
    data date NOT NULL,
    "demanda_TWh" double precision,
    PRIMARY KEY ("ID"),
    CONSTRAINT fk_pais_id FOREIGN KEY (pais_id)
        REFERENCES PAISES ("ID") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT fk_data FOREIGN KEY (data)
        REFERENCES CALENDARIO (data) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);

ALTER TABLE IF EXISTS DEMANDA_ENERGIA
    OWNER to airflow;


-- -----------------------------------------------------
-- Table `datawarehouse`.`PREVISAO_CASOS`
-- -----------------------------------------------------
DROP TABLE IF EXISTS PREVISAO_CASOS;

CREATE TABLE IF NOT EXISTS PREVISAO_CASOS
(
    "ID" serial NOT NULL,
    pais_id integer NOT NULL,
    data date NOT NULL,
    data_prev_1 date,
    data_prev_2 date,
    data_prev_3 date,
    data_prev_4 date,
    data_prev_5 date,
    data_prev_6 date,
    data_prev_7 date,
    casos_prev_1 integer,
    casos_prev_2 integer,
    casos_prev_3 integer,
    casos_prev_4 integer,
    casos_prev_5 integer,
    casos_prev_6 integer,
    casos_prev_7 integer,
    CONSTRAINT "PREVISAO_CASOS_pkey" PRIMARY KEY ("ID"),
    CONSTRAINT fk_data FOREIGN KEY (data)
        REFERENCES CALENDARIO (data) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_pais_id FOREIGN KEY (pais_id)
        REFERENCES PAISES ("ID") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS PREVISAO_CASOS
    OWNER to airflow;
	
-- -----------------------------------------------------
-- Table `datawarehouse`.`COVID_WORLDOMETER`
-- -----------------------------------------------------
DROP TABLE IF EXISTS COVID_WORLDOMETER;

CREATE TABLE IF NOT EXISTS COVID_WORLDOMETER
(
    "ID" serial NOT NULL,
    pais_id integer NOT NULL,
    data date NOT NULL,
    populacao integer,
    total_testes integer,
    "mortes_1Mpop" double precision,
    "teste_1Mpop" double precision,
    "casos_1Mpop" double precision,
    letalidade double precision,
    fatalidade double precision,
    taxa_positividade double precision,
    percentual_recuperados double precision,
    percentual_mortes double precision,
    CONSTRAINT "COVID_WORLDOMETER_pkey" PRIMARY KEY ("ID"),
    CONSTRAINT fk_data FOREIGN KEY (data)
        REFERENCES CALENDARIO (data) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_pais_id FOREIGN KEY (pais_id)
        REFERENCES PAISES ("ID") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS COVID_WORLDOMETER
    OWNER to airflow;
