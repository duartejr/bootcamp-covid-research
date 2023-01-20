COPY datawarehouse."DEMANDA_ENERGIA"(
	data, pais_id, "demanda_TWh")
FROM '/opt/bootcamp-covid/datalake/gold/demanda_energia_v2.csv'
DELIMITER ',';