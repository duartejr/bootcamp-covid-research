INSERT INTO datawarehouse."DEMANDA_ENERGIA"(
	"ID", pais_id, data, "demanda_TWh")
	VALUES (?, ?, ?, ?);
	
select * from datawarehouse."PAISES";

COPY datawarehouse."DEMANDA_ENERGIA"(
	data, pais_id, "demanda_TWh")
FROM 'D:\bootcamp-covid\datalake\gold\demanda_energia_v2.csv'
DELIMITER ','
CSV HEADER;