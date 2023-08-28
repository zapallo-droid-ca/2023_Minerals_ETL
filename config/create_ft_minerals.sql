CREATE TABLE ft_minerals (
"key" VARCHAR(20) PRIMARY KEY, 
country_code VARCHAR(10), 
mineral_code VARCHAR(10), 
unit_code VARCHAR(10), 
"year" INTEGER, 
"value" NUMERIC,
quantity_import NUMERIC, 
quantity_export NUMERIC, 
value_export NUMERIC, 
value_import NUMERIC,
trade_isna BOOLEAN,
FOREIGN KEY ("year") REFERENCES dim_calendar ("year"),
FOREIGN KEY (mineral_code) REFERENCES dim_mineral (mineral_code),
FOREIGN KEY (country_code) REFERENCES dim_country (country_code),
FOREIGN KEY (unit_code) REFERENCES dim_unit (unit_code)
)






	







