CREATE TABLE ft_production (
"key" VARCHAR(20) PRIMARY KEY, 
country_code VARCHAR(10), 
mineral_code VARCHAR(10), 
"year" INTEGER, 
"value" NUMERIC,
FOREIGN KEY ("year") REFERENCES dim_calendar ("year"),
FOREIGN KEY (mineral_code) REFERENCES dim_mineral (mineral_code),
FOREIGN KEY (country_code) REFERENCES dim_country (country_code)
)






	







