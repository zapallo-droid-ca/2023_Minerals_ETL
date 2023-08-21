##-- Commodities Production. Data Extraction
"""
ukgs_extract.py doc:
    this file contains the extraction function to extract the raw data from the defined source in
    the utility file config.json
    
    The utility file config.json contains the directory paths and process parameters.
"""

##-- Libraries and Packages
import os
import json
import pandas as pd
import sqlite3

main_wd = os.getcwd()[:os.getcwd().find('2023.Minerals-ETL') + len('2023.Minerals-ETL') +1]
os.chdir(main_wd)
os.chdir([x for x in json.load(open('./config/config.json',))[0]['directory'] if 'wd_custom_libraries' in x][0]['wd_custom_libraries'])    
import aux_load
os.chdir(main_wd)

##-- Work Directory  
wd_in = [x for x in json.load(open('./config/config.json',))[0]['output_data'] if 'processed' in x][0]['processed']
wd_out = [x for x in json.load(open('./config/config.json',))[0]['output_data'] if 'loaded' in x][0]['loaded']
csvAttr_imp = json.load(open('./config/config.json',))[0]['csvAttr_imp'][0]
csvAttr_exp = json.load(open('./config/config.json',))[0]['csvAttr_exp'][0]

##-- Reading Data
# Fact Tables
ft_production= pd.read_csv(wd_in + 'df_prod_agg.csv.gz', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])
ft_production = ft_production[['key','country_code','mineral_code','unit_code','year','value']].copy() #Reordering

ft_trade = pd.read_csv(wd_in + 'df_trade_pivot.csv.gz', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])
ft_trade = ft_trade[['key','country_code','mineral_code','year','unit_code','quantity_Import','quantity_Export','value_Export','value_Import']]

#Dimensions
dim_trade_flow = pd.read_csv(wd_in + 'dim_trade_flow.csv.gz', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])

dim_country = pd.read_csv(wd_in + 'dim_country.csv.gz', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])

dim_unit = pd.read_csv(wd_in + 'dim_trade_unit.csv.gz', sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding'])

dim_mineral = pd.read_json(main_wd + './config/uncomtrad_aux.json').drop(columns = 'commodity_code')

#Calendar:
min_year = min([min(ft_production.year),min(ft_trade.year)])
max_year = max([max(ft_production.year),max(ft_trade.year)])

dim_calendar = pd.DataFrame(list(range(min_year,max_year,1)), columns = ['year'])

##-- To .csv.gz backup
dim_calendar.to_csv(wd_out + 'dim_calendar.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding = csvAttr_exp['encoding'])
dim_country.to_csv(wd_out + 'dim_country.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding = csvAttr_exp['encoding'])
dim_mineral.to_csv(wd_out + 'dim_mineral.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding = csvAttr_exp['encoding'])
dim_trade_flow.to_csv(wd_out + 'dim_trade_flow.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding = csvAttr_exp['encoding'])
dim_unit.to_csv(wd_out + 'dim_unit.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding = csvAttr_exp['encoding'])
ft_production.to_csv(wd_out + 'ft_production.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding = csvAttr_exp['encoding'])
ft_trade.to_csv(wd_out + 'ft_trade.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding = csvAttr_exp['encoding'])

##-- Preparing data to load (list of tuples)
dim_calendar = [tuple(x) for x in dim_calendar.itertuples(index = False)]
dim_country = [tuple(x) for x in dim_country.itertuples(index = False)]
dim_mineral = [tuple(x) for x in dim_mineral.itertuples(index = False)]
dim_trade_flow = [tuple(x) for x in dim_trade_flow.itertuples(index = False)]
dim_unit = [tuple(x) for x in dim_unit.itertuples(index = False)]
ft_production = [tuple(x) for x in ft_production.itertuples(index = False)]
ft_trade = [tuple(x) for x in ft_trade.itertuples(index = False)]


##-- DB Connection
conn = sqlite3.connect(main_wd + '/db/minerals_db.db')

tables_to_create = ['dim_country','dim_mineral','dim_calendar',
                    'dim_unit','dim_trade_flow','ft_production',
                    'ft_trade']

for table_name in tables_to_create:
    aux_load.table_creation(table_name = table_name,
                            query_path = main_wd + f'./config/create_{table_name}.sql',
                            conn = conn)
    
conn.commit()
conn.close()

##-- DB Loading
conn = sqlite3.connect(main_wd + '/db/minerals_db.db')
cursor = conn.cursor()

cursor.executemany("INSERT INTO dim_calendar VALUES (?)", dim_calendar)
print('dim_calendar loaded')
cursor.executemany("INSERT INTO dim_country VALUES (?,?,?)", dim_country)
print('dim_country loaded')
cursor.executemany("INSERT INTO dim_mineral VALUES (?,?)", dim_mineral)
print('dim_mineral loaded')
cursor.executemany("INSERT INTO dim_trade_flow VALUES (?,?)", dim_trade_flow)
print('dim_trade_flow loaded')
cursor.executemany("INSERT INTO dim_unit VALUES (?,?)", dim_unit)
print('dim_unit loaded')
cursor.executemany("INSERT INTO ft_production VALUES (?,?,?,?,?,?)", ft_production)
print('ft_production loaded')
cursor.executemany("INSERT INTO ft_trade VALUES (?,?,?,?,?,?,?,?,?)", ft_trade)
print('ft_trade loaded')

conn.commit()

conn.close()





