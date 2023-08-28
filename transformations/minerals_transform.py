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


main_wd = os.getcwd()[:os.getcwd().find('2023.Minerals-ETL') + len('2023.Minerals-ETL') +1]
os.chdir(main_wd)
os.chdir([x for x in json.load(open('./config/config.json',))[0]['directory'] if 'wd_custom_libraries' in x][0]['wd_custom_libraries'])    
import aux_transform
import aux_nlp_normalizers as aux_nlp
os.chdir(main_wd)

##-- Work Directory
   
wd_in = [x for x in json.load(open('./config/config.json',))[0]['output_data'] if 'root' in x][0]['root'] + '/processed_data/'
wd_out = [x for x in json.load(open('./config/config.json',))[0]['output_data'] if 'root' in x][0]['root'] + '/processed_data/'

csvAttr_imp = json.load(open('./config/config.json',))[0]['csvAttr_imp'][0]
csvAttr_exp = json.load(open('./config/config.json',))[0]['csvAttr_exp'][0]

##-- Configuration
axu_cols = ['quantity_Export', 'quantity_Import', 'weight_Export', 'weight_Import', 'value_Export', 'value_Import', 'key']

##-- Data
df_base = pd.read_csv(wd_out + '/df_prod_agg.csv.gz', sep = csvAttr_imp['sep'], encoding =  csvAttr_imp['encoding'])
df_aux = pd.read_csv(wd_out + '/df_trade_pivot.csv.gz', sep = csvAttr_imp['sep'], encoding =  csvAttr_imp['encoding'])[axu_cols]

df = df_base.merge(df_aux, how = 'left', on = 'key')

axu_cols.remove('key')
df['trade_isna'] = df.apply(lambda x: True if x[axu_cols].sum() == 0 else False, axis = 1)
df.fillna(0, inplace = True)

##-- Exporting
df.to_csv(wd_out + '/ft_minerals.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])

