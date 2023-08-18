##-- Commodities Trade. Data Extraction
"""
unct_extract.py doc:
    this file contains the extraction function to extract the raw data from the defined source in
    the utility file config.json
    
    The utility file config.json contains the directory paths and the parameters to data transformation,
    of the data extracted from United Nations Comtrade Database data.
"""

##-- Libraries and Packages
import os
import json
import chardet
import pandas as pd
import numpy as np

main_wd = os.getcwd()[:os.getcwd().find('2023.Minerals-ETL') + len('2023.Minerals-ETL') +1]
os.chdir(main_wd)
os.chdir([x for x in json.load(open('./config/config.json',))[0]['directory'] if 'wd_custom_libraries' in x][0]['wd_custom_libraries'])    
import aux_transform
os.chdir(main_wd)

##-- Work Directory
source = 'unct'
   
wd_in = [x for x in json.load(open('./config/config.json',))[0]['raw_data'] if source in x][0][source]
wd_out = [x for x in json.load(open('./config/config.json',))[0]['output_data'] if 'root' in x][0]['root']
csvAttr_imp = json.load(open('./config/config.json',))[0]['csvAttr_imp'][0]
csvAttr_exp = json.load(open('./config/config.json',))[0]['csvAttr_exp'][0]

##-- Configuration
files_in_dir = aux_transform.filesinpath_funct(path = wd_in)

##-- Raw Data
aux_minerals = pd.read_json(main_wd + './config/uncomtrad_aux.json')[['mineral_code','commodity_code']]

df = pd.DataFrame()

counter = 0
for file in files_in_dir.file:
    counter += 1
    print(f'loading file {counter} of {files_in_dir.shape[0]}')
    with open(wd_in + file, 'rb') as f:
        raw_data = f.read()
        
    encodingType = chardet.detect(raw_data)['encoding']
    
    df_temp = pd.read_csv(wd_in + file, encoding = encodingType)
    
    df = pd.concat([df,df_temp])
    del(df_temp)
    print(f'file {counter} of {files_in_dir.shape[0]} loaded')

##-- Creating Dimension for countries
dim_country = df.copy()[['ReporterCode', 'ReporterISO', 'ReporterDesc']].drop_duplicates().reset_index(drop = True)

##-- Consolidating mineral codes
df = df.rename(columns = {'CmdCode':'commodity_code'}).merge(aux_minerals, on = 'commodity_code', how = 'left')
df.drop(columns = 'commodity_code', inplace = True)

##-- kgs to tonnes

df['Qty'] = np.where(df['QtyUnitCode'] == 8, df['Qty'] / 1000, df['Qty'] )
df['QtyUnitCode'] = np.where(df['QtyUnitCode'] == 8, 1 , df['QtyUnitCode'] )
df['QtyUnitAbbr'] = np.where(df['QtyUnitCode'] == 8, 'tonnes' , df['QtyUnitCode'] )

##-- Aggregatting data:
index_cols = ['ReporterISO','FlowCode','FlowDesc', 'QtyUnitAbbr',
              'Period', 'mineral_code', 'QtyUnitCode']

df = df.groupby(index_cols).agg(quantity = ('Qty','sum'), 
                                weight = ('NetWgt','sum'), 
                                value = ('PrimaryValue','sum')).reset_index()

##-- Pivoting data:
index_ = ['ReporterISO','mineral_code','Period','QtyUnitCode','QtyUnitAbbr']
columns_ = ['FlowDesc']
values_ = ['quantity', 'weight','value']

df_pivot = aux_transform.df_pivot_wnames(data = df, index_ = index_, columns_ = columns_, values_ = values_) 

df_pivot.rename(columns = {'ReporterISO':'country_code',
                           'Period':'year',
                           'QtyUnitCode':'unit_code',
                           'QtyUnitAbbr':'unit_desc'}, inplace = True)

df.rename(columns = {'QtyUnitCode':'unit_code',
                     'QtyUnitAbbr':'unit_desc',
                     'FlowCode':'flow_code',
                     'FlowDesc':'flow_desc'}, inplace = True)

df_pivot['key'] = df_pivot['year'].astype(str) + '-' + df_pivot['unit_code'].astype(str) + '-' + df_pivot['country_code'] + '-' + df_pivot['mineral_code']

##-- Creating Other Dimensions
dim_trade_flow = df.copy()[['flow_code', 'flow_desc']].drop_duplicates().reset_index(drop = True)
dim_trade_unit = df.copy()[['unit_code', 'unit_desc']].drop_duplicates().reset_index(drop = True)

##-- Exporting
df.to_csv(wd_out + 'processed_data/df_trade_agg.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])
df_pivot.to_csv(wd_out + 'processed_data/df_trade_pivot.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])

dim_trade_flow.to_csv(wd_out + 'processed_data/dim_trade_flow.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])
dim_trade_unit.to_csv(wd_out + 'processed_data/dim_trade_unit.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])

dim_trade_unit = pd.concat([dim_trade_unit,pd.DataFrame([{'unit_code':'1','unit_desc':'tonnes'}, {'unit_code':'0','unit_desc':'million cubic metres'}])])