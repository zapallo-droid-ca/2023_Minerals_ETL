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
    
    
codes = df[['CmdCode','CmdDesc']].drop_duplicates()

index = ['Period','CmdDesc', 'ReporterISO', 'ReporterDesc' , 'FlowDesc']
target = 'PrimaryValue'
category_var ='CmdCode'
category = 2709

data = df[df[category_var] == category].copy()[index + [target]].reset_index(drop = True)
data = data.groupby(index).agg(value = (target,'sum')).reset_index()

data['value'].sum()
df[target].sum()

df_test = df_test.groupby(['Period','FlowDesc']).agg(value = ('value','sum')).reset_index().sort_values('Period', ascending = False)
df_test[df_test['Period'] == 2020]['value'] / 1000000


df_test[df_test['Period'] == 2020]['value'][0]  + df_test[df_test['Period'] == 2020]['value'][1]
df_test['value'].sum()
df[target].sum()

df = pd.read_csv(wd_in + '/2020.csv', sep = ',')




df.iloc[:,:10].drop_duplicates().shape

df[index + [target]].drop_duplicates()[target].sum()

data[index].drop_duplicates().shape
















