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

##-- Imputing Units - The most common unit is kg for all the minerals


t = df[df['ReporterISO'] == 'IRQ'].copy()



df_temp_units = df[['CmdCode','CmdDesc','QtyUnitCode','QtyUnitAbbr']].drop_duplicates()

df['QtyUnitCode'] = np.where(df['QtyUnitCode'] == -1, 8, df['QtyUnitCode'])
df['QtyUnitAbbr'] = np.where(df['QtyUnitAbbr'].isna(), 'kg', df['QtyUnitAbbr'])

##--former_states
#There are some countries that needs to be added to a major one
former_states_i = {'SCG':'SRB','ANT':'NLD','GLP':'FRA','GUF':'FRA','MYT':'FRA','REU':'FRA','ZA1':'ZAF','S19':'CHN'}
former_states_c = {'SCG':'688','ANT':'528','GLP':'251','GUF':'251','MYT':'251','REU':'251','ZA1':'710','S19':'156'}
former_states_n = {'SCG':'Serbia','ANT':'Netherlands','GLP':'France','GUF':'France','MYT':'France','REU':'France','ZA1':'South Africa','S19':'China'}

df['ReporterISO'] =  df['ReporterISO'].replace(former_states_i)
df['ReporterCode'] = df['ReporterCode'].apply(lambda x: x.replace(former_states_c[x]) if x in former_states_c else x)
df['ReporterDesc'] = df['ReporterDesc'].apply(lambda x: x.replace(former_states_n[x]) if x in former_states_n else x)

##-- Creating Dimension for countries
dim_country = df.copy()[['ReporterCode', 'ReporterISO', 'ReporterDesc']].drop_duplicates().reset_index(drop = True)






























##-- Consolidating mineral codes
df = df.rename(columns = {'CmdCode':'commodity_code'}).merge(aux_minerals, on = 'commodity_code', how = 'left')
df.drop(columns = 'commodity_code', inplace = True)

##-- Units transformations
#- kgs to tonnes
df['Qty'] = np.where(df['QtyUnitCode'] == 8, df['Qty'] / 1000, df['Qty'] )
df['QtyUnitAbbr'] = np.where(df['QtyUnitCode'] == 8, 'tonnes' , df['QtyUnitAbbr'] )
df['QtyUnitCode'] = np.where(df['QtyUnitCode'] == 8, 1 , df['QtyUnitCode'] )

#- un to l #assuming that u is equivalent to l, there is not detail about
df['Qty'] = np.where(df['QtyUnitCode'] == 8, df['Qty'] / 1000, df['Qty'] )
df['QtyUnitAbbr'] = np.where(df['QtyUnitCode'] == 5, 'l' , df['QtyUnitAbbr'] )
df['QtyUnitCode'] = np.where(df['QtyUnitCode'] == 5, 7 , df['QtyUnitCode'] )


# source: https://www.engineeringtoolbox.com/fossil-fuels-energy-content-d_1298.html
avg_e_content_gas_m = 40.60 #MJ/m3 Natural gas (US marked) #Gross Heating Value / PCS
avg_e_content_gas_L = avg_e_content_gas_m / 1000  #liters Natural gas (US marked) #Gross Heating Value / PCS
avg_e_content_gas_k = 52.2 #kg Natural gas (US marked) #Gross Heating Value / PCS
avg_e_content_gas_t = avg_e_content_gas_k / 1000 #tonnes

avg_e_content_pet_m = 40.60 #m3 Natural gas (US marked) #Gross Heating Value / PCS
avg_e_content_pet_L = avg_e_content_pet_m / 1000 #liters // crude oil (US marked) #Gross Heating Value / PCS
avg_e_content_pet_k = 45.5 #kg // crude oil (US marked) #Gross Heating Value / PCS
avg_e_content_pet_t = avg_e_content_pet_k / 1000 #tonnes

lithium_liters_in_tonne = 1872.66 #L/tonne

#cm13: Natural Gas ; cm14: Petroleum, cm04: lithium
#tn: 1, kg: 8, l: 7, u:5

#-Natural Gas - From tonne to toe
unit_code = 1 ; mineral_code = 'cm13' ; average_energy_content = avg_e_content_gas_t ; x_value_unit = 'MJ/tonne'

df['Qty'] = df.apply(lambda x: aux_transform.unit_to_toe(x['Qty'], x_value_unit, average_energy_content) if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['Qty'], axis=1)
df['QtyUnitAbbr'] = df.apply(lambda x: 'mtoe' if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['QtyUnitAbbr'], axis=1)
df['QtyUnitCode'] = df.apply(lambda x: 0 if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['QtyUnitCode'], axis=1)


#-Natural Gas - From L to toe
unit_code = 7 ; mineral_code = 'cm13' ; average_energy_content = avg_e_content_gas_L ; x_value_unit = 'MJ/L'

df['Qty'] = df.apply(lambda x: aux_transform.unit_to_toe(x['Qty'], x_value_unit, average_energy_content) if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['Qty'], axis=1)
df['QtyUnitAbbr'] = df.apply(lambda x: 'mtoe' if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['QtyUnitAbbr'], axis=1)
df['QtyUnitCode'] = df.apply(lambda x: 0 if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['QtyUnitCode'], axis=1)


#-Petroleum (Crude) - From tonne to toe
unit_code = 1 ; mineral_code = 'cm14' ; average_energy_content = avg_e_content_pet_t ; x_value_unit = 'MJ/tonne'

df['Qty'] = df.apply(lambda x: aux_transform.unit_to_toe(x['Qty'], x_value_unit, average_energy_content) if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['Qty'], axis=1)
df['QtyUnitAbbr'] = df.apply(lambda x: 'mtoe' if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['QtyUnitAbbr'], axis=1)
df['QtyUnitCode'] = df.apply(lambda x: 0 if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['QtyUnitCode'], axis=1)

#-Petroleum (Crude) - From L to toe
unit_code = 7 ; mineral_code = 'cm14' ; average_energy_content = avg_e_content_pet_L ; x_value_unit = 'MJ/L'

df['Qty'] = df.apply(lambda x: aux_transform.unit_to_toe(x['Qty'], x_value_unit, average_energy_content) if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['Qty'], axis=1)
df['QtyUnitAbbr'] = df.apply(lambda x: 'mtoe' if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['QtyUnitAbbr'], axis=1)
df['QtyUnitCode'] = df.apply(lambda x: 0 if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['QtyUnitCode'], axis=1)

#-Lithium - From L to tonne
unit_code = 7 ; mineral_code = 'cm04' ;

df['Qty'] = df.apply(lambda x: x['Qty'] / lithium_liters_in_tonne if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['Qty'], axis=1)
df['QtyUnitAbbr'] = df.apply(lambda x: 'tonnes' if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['QtyUnitAbbr'], axis=1)
df['QtyUnitCode'] = df.apply(lambda x: 1 if x['QtyUnitCode'] == unit_code and x['mineral_code'] == mineral_code else x['QtyUnitCode'], axis=1)


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

dim_trade_unit = dim_trade_unit.drop_duplicates().reset_index(drop = True)


##-- Exporting
df.to_csv(wd_out + 'processed_data/df_trade_agg.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])
df_pivot.to_csv(wd_out + 'processed_data/df_trade_pivot.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])

dim_trade_flow.to_csv(wd_out + 'processed_data/dim_trade_flow.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])
dim_trade_unit.to_csv(wd_out + 'processed_data/dim_trade_unit.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])

