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
source = 'ukgs'
   
wd_in = [x for x in json.load(open('./config/config.json',))[0]['raw_data'] if source in x][0][source]
wd_out = [x for x in json.load(open('./config/config.json',))[0]['output_data'] if 'root' in x][0]['root']
csvAttr_imp = json.load(open('./config/config.json',))[0]['csvAttr_imp'][0]
csvAttr_exp = json.load(open('./config/config.json',))[0]['csvAttr_exp'][0]

##-- Configuration
files_in_dir = aux_transform.filesinpath_funct(path = wd_in, extensions = ['csv','gz'])

##-- Raw Data
df = pd.read_csv(wd_in + files_in_dir.file[0], sep = csvAttr_imp['sep'], encoding = csvAttr_imp['encoding']).drop(columns = ['commodity_desc','Sub-commodity'])

#-Minerals
#aux_minerals = pd.read_json(main_wd + './config/ukgs_aux.json').drop(columns = 'unit')[['mineral_code','commodity_code']]
aux_minerals = pd.read_json(main_wd + './config/ukgs_aux.json')[['mineral_code','commodity_code','unit']].rename(columns = {'unit':'unit_code'})
aux_minerals['unit_code'] = aux_minerals['unit_code'].astype(str)
aux_minerals['unit_code'] = aux_minerals['unit_code'].replace({'tonnes':'1','million cubic metres':'x'})

#-Regions
aux_regions = pd.read_csv('./raw_data/aux_sources/curatedRegions.csv', sep = ',', encoding= csvAttr_imp['encoding'])[['Three_Letter_Country_Code','Continent_Name']].drop_duplicates()
aux_regions = aux_regions[aux_regions['Three_Letter_Country_Code'].isna() == False].reset_index(drop = True)
aux_regions.rename(columns = {'Three_Letter_Country_Code':'country_code','Continent_Name':'region_desc'}, inplace = True)
aux_regions = aux_regions[((aux_regions['country_code'] == 'CYP') & (aux_regions['region_desc'] == 'Asia')) == False].reset_index(drop = True)
aux_regions = aux_regions[((aux_regions['country_code'] == 'GEO') & (aux_regions['region_desc'] == 'Asia')) == False].reset_index(drop = True)
aux_regions = aux_regions[((aux_regions['country_code'] == 'AZE') & (aux_regions['region_desc'] == 'Asia')) == False].reset_index(drop = True)
aux_regions = aux_regions[((aux_regions['country_code'] == 'ARM') & (aux_regions['region_desc'] == 'Asia')) == False].reset_index(drop = True)
aux_regions = aux_regions[((aux_regions['country_code'] == 'RUS') & (aux_regions['region_desc'] == 'Asia')) == False].reset_index(drop = True)
aux_regions = aux_regions[((aux_regions['country_code'] == 'TUR') & (aux_regions['region_desc'] == 'Asia')) == False].reset_index(drop = True)
aux_regions = aux_regions[((aux_regions['country_code'] == 'KAZ') & (aux_regions['region_desc'] == 'Europe')) == False].reset_index(drop = True)
aux_regions = aux_regions[((aux_regions['country_code'] == 'UMI') & (aux_regions['region_desc'] == 'Oceania')) == False].reset_index(drop = True)

##-- Consolidating mineral codes
df = df.merge(aux_minerals, on = 'commodity_code', how = 'left')
df.drop(columns = 'commodity_code', inplace = True)

##-- Units transformations
# source: https://www.engineeringtoolbox.com/fossil-fuels-energy-content-d_1298.html
avg_e_content_gas_m = 40.60 #/ 1000000 #MJ/m3 Natural gas (US marked) #Gross Heating Value / PCS
avg_e_content_pet_k = 45.5 #MJ/kg // crude oil (US marked) #Gross Heating Value / PCS
avg_e_content_pet_t = avg_e_content_pet_k / 1000 #tonnes

#cm13: Natural Gas ; cm14: Petroleum
#tn: 1, kg: 8, l: 7, u:5

#-Natural Gas - From millions m3 to toe
unit_code = 'x' ; mineral_code = 'cm13' ; average_energy_content = avg_e_content_gas_m ; x_value_unit = 'MJ/millon m3'
df['value'] = df.apply(lambda x: aux_transform.unit_to_toe(x['value'], x_value_unit, average_energy_content) if x['unit_code'] == unit_code and x['mineral_code'] == mineral_code else x['value'], axis=1)
df['unit_code'] = df.apply(lambda x: 0 if x['unit_code'] == unit_code and x['mineral_code'] == mineral_code else x['unit_code'], axis=1)

#-Petroleum (Crude) - From tonne to toe
unit_code = '1' ; mineral_code = 'cm14' ; average_energy_content = avg_e_content_pet_t ; x_value_unit = 'MJ/tonne'
df['value'] = df.apply(lambda x: aux_transform.unit_to_toe(x['value'], x_value_unit, average_energy_content) if x['unit_code'] == unit_code and x['mineral_code'] == mineral_code else x['value'], axis=1)
df['unit_code'] = df.apply(lambda x: 0 if x['unit_code'] == unit_code and x['mineral_code'] == mineral_code else x['unit_code'], axis=1)


##-- Dealing with countries: 
df_countries = pd.DataFrame(df['Country'].drop_duplicates())
df_countries['country_short_name'] = df_countries['Country'].str.split(',').str[0]

# Reading df with ISO codes: (from other project)
aux_country = pd.read_csv('./raw_data/aux_sources/dim_country.csv', sep = csvAttr_imp['sep'], encoding= csvAttr_imp['encoding']).iloc[:,:-2]
aux_country['country_desc'] = aux_country.apply(lambda x: x['unComtrade_text'] if pd.isna(x['unComtrade_textNote']) else x['unComtrade_textNote'], axis=1)

#Normalizing values: Country name 
abbreviations = {
                    'dem': 'democratic',
                    'rep': 'republic',
                    'usd': 'united states',
                    'pdr': 'people democratic republic',
                    'usa': 'united states of america',
                    'soviet': 'russia'
                }

custom_stopwords = ['republic','democratic', 'federation', 'federal','federated','state','province','union']

aux_country['country_desc'] = aux_nlp.nlp_preprocess(data = aux_country['country_desc'], abrev_dict = abbreviations, stop_words_treatment = True, custom_stopwords = custom_stopwords)
aux_country = aux_country.copy()[['country_desc','alpha3ISO']]

df_countries['country_short_name'] = aux_nlp.nlp_preprocess(data = df_countries['country_short_name'], abrev_dict = abbreviations, stop_words_treatment = True, custom_stopwords = custom_stopwords)

df_countries_normalized = aux_nlp.names_normalizer_ensembled(x_input = df_countries['country_short_name'], y_compare = aux_country, y_text = 'country_desc', y_code = 'alpha3ISO')

##-- There are some issues with some countries, this should be upgraded in the future, there are even extinguish countries with no codes
#Manual check and corrections (the normalizer is using simple distance calculations)   
iter_values = [['united states america','USA'],['serbia montenegro','SRB'],['taiwan','TWN'],['yugoslavia','HRV'],['laos','LAO'],
               ['tanzania','TZA'],['east timor','TLS'],['sharjah','ARE'],['yemen people','YEM'],
               ['brunei','BRN'],['bolivia','BOL'],['zaire','COG'],['hong kong','CHN'],['switzerland','CHE'],
               ['rhodesia','ZWE'],['norway','NOR'],['new hebrides','VUT'],['ivory coast','CIV']]

for base_text, target_text in iter_values:
    df_countries_normalized.loc[df_countries_normalized['country_short_name'] == base_text,'code'] = target_text

df_countries = df_countries.merge(df_countries_normalized.iloc[:,:2].copy(), on = 'country_short_name', how = 'left').drop(columns = ['country_short_name'])

##-- Dealing with names in fact table
df = df.merge(df_countries, on = 'Country', how = 'left').rename(columns = {'Country':'country_code'})
df['country_code'] = df['code'] 
df.drop(columns = 'code', inplace = True)

##-- Creating Dimension
df_countries.rename(columns = {'Country':'country_desc','code':'country_code'}, inplace = True)
df_countries = df_countries.drop(columns = 'country_desc').drop_duplicates().reset_index(drop = True)
#Descriptions
df_countries = df_countries.merge(aux_country.rename(columns = {'alpha3ISO':'country_code'}), on = 'country_code', how = 'left')
#Regions
df_countries = df_countries.merge(aux_regions, on = 'country_code', how = 'left')
df_countries = df_countries[['country_code','country_desc','region_desc']]
#Other countries non producers

aux_country = aux_country.rename(columns = {'alpha3ISO':'country_code'}).merge(aux_regions, on = 'country_code', how = 'left')
aux_country = aux_country[['country_code','country_desc','region_desc']]

aux_country = aux_country[(aux_country['country_code'].isin(df_countries['country_code']) == False) & (aux_country['region_desc'].isna() == False)]
df_countries = pd.concat([df_countries,aux_country])
df_countries.sort_values(by = 'country_code', ascending = True, inplace = True)


del(aux_country, df_countries_normalized, custom_stopwords, abbreviations, iter_values)

#Creating Primary Keys
df['key'] = df['year'].astype(str) + '-' + df['unit_code'].astype(str) + '-' + df['country_code'] + '-' + df['mineral_code'].astype(str)

#Aggregating the fact table
df = df.groupby(['country_code', 'mineral_code','unit_code','year','key']).agg(value = ('value','sum')).reset_index()

##-- Exporting
df.to_csv(wd_out + 'processed_data/df_prod_agg.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])

df_countries.to_csv(wd_out + 'processed_data/dim_country.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding =  csvAttr_exp['encoding'])
