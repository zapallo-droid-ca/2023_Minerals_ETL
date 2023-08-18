##-- Commodities Production. Data Extraction
"""
ukgs_extract.py doc:
    this file contains the extraction function to extract the raw data from the defined source in
    the utility file config.json
    
    The utility file config.json contains the directory paths and the parameters to data extraction,
    including the codes to the requests of the UKGS data.
"""

##-- Libraries and Packages
import requests
from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import time


##-- Work Directory
source = 'ukgs'

main_wd = os.getcwd()[:os.getcwd().find('2023.Minerals-ETL') + len('2023.Minerals-ETL') +1]
os.chdir(main_wd)

os.chdir(json.load(open('./config/config.json',))[0]['directory'][0]['wd_root'])

wd_out = [x for x in json.load(open('./config/config.json',))[0]['raw_data'] if source in x][0][source]
wd_extract = [x for x in json.load(open('./config/config.json',))[0]['logs'] if 'extract' in x][0]['extract']
csvAttr_exp = json.load(open('./config/config.json',))[0]['csvAttr_exp'][0]


##-- Configuration
ukgs_root = 'https://www2.bgs.ac.uk/mineralsUK/statistics/'
ukgs_defi = 'wms.cfc?method=listResults&dataType=Production&'
ukgs_cmty = '' #to be created in the loop
ukgs_date = '' #to be created in the loop
ukgs_ctry = '&country=&agreeToTsAndCs=agreed'

sleep_time = 2


##-- Resources
commodities = pd.read_json(main_wd + './config/ukgs_aux.json')[['commodity_code','commodity_desc']]
commodities = list(zip(commodities['commodity_code'],commodities['commodity_desc']))
periods = [(2012,2021),(2002,2011),(1992,2001),(1982,1991),(1972,1981),(1970,1971)]

df = pd.DataFrame()

for commodity in commodities:
    print(f'starting {commodity} extraction')
    
    commodity_code = commodity[0]
    commodity_desc = commodity[1]
    
    ukgs_cmty = f'commodity={commodity_code}&'
       
    for period in periods:
        date_from = period[0]
        date_to = period[1]
        
        ukgs_date = f'dateFrom={date_from}&dateTo={date_to}'
            
        #Requesting data from source
        r = requests.get(ukgs_root+ukgs_defi+ukgs_cmty+ukgs_date+ukgs_ctry)
        
        #HTML parsing
        soup = BeautifulSoup(r.text, 'html.parser')
        table = soup.find('table', class_='bodyTable')
        
        #Rows and columns
        try:
            table_rows = table.find_all('tr') #rows
                
            header_row = table.find("tr") #columns
            column_names = [header.get_text(strip=True) for header in header_row.find_all(["th", "td"])]
            
            #Unpacking HTML format
            table_temp = []
            for row in table_rows:
                cells = row.find_all("td")
                row_data = [cell.get_text(strip=True) for cell in cells]
                
                if len(row_data) != 0:
                    table_temp.append(row_data)
            
            #Transforming class to DataFrame
            table_temp = pd.DataFrame(table_temp)
            
            #Keeping only index and columns with values
            col_index = [0,1] + list(range(3,(len(column_names[2:])+1)*2,2))  
            table_temp = table_temp[col_index].copy()
            table_temp.columns = column_names
            
            #Creating dimensions related to the commodity description
            table_temp['commodity_code'] = commodity_code
            table_temp['commodity_desc'] = commodity_desc
            
            #Dataset transformation into vertical format
            table_temp = table_temp.melt(id_vars = ['Country','commodity_code','commodity_desc','Sub-commodity'], var_name = 'year')
            table_temp.to_csv(wd_extract +  f'{commodity_code}-{date_from}to{date_to}.csv', index = False, sep = csvAttr_exp['sep'], encoding = csvAttr_exp['encoding'])
            
            #Appending to main dataset
            df = pd.concat([df,table_temp])
            
            del(table_temp)
            
        except:
            pass
        
        print(f'period {period} of {commodity} has been extracted')
        
        time.sleep(sleep_time) #waiting time before requesting a new query
    time.sleep(sleep_time) #waiting time before requesting a new query

df.reset_index(drop = True, inplace = True)

df.to_csv(wd_out + 'df_ukgs.csv.gz', index = False, sep = csvAttr_exp['sep'], encoding = csvAttr_exp['encoding'])



