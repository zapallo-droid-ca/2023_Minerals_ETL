### Project: Mineral Commodity Survey

## Definitions:



## Main Sources:

| **Author** | **Source** |
|------------|------------|
|British Geological Survey (UK) | [World mineral statistics data](https://www2.bgs.ac.uk/mineralsuk/statistics/wms.cfc?method=searchWMS) |
|United Nations Comtrade DB | |[UN Comtrade Database](https://comtradeplus.un.org/) |


## Sources Characteristics:

**British Geological Survey, World mineral statistics data:**
World mineral statistics data
Data compiled by The British Geological Survey (BGS) and its predecessor organisations. 

Data availability
Currently the tool grant access to data from the World Mineral Statistics archive for the years 1970 to 2021. 

For certain commodities data are not available for all years, for example the BGS commenced collation of data for primary aggregates in 1998 and consequently there are no data for earlier years for this particular commodity. Trade data (imports and exports) are available for all countries up to 2002, and for selected European countries only from 2003 to 2018.

**United Nations Comtrade Database:**
Contains aggregated data and detailed about global trade statistics by products and partners, with the 99% of the world's merchandise trade.


## Technical Resources:

* **Web Scrapping:** due to the limitations for the World mineral statistics data download queries, a web scrapper has been developed to automatize the process. The available tool allows you to download 10 years by each commodity.

* **Natural Language Processing:** The country dimension in the databases is not standardized, to use a ISO database a NLP normalizer has been developed.


## Current DB Schema:



## Project Structure:

config/: Store configuration files for the ETL process, including paths and sql queries for DB creation.

db/: Contains the .db file with the sqlite DB.
[] DB

docs/: Include documentation for the ETL process and other relevant information.
[] Domain research
[] Data Dictionary (TBD)
[] Process Flow Scheme(TBD)
[] Data Base Schema(TBD)

load/: Scripts and code for loading data from output_data to db.
[] Load script

logs/: Store log files generated during the ETL process.
[] Loading logs (TBD)

monitoring/: Scripts or tools for monitoring and alerting ETL process failures.
[] Quality and testing scripts (TBD)

output_data:
[] output_data/: Store intermediate and final transformed data.
[] cleaned_data/: Transformed and cleaned data.
[] aggregated_data/: Aggregated or summarized data.
[] processed_data/: Data ready for loading.

raw_data/: Store the original data extracted from the sources.

transformations/: Store the transformation scripts and code

utils/: Store utility scripts or functions commonly used across the process.


## Other sources to be added in the future:
**Public Investments (2020 million USD) by Country/area, Technology and Year:**
IRENA (International Renewable Energy Agency) dataset, contains the yearly amount invested in millions USD (2020 values) of countries by power generation technologies.


## Virtual Environment:
The file requirements.txt contains the pipenv requirements for emulating the vm created for the project