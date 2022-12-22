# Databricks notebook source
# start by importing library
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import types
spark = SparkSession.builder.appName('Basics').getOrCreate()
import requests
import json
from datetime import datetime

# COMMAND ----------

# gather all source root urls in a JSON  and validate them visually
source_url= requests.get('https://swapi.dev/api')
source_url.status_code
source_url_json=source_url.json()
source_url_json

# COMMAND ----------

# Only during development, when clean up needed, for ad hock deletion of raw layer tables
#spark.sql("use starwar_raw")
#spark.sql("drop table films")
#spark.sql("drop table people")
#spark.sql("drop table planets")
#spark.sql("drop table species")
#spark.sql("drop table starships")
#spark.sql("drop table vehicles")
#spark.sql("drop database starwar_raw")

# COMMAND ----------


spark.sql("show databases").show()

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

# These SQLs was used for the first time to create database. We will need to execute them for the first time while running in PROD for the first time
#%sql create database starwar_raw;
#%sql create database starwar_curated;


# COMMAND ----------

#################################################################################################################################################
### Ingestion of API data from source into raw layer
### Assumption - for testing purpose, range of 0 to 100 for each api is enough. 
###              for PROD runs, we will have bigger cluster so we can identify actual
###              range by running one time loop to get the range. Or we can get the range from SMEs and business owner.
#################################################################################################################################################

### Framework configurations fields
load_type = 'full'
raw_dbname = 'starwar_raw'
curated_dbname = 'starwar_curated'
main_directory = '/FileStore/tables/'
start_limit = 1
end_limit = 100

### Full clean up raw layer, only in developement region
#DROP database IF EXISTS curated CASCADE

### Creation of database and its usage

#spark.sql(f"create database {raw_dbname}")
spark.sql(f"use {raw_dbname}")

import json
import requests

### business logic

def ingest_swapi_data(url, start_limit, end_limit):
    """ Call the SWAPI and create a list of data. """
    data_set = []
    for number in range(start_limit, end_limit):
        response = requests.get(f"{url}{number}/")

        if response.status_code != 200:
            continue
        else:
            data = response.json()

            data_set.append(data)
    return data_set

# capture current timestamp

def get_timestamp():   
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print("Current Time is :", current_time)
    return current_time

### Logic to maintain date for next run of curated zone starts here.
### This will store the current timestamp for the next delta / incremental load.
### I have created seperate cells for delta load in below code

run_time = get_timestamp()
run_time_lst =spark.createDataFrame([[run_time]])
#run_time_lst.write.mode('overwrite').saveAsTable('last_load_time')
    
### Loop through the resources and url dictionary

for resource, url in source_url_json.items():
    print(resource)
#   response variable contains list of all jsons of sopecific resource
    response = ingest_swapi_data(url, start_limit, end_limit)
# converting response data into spark dataframe 'df'
    df=spark.createDataFrame(response)
#    df.printSchema()  for testing and validation only

#    filepath = main_directory + resource +'/' + run_time + '/' --needed only if we need to maintain history in json files. This could help in identifying delete scenarios whete we might need to compare current data with yesterday's or laste runs data

#    overwtite mode or drop and recreate is used because we will need to load full data every time in raw zone. This is because we do not have any other 
#    mechanism than using fields 'edited' and 'created' for delta detection.
#    df.write.format('delta').mode('overwrite').saveAsTable(resource)
    spark.sql(f"drop table {resource} ")
    df.write.format('delta').saveAsTable(resource)



# COMMAND ----------

#Test - Validate database, table and respective counts
spark.sql("show tables").show()
spark.sql("select 'people', count(*) from people").show()
spark.sql("select 'films', count(*) from films").show()
spark.sql("select 'planets', count(*) from planets").show()
spark.sql("select 'species', count(*) from species").show()
spark.sql("select 'starships', count(*) from starships").show()
spark.sql("select 'vehicles', count(*) from vehicles").show()

# COMMAND ----------

# MAGIC %sql select * from people

# COMMAND ----------

# MAGIC %sql describe formatted starwar_raw.people

# COMMAND ----------

#### Read the raw zone tables into respective dataframes

people=spark.sql("select * from people")
films=spark.sql("select * from films")
planets=spark.sql("select * from planets")
species=spark.sql("select * from species")
starships=spark.sql("select * from starships")


# COMMAND ----------

### Create a keys table to store key relationships between people, films, species and starships resources becuase these are one to many relationships with people
### Note that home planet has 1x1 relationship with people because its a string

keys_data = []
for i in people.collect():
    people_key = i["url"]
    
    for films in i["films"]:
        row = []
        row = [people_key,films]
        keys_data.append(row)
    for species in i["species"]:
        row = []
        row = [people_key,species]
        keys_data.append(row)
    for starships in i["starships"]:
        row = []
        row = [people_key,starships]
        keys_data.append(row)
keys_schema = ['people_key','related_key']

related_keys = spark.createDataFrame(keys_data,keys_schema).distinct()
spark.sql("drop table related_keys")
related_keys.write.format('delta').saveAsTable('related_keys')


# COMMAND ----------

# MAGIC %sql select * from starwar_raw.related_keys order by people_key;

# COMMAND ----------

###########################################################################################################
## FULL LOAD
## Assumption1 - only five fields are needed, and there corresponding source fields mapping is listed below 
##   1 Character Name  - name attribute from resource people
##   2 Film            - title attribute from resource film
##   3 Starship        - name attribute from resource starship 
##   4 Home Planet     - name attribute from resource planet
##   5 Language        - language attribute from resource species
## Assumption2 - Field Ind is for type of record. It will be
##               F - for full load
##               I - for Inserts means created records during the delta process
##               U - for Updates means updated recors during the delta process
## Assumption3 - Field URL from 'people' resource will be kept in final table for validation purpose.
##               If this is not needed, we will exclude it in next version
############################################################################################################
spark.sql("use starwar_raw")

characters = spark.sql("select distinct p.url, p.name as character_name, f.title as film,s.name as starship, ps.name as home_planet, sc.name as species, sc.language, 'F' as ind  from people p join related_keys k on trim(p.url) = trim(k.people_key) left join films f on trim(k.related_key) = f.url left join starships s on trim(k.related_key) = s.url left join planets ps on trim(p.homeworld) = trim(ps.url) left join species sc on trim(k.related_key) = trim(sc.url) order by p.url")

spark.sql("use starwar_curated")
spark.sql("drop table characters")
characters.write.format('delta').saveAsTable('characters')


# COMMAND ----------

# MAGIC %sql select * from characters order by url;

# COMMAND ----------

### Bonus quetions 1 ########################################################################################################
### # of other members of their species on the same ship
###  Assumption - 'their' means character's. So we need to get number of species of a characters on the same ship
###               We will need to join several tables to get this
###  Note in below query, the where clause filters non null values. It needs bigger data to get results. Current range is only 1 to 100.
###  Please see solution in the next cell
#############################################################################################################################

# COMMAND ----------

# MAGIC %sql select count(distinct character_name) as number_of_member,character_name, species, starship from starwar_curated.characters where species is not null and starship is not null group by character_name,species, starship 

# COMMAND ----------

### Bonus quetions 2
### The characters rank by number of films (most to least), number of starships (most to least)
### Please see solution in the next few cells

# COMMAND ----------

# MAGIC %sql with films_numbers as ( select character_name, count(distinct film) as films_count from starwar_curated.characters group by character_name), starships_numbers as (select character_name, count(distinct starship) as starships_Count from starwar_curated.characters group by character_name) select f.character_name,f.films_count,s.starships_Count from films_numbers f join starships_numbers s on f.character_name=s.character_name order by films_count desc, starships_count desc

# COMMAND ----------

### reset/changed run_time just for testing purpose for delta scenarios
run_time = '2010-12-09T13:50:51.644000Z'

# COMMAND ----------

f"select * from starwar_raw.people where created >= '{run_time}' or edited >= '{run_time}'"

# COMMAND ----------

### Validated that at least some data is coming as delta. For testing purpose, delta date has been retrogressed back to '2010-06-20 21:36:09' to ensure this
spark.sql(f"select * from starwar_raw.people where created >= '{run_time}' or edited >= '{run_time}'").show()

# COMMAND ----------

run_time

# COMMAND ----------

###########################################################################################################
### DELTA LOAD
### for delta detection, we will use fields 'created' and 'edited' from each tables.
### Assumption is edited = U means update and created = I means insertion
############################################################################################################
### Framework configurations fields
load_type = 'delta'
raw_dbname = 'starwar_raw'
curated_dbname = 'starwar_curated'

# if load_type = 'delta'
spark.sql("use starwar_raw")

characters_delta = spark.sql(f"select p.url, p.name as character_name, f.title as film,s.name as starship, ps.name as home_planet, sc.language, 'I' as ind from people p join related_keys k on trim(p.url) = trim(k.people_key) left join films f on trim(k.related_key) = f.url left join starships s on trim(k.related_key) = s.url left join planets ps on trim(p.homeworld) = trim(ps.url) left join species sc on trim(k.related_key) = trim(sc.url) where p.created >= '{run_time}' or f.created >= '{run_time}' or s.created >= '{run_time}' or sc.created >= '{run_time}' or ps.created >= '{run_time}' union select p.url, p.name as character_name, f.title as film,s.name as starship, ps.name as home_planet, sc.language, 'U' as ind from people p join related_keys k on trim(p.url) = trim(k.people_key) left join films f on trim(k.related_key) = f.url left join starships s on trim(k.related_key) = s.url left join planets ps on trim(p.homeworld) = trim(ps.url) left join species sc on trim(k.related_key) = trim(sc.url) where p.edited >= '{run_time}' or  f.edited >= '{run_time}' or s.edited >= '{run_time}' or sc.edited >= '{run_time}' or ps.edited >= '{run_time}'")

characters_delta.show()
spark.sql("use starwar_curated")
#spark.sql("drop table characters_delta")
characters.write.format('delta').saveAsTable('characters_delta')


# COMMAND ----------

### performing upsert using merge statement. 
### This error is occuring because we manipulated the delta date. If the date is yesterday's date and if there is a true delta, this merge command will work.
spark.sql("merge into characters using characters_delta on characters.url = characters_delta.url when matched and characters_delta.ind = 'U' then update set * when not matched then insert *")

# COMMAND ----------


