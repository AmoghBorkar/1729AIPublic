# This file gets the list of all files from GDELT website, unzips, 
# pre-processes them and stores to postgres on azure pg server

# Import needed packages
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import xmltodict
from bs4 import BeautifulSoup
import lxml
import time
import psycopg2
from sqlalchemy import create_engine
engine = create_engine('postgresql://localhost:5432/postgres?user=<>&password=<>')


download_start_date = datetime(2022,5,10)
download_end_Date = datetime(2022,5,20)
# # Postgres connection parameters - other way to connect
# host = "esgdb.postgres.database.azure.com"
# dbname = "postgres"
# user = "amogh"
# password = "MH12ta7767#"
# sslmode = "require"

# # Construct connection string
# conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
# conn = psycopg2.connect(conn_string)
# print("Connection established")

# cursor = conn.cursor()


# Scrape the data
url = """https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"""

# Get the list of filenames. Some of the further processing is to get the URLs from text.
req = requests.get(url,verify = False)

# Read in all the files data
stringList = req.text.split("\n")
files_data_df = pd.DataFrame([x.split(' ') for x in stringList], columns = ['Ref1','Ref2','URL'])

# Keep only the GKG URLs
files_data_df = files_data_df.dropna()
files_data_df['file_type'] = files_data_df['URL'].apply(lambda x: x.split(".")[-3])
files_data_df = files_data_df[files_data_df['file_type'] == 'gkg']
files_data_df = files_data_df.reset_index(drop=True)

# Format the Date string
files_data_df['date_str'] = files_data_df['URL'].apply(lambda x:x.split("/")[-1].split(".")[0])
files_data_df['date'] = files_data_df['date_str'].apply(lambda x:datetime.strptime(x,'%Y%m%d%H%M%S'))

#Filter to files between start and end dates
files_data_df = files_data_df[files_data_df['date'] > download_start_date]
files_data_df = files_data_df[files_data_df['date']< download_end_Date]
files_data_df = files_data_df.reset_index(drop=True)
len(files_data_df)


# Write to the postgres table
files_data_df.to_sql('gdelt_files_loaded',engine, schema='public',if_exists='append')

# Extract the data
gkg_columns = ['GKGRECORDID','DATE','SourceCollectionIdentifier','SourceCommonName',
'DocumentIdentifier','Unknown1','Unknown2','Themes','V2Themes',
'Locations','V2Locations','Persons','V2Persons','Organizations','V2Organizations',
'V2Tone','Unknown3','Unknown4','SharingImage','Unknown5','Unknown6','SocialVideoEmbeds',
'Unknown7','AllNames','Amounts', 'TranslationInfo','Extras']

gkg_columns_to_keep = ['GKGRECORDID','DATE','SourceCollectionIdentifier','SourceCommonName',
'DocumentIdentifier','V2Themes',
'V2Locations','V2Persons','V2Organizations',
'V2Tone','SharingImage','SocialVideoEmbeds','AllNames','page_title']

gkg_output_df = pd.DataFrame()

for i in range(0,len(files_data_df),1):
    # Reset the dataframe
    gkg_data_df = pd.DataFrame()
    try:
        data_url = files_data_df['URL'][i]
        data_res = urlopen(data_url)
        #read the zipfile
        zipfile = ZipFile(BytesIO(data_res.read()))
        #Get the CSV filename
        fname = zipfile.namelist()[0]
        gkg_data_df = pd.read_csv(zipfile.open(fname),dtype=object,delimiter='\t',engine='python',encoding='ISO-8859-1')
        gkg_data_df.columns =  gkg_columns
        gkg_data_df['page_title'] = ''

        # Extract the page title
        for index,row in gkg_data_df.iterrows():
            soup = BeautifulSoup(row['Extras'])
            page_title = soup.findAll('page_title')
            if len(page_title) > 0:
                page_title = str(page_title[0]).replace("<page_title>","").replace("</page_title>","")
                gkg_data_df.loc[index,'page_title'] = page_title

        #Format the date and time
        gkg_data_df['temp_datetime'] = gkg_data_df['DATE'].apply(lambda x: datetime.strptime(x,'%Y%m%d%H%M%S'))
        gkg_data_df['DATE'] = gkg_data_df['temp_datetime'].apply(lambda x: str(x.date()))
        gkg_data_df['TIME'] = gkg_data_df['temp_datetime'].apply(lambda x: str(x.time()))
        del gkg_data_df['temp_datetime']

        # Delete duplicates & other details
        gkg_data_df  = gkg_data_df.drop_duplicates(subset=['page_title'])
        gkg_data_df = gkg_data_df[gkg_columns_to_keep]
        #gkg_output_df = gkg_output_df.append(gkg_data_df)
        # Write - append to the database
        gkg_data_df.to_sql('gdelt_gkg_data',engine, schema='public',if_exists='append')

        #Close the zip file
        zipfile.close()

    except:
        #Reset the connection - find a better way to do this later
        from urllib.request import urlopen
        continue

    



