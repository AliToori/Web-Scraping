#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import re
import csv
import pandas as pd
import sqlalchemy as sa
import pyodbc
import traceback
from datetime import datetime
import zipfile, io

# Grabs all the
def get_zip(zip_url):
    filename = None
    with requests.get(zip_url) as z:
        z = zipfile.ZipFile(io.BytesIO(z.content))
        # Change the directory for zip files
        z.extractall(path="..\zips")
        for info in z.infolist():
            filename = info.filename
    return filename


def get_page(page_url):
    links = []
    tNames = []
    with requests.get(page_url) as r:
        if r.ok:
            print('***** Page URL ***** \n')
            print(r.url)
            print('***** Server is happy ***** \n')
            print(r.status_code)
            today = str(datetime.now().strftime('%Y%m%d'))
            print('***** TODAY DATE ***** \n' + today)
            soup = BeautifulSoup(r.content, 'lxml')
            table_body = soup.find('tbody')
            if table_body.find_all("td", string=re.compile(today)) is not None:
                print('SEARCHING FOR TODAY ... \n')
                columns = soup.find_all("td", string=re.compile(today))
                i = -1
                for column in columns:
                    i += 1
                    label = str(column.text).split(".")
                    tNames.append(label[5])
                    links.append(column.find_parent().find("a").get("href"))
                    print(tNames, links)
            else:
                print("NO FILE FOUND")
        else:
            print('***** Server is angry ***** \n')
            print(r.status_code)
    return tNames, links

def upload_csv(csv_url, table_name):
    df = pd.DataFrame.from_csv(csv_url, header=0, index_col=0)
    # Please put here your SQL Server credentials or create your own connection
    engine = sa.create_engine("mssql+pyodbc://<username>:<password>@<dsnname>")
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, index_label=False)
    engine.dispose()

# ###########################################################################
# ###########################################################################
# Starts from here


url_Data_Main = "http://mis.ercot.com/misapp/GetReports.do?reportTypeId=203&reportTitle=TDSP%20ESI%20ID%20Extracts&showHTMLView=&mimicKey"
zip_names = []
tNames, links = get_page(url_Data_Main)
i = -1
for link in links:
    i+=1
    zip_names.append(get_zip(links))
    upload_csv(zip_names[i], tNames[i])
