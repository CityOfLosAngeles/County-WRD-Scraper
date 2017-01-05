from bs4 import BeautifulSoup
import requests
import sqlite3
import unicodedata
import json
import os

session = requests.Session()
session.cookies.clear()

headers={
  'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'
}

content = []

url = "https://dpw.lacounty.gov/wrd/Precip/alertlist.cfm"
base_url = "https://dpw.lacounty.gov/wrd/Precip/"
response = session.get(url, headers=headers)

soup = BeautifulSoup(response.text, "html.parser")
#first get the col. names
col_names = map((lambda th: str(th.getText()).replace(".","").replace(" ", "_").lower()), soup.findAll('th'))
col_names_inner = None

#get the rows
rows = soup.findAll('tr',{'bgcolor':['ECECEC','ffffff']})
sanitized_rows = []

for row in rows:
    #get outer items
    items = map((lambda td: str(td.getText().strip(u'\xa0'))), row.findAll('td'))
    sanitized_row = dict(zip(col_names, items))
    
    #get inner items
    link = row.find('a').get('href').split('(')[1].replace(")","").replace("'","")
    row_data_res = session.get(base_url + link, headers=headers)
    soup = BeautifulSoup(row_data_res.text, "html.parser")

    #check if we dont have inner col names
    if col_names_inner == None:
        col_names_inner = map((lambda th: str(th.getText()).replace(".","").replace(" ", "_").replace("/","").lower()), soup.findAll('th'))
    
    #sanitize inner items
    readings = soup.findAll('table')[1].findAll('tr')[1:]
    if len(readings) == 0:
        print("Skipping row with alert id %s due to the lack of readings" % sanitized_row["alert_id"])
        continue
    sanitized_readings = []
    for reading in readings:
        sanitized_reading = []
        for td in reading.findAll('td'):
            sanitized_reading.append(str(unicodedata.normalize("NFKD",td.getText())).strip())
        
        hash_sanitized = dict(zip(col_names_inner, sanitized_reading))
        sanitized_readings.append(hash_sanitized)

    sanitized_row["data"] = sanitized_readings
    sanitized_rows.append(sanitized_row)

print("Done scrapping")
print("Started writing to json output file")
with open('results.json', 'w') as f:
    json.dump(sanitized_rows, f)

os.system("firebase-import --database_url https://stormwatercapt.firebaseio.com/  --path / --json results.json --service_account credentials.json --force")