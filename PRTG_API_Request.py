import urllib.request
import json
import ssl
import pandas as pd
import datetime as dt
import time
import requests

ssl._create_default_https_context = ssl._create_unverified_context

while True:
    url = "[YOUR_API_URL]"  # GET call (request) using PRTG API to extract status info in json format
    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request)
    Elements_To_Remove = ['prtg-version', 'treesize']
    Elements_Read = json.loads(response.read())
    for key in Elements_To_Remove:
        if key in PRTG_Read:
            del PRTG_Read[key]  # removing unwanted elements from json output

    df = pd.DataFrame(PRTG_Read['sensors'])  # putting data in pandas dataframe to aggregate and format
    del df['[col]']  # deleting an existing col
    df['CCount'] = 1  # adding a new col
    df = df.groupby(['[your_col]']).CCount.sum().reset_index()
    df2 = df.pivot_table(columns='your_col',
                         values='CCount',
                         fill_value=0,
                         aggfunc='sum') # pivot- could be done on multiple cols i.e. groupby['col_1', 'col_2']
    df2['Curr_Datetime'] = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f") # add date col
    j = df2.to_json(orient='records')  # to_json function to orient by rows-formatting for PBI json format

    push_url = "YOUR_PBI_URL" # your PowerBI push URL

    r = requests.post(push_url, j)
    print(j + " pushed.")  # get row
    print(r.status_code)  # get status code- should be 200

    time.sleep(60)  # loop every 1 minute
