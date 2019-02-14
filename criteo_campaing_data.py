
# coding: utf-8
# Script to get data campaing from Criteo

import requests
import json
import pandas as pd
from io import StringIO
import datetime
from datetime import datetime as dtime, timedelta
import time
import pkg_treatment as trt
import pkg_variables as var
import pkg_data_base as ndb


def get_token(client_id, client_secret):
    """Generate token to access data.

    Args:
        client_id: Id API Criteo.
        client_secret: Password API Criteo.
    Returns:
        Access Token.
    """
    # Prepare headers
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
    }
    # Prepare keys
    data = [
      ('client_id', client_id),
      ('client_secret', client_secret),
      ('grant_type', 'client_credentials'),
    ]
    # Request and get token
    res_token = requests.post('https://api.criteo.com/marketing/oauth2/token', headers=headers, data=data)
    # Convert to JSON
    res_token_json = res_token.json()

    return res_token_json["access_token"]

def get_response(token, date_start, date_end):
    """Get data.

    Args:
        token: token to access data.
        date_start, date_end: Range of day to run.
    Returns:
        The Criteo Reporting response.
    """
    # Prepare variables url and headers
    url = 'https://api.criteo.com/marketing/v1/statistics'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/octet-stream',
        'Authorization': 'Bearer '+ token,
    }
    # Prepare variables with filters
    params = {
      "reportType": "CampaignPerformance",
      "ignoreXDevice": 'true',
      "startDate": date_start,
      "endDate": date_end,
      "dimensions": ['Day', 'CampaignId'],
      "metrics": ['Clicks', 'Displays', 'Audience', 'AdvertiserCost'],
      "currency": "BRL",
      "timezone": "GMT"
     }
    # Request and get data
    data_response = requests.post(url, data=json.dumps(params), headers=headers)

    return pd.read_csv(StringIO(data_response.text), sep=';')

def main():
    # Variables to get token
    client_id = var.CLIENT_ID
    client_secret = var.CLIENT_SECRET

    # Generate token
    token = get_token(client_id, client_secret)

    # Receive days range and token
    days_range_initial, days_range_final = var.STAGING_API_DAYS_RANGE_INITIAL, var.STAGING_API_DAYS_RANGE_FINAL

    now = dtime.now()
    now_start = now - datetime.timedelta(abs(days_range_initial))
    now_final = now - datetime.timedelta(abs(days_range_final))
    date_start = now_start.date().strftime('%Y-%m-%d')
    date_final = now_final.date().strftime('%Y-%m-%d')

    # Create variables that will be used in for loop
    date1 = datetime.date(int(date_start[:4]),int(date_start[5:7]), int(date_start[8:10]))
    date2 = datetime.date(int(date_final[:4]),int(date_final[5:7]), int(date_final[8:10]))
    d1 = datetime.date.toordinal(date1)
    d2 = (datetime.date.toordinal(date2))+1

    date_start_h = now_start.date().strftime("%Y-%m-%dT%H:%M:%S")
    date_final_h = now_final.date().strftime("%Y-%m-%d") + 'T23:59:59'

    df_data = pd.DataFrame()

    # Loop to run everyday
    for i in range(d1,d2):
        select_date_start = datetime.datetime.fromordinal(i).strftime('%Y-%m-%d') + 'T00:00:00'
        select_date_final = datetime.datetime.fromordinal(i).strftime('%Y-%m-%d') + 'T23:59:59'

        df = get_response(token, date_start=select_date_start, date_end=select_date_final)

        df_data = df_data.append(df)

    #Treat Fields and columns
    trt.treat_columns(df_data)
    trt.adjust_columns_name(df_data)
    df_data['day'] = pd.to_datetime(df_data['day'])

    # Execute bulk loader
    ndb.bulk_loader_insert('groot', 'cr_campaing_data', df_data)

if __name__ == '__main__':
    main()
