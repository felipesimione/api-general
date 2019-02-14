import requests
import json
import datetime
import smtplib
import os
import sys
import csv
import subprocess
import pandas as pd
from pandas.io.json import json_normalize
import pkg_variables as var
import pkg_data_base as ndb
import pkg_treatment as trt

def get_data(days_range_initial, days_range_final, token):
    """Queries Facebook Reporting.

    Args:
      days_range_initial, days_range_final: Ranges of day to run.
      token: token to access facebook api.
    Returns:
      The Facebook Reporting response.
    """
    try:
        df_fb_data = pd.DataFrame()

        # Loop to run every day
        for days in range(days_range_initial, days_range_final + 1):
            # Transform range number to date
            query_date = datetime.datetime.today() - datetime.timedelta(days=abs(days))
            query_date_str = query_date.strftime('%Y-%m-%d')

            # Prepare query fields
            encoded_date_range = '{' + \
                "'since': '{0}', 'until' : '{0}'".format(query_date_str) + '}'

            fields = ['campaign_id','campaign_name','objective','impressions','clicks',
                            'spend', 'date_start', 'date_stop']

            data = {'access_token': token, 'level': 'campaign',
                    'time_range': encoded_date_range, 'fields': ','.join(fields)}

            # Connect Facebook Report and get data
            url = "https://graph.facebook.com/v3.1/act_xxxxxxxxxxxxxxxxxx/insights"
            r = requests.get(url, params=data)

            response_content = json.loads(r.content)

            # Normalize data in dataframe
            current_data = json_normalize(response_content['data'])
            df_fb_data = df_fb_data.append(current_data)

        return df_fb_data

    except:
        return print("Erro: {}".format(response_content))

def main():
    # Receive days range and token
    days_range_initial, days_range_final = var.STAGING_API_DAYS_RANGE_INITIAL, var.STAGING_API_DAYS_RANGE_FINAL
    token = var.TOKEN_FACEBOOK
    # Get facebook data
    df_fb_data = get_data(days_range_initial, days_range_final, token)

    # Treat filds
    df_fb_data['campaign_id']   = df_fb_data['campaign_id'].astype('int64')
    df_fb_data['impressions']   = df_fb_data['impressions'].astype('int64')
    df_fb_data['clicks']        = df_fb_data['clicks'].astype('int64')
    df_fb_data['spend']         = df_fb_data['spend'].astype('float64')
    df_fb_data['date_start']    = pd.to_datetime(df_fb_data['date_start'], format='%Y-%m-%d %H:%M')
    df_fb_data['date_stop']     = pd.to_datetime(df_fb_data['date_stop'], format='%Y-%m-%d %H:%M')

    trt.treat_columns(df_fb_data)

    # Execute bulk loader
    ndb.bulk_loader_insert('groot', 'fb_campaign_data', df_fb_data)


if __name__ == '__main__':
    main()
