"""Analytics Reporting API V4."""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pyodbc
import numpy as np
import pandas as pd
import datetime
from datetime import datetime as dtime, timedelta
import time
import psycopg2
import os
from io import StringIO
import pkg_treatment as trt
import pkg_variables as var
import pkg_data_base as ndb

scopes = ['https://www.googleapis.com/auth/analytics.readonly']
key_file = var.KEY_FILE_LOCATION
view_id = var.VIEW_ID

def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
        An authorized Analytics Reporting API V4 service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file, scopes)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics

def get_report(analytics,select_date,page_token):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
    select_date: Data do acesso selecionada como filtro
    page_token: Número da linha inicial (start-index)
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': view_id,
          'dateRanges': [{'startDate': select_date, 'endDate': select_date}],
          'metrics': [{'expression': 'ga:revenuePerTransaction'}],
          'dimensions': [{'name': 'ga:transactionId'},{'name': 'ga:date'}
                         ,{'name': 'ga:channelGrouping'},{'name': 'ga:medium'}
                         ,{'name': 'ga:source'},{'name': 'ga:campaign'},{'name': 'ga:deviceCategory'}],
          'pageToken': '0',
          'pageSize': '10000',
          'samplingLevel': 'LARGE'
        }]
      }
  ).execute()


def get_response(response):
  """Treat data and normalize to dataframe.

  Args:
    response: Data to normalize to dataframe.
  Returns:
    Dataframe with normalize data.
  """
  list = []

  # get report data
  for report in response.get('reports', []):
    # set column headers
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    for row in rows:
        # create dict for each row
        dict = {}
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        # fill dict with dimension header (key) and dimension value (value)
        for header, dimension in zip(dimensionHeaders, dimensions):
          dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
              for metric, value in zip(metricHeaders, values.get('values')):
                #set int as int, float a float
                if '.' in value or ',' in value:
                  dict[metric.get('name')] = float(value)
                else:
                  dict[metric.get('name')] = int(value)

        list.append(dict)

    df = pd.DataFrame(list)
    return df

def main():
    analytics = initialize_analyticsreporting()

    # define range of date to update and delete
    now = dtime.now()
    now_start = now - datetime.timedelta(days=abs(var.STAGING_API_DAYS_RANGE_INITIAL))
    now_final = now - datetime.timedelta(days=abs(var.STAGING_API_DAYS_RANGE_FINAL))
    date_start = now_start.date().strftime('%Y-%m-%d')
    date_final = now_final.date().strftime('%Y-%m-%d')

    # created variables it well be used in the for
    date1 = datetime.date(int(date_start[:4]),int(date_start[5:7]), int(date_start[8:10]))
    date2 = datetime.date(int(date_final[:4]),int(date_final[5:7]), int(date_final[8:10]))
    d1 = datetime.date.toordinal(date1)
    d2 = (datetime.date.toordinal(date2))+1

    # get initial number of row (start-index)
    page_token = '0'

    for i in range(d1,d2):
        select_date = datetime.datetime.fromordinal(i).strftime('%Y-%m-%d')
        # Loop to when have more then 10K rows (limit Google Analytics)
        while page_token != None:
            response = get_report(analytics,select_date,page_token)
            df_sessions = get_response(response)
            # Excluded ga: the start name of columns
            df_sessions.columns = df_sessions.columns.str.replace("ga:", "")
            trt.treat_columns(df_sessions)
            ndb.bulk_loader_insert('groot', 'ga_channel_last_click', df_sessions)
            # get next row (start-index | próxima página)
            page_token = response['reports'][0].get('nextPageToken')

if __name__ == '__main__':
    main()
