import logging
import json
from typing import List
import datetime

from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient import discovery

import pandas as pd
import numpy as np


from my_google_drive_and_sheet.google_drive_and_sheets import GoogleDriveandSpreadsheets  # my class for google spreadsheets


def get_query_results(client: bigquery.Client, sql_query: str) -> pd.DataFrame():
    """
    get data from bq via sql query as dataframe
    """
    result = client.query(sql_query).to_dataframe()
    return result


def prepare_values_list_to_insert(df: pd.DataFrame()) -> List[list]:
    """
    prepare data to insert - union column names and data itself as one list
    """
    return [df.columns.tolist()] + df.values.tolist()


if __name__ == '__main__':
    # project_id where billing for this app is set
    bigquery_source_project = 'my-project-sheet-integration'

    # construct a bq client object
    bq_client = bigquery.Client(project = bigquery_source_project)
    print(f'Successfully connected to BigQuery')

    # for sheet intergration
    google_loader = GoogleDriveandSpreadsheets()
    credentials_info = 'my-project-sheet-integration-5e66b5395e0a.json' # тут нужно передать путь к переменной в вольте?
    service_account_info = json.load(open(credentials_info))
    scopes_arr=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    google_credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes = scopes_arr)
    print(f'Successfully got credentials for google authorization')

    # load data from bigquery to pd.dataframe
    # 1
    games_post_wide = 'bigquery-public-data.baseball.games_post_wide'
    sql = f"""
        select gameId, seasonID from {games_post_wide}
        limit 5000
    """
    df_games_post_wide = get_query_results(bq_client, sql)
    print(f'Successfully loaded {games_post_wide} from bigquery')

    # 2
    chicago_crime = 'bigquery-public-data.chicago_crime.crime'
    sql = f"""
        select * from {chicago_crime}
        limit 5000
    """
    df_chicago_crime = get_query_results(bq_client, sql)
    print(f'Successfully loaded {df_chicago_crime} from bigquery')

    # check if file exists and get spreadsheet_id
    spreadsheet_name = 'python integration'
    spreadsheet_id = None
    for file in google_loader.get_drive_file_list():
        if file['name'] == spreadsheet_name:
            spreadsheet_id = file['id']
        else:
            raise Exception(f'no file {spreadsheet_name} in drive')

    sheet_title = 'chicago_crime'
    # delete all data
    google_loader.delete_sheet_values(spreadsheet_id, sheet_title, 'A1', 'AZ300000') # all data in worksheet
    print(f'Successfully deleted data from {sheet_title} sheet in file {spreadsheet_name}')
    
    # insert data in spreadsheet in concrete sheet_name = sheet_title
    df = df_chicago_crime[['case_number', 'block', 'primary_type']].head(1000)
    start_raw = 'R1C1' # first cell in sheet
    end_raw = 'R'+str(df.shape[0]+1) + 'C' + str(df.shape[1]) # +1 for first row with column names
    google_loader.insert_sheet_values(spreadsheet_id, sheet_title, prepare_values_list_to_insert(df), start_raw, end_raw, 'ROWS')
    print(f'Successfully inserted data to {sheet_title} sheet in file {spreadsheet_name}')

    # delete all basic filters from all sheets if they existed
    google_loader.delete_all_basic_filters(spreadsheet_id)
    print(f'Successfully deleted all basic filters in file {spreadsheet_name}')
