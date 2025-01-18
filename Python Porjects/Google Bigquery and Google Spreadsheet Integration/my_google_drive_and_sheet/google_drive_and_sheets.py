import logging
import json
from typing import List
import datetime
import requests

from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient import discovery

import pandas as pd
import numpy as np

class GoogleDriveandSpreadsheets:
    
    def __init__(self):
        self.sheet_service = self.get_client('sheets', 'v4')
        self.drive_service = self.get_client('drive', 'v3')

    # build client for drive integration and for sheet integration
    # service_name = drive, service_version = v3 - for google drive
    # service_name = sheets, service_version = v4 - for google spreadsheets
    def get_client(self, service_name: str, service_version: str) -> discovery.build:
        credentials_info = 'my-project-sheet-integration-5e66b5395e0a.json' # тут нужно передать путь к переменной в вольте?
        service_account_info = json.load(open(credentials_info))
        scopes_arr=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        google_credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes = scopes_arr)
        result = discovery.build(service_name, service_version, credentials = google_credentials)
        return result
    
    # function for logging if .py file will be executed - test when it will work as app
    def get_Logger(self, name:str, log_level:str = "INFO"):
        logging.basicConfig()
        logger = logging.getLogger(name)
        logger.setLevel(log_level.upper())
        return logger


    # get list of available files in google drive for this acc
    def get_drive_file_list(self) -> list: 
        file_list = self.drive_service.files().list().execute()['files']
        return file_list
        #name, id - necessary keys
        
        
    # get list of worksheets from spreadsheet
    def get_worksheets_list_in_spreadsheet(self, spreadsheet_id: str) -> list: 
        worksheets_list = self.sheet_service.spreadsheets().get(spreadsheetId = spreadsheet_id).execute()['sheets']
        title_list = []
        for sheet in worksheets_list:
            title_list.append(sheet['properties']['title'])
        return title_list
        
    
    """
    create new worksheet in spreadsheet = spreadsheet_id, 
    with worksheet name = new_sheet_name 
    """
    def create_new_sheet_in_spreadsheet(self, spreadsheet_id: str, new_sheet_name: str):
        body = {
                "requests":{
                    "addSheet":{
                        "properties":{
                            "title":new_sheet_name
                        }
                    }
                }
            }
        if new_sheet_name in self.get_worksheets_list_in_spreadsheet(spreadsheet_id):
            raise Exception(f'worksheet {new_sheet_name} already exists in spreadsheet')
        else:
            self.sheet_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()    
    
    
    """
    get data from spreadsheet = spreadsheet_id, 
    worklist inside spreashsheet = spreadsheet_sheet_name, 
    start_raw = name of the start row of range (e.g.A1 or R1C1)
    end_raw = name of the end row of range (e.g.C1 or R10C15)
    """
    def get_sheet_values(self, spreadsheet_id: str, spreadsheet_sheet_name: str, start_raw: str, end_raw: str) -> List[list]:
        range_name  = f"'{spreadsheet_sheet_name}'!{start_raw}:{end_raw}"
        result = self.sheet_service.spreadsheets().values().batchGet(spreadsheetId = spreadsheet_id, 
                                                          ranges = range_name).execute()['valueRanges'][0].get('values', []) #get only values from response from first range
        return result
    # get_sheet_values(sheet_service, spreadsheet_id, 'games_post_wide', 'A1', 'B10')
    
    
    
    """
    delete data from spreadsheet = spreadsheet_id, 
    worklist inside spreashsheet = spreadsheet_sheet_name, 
    start_raw = name of the start row of range (e.g.A1 or R1C1)
    end_raw = name of the end row of range (e.g.C1 or R10C15)
    """
    def delete_sheet_values(self, spreadsheet_id: str, spreadsheet_sheet_name: str, start_raw: str, end_raw: str):
        range_name  = f"'{spreadsheet_sheet_name}'!{start_raw}:{end_raw}"
        clear_values_request_body = {}
        self.sheet_service.spreadsheets().values().clear(spreadsheetId = spreadsheet_id, range = range_name, body = clear_values_request_body).execute()

    # delete_sheet_values(sheet_service, spreadsheet_id, 'games_post_wide', 'A1', 'B10')
    
    
    """
    insert data to spreadsheet = spreadsheet_id, 
    worklist inside spreashsheet = spreadsheet_sheet_name, 
    start_raw = name of the start row of range (e.g.A1 or R1C1)
    end_raw = name of the end row of range (e.g.C1 or R10C15)
    initial data should be List[list], where 1 row = separate list [], 2 row =[], etc. (e.g. [['tom gt sr', 10], ['nick', 15], ['juli', 14]])
    valueInputOption = https://developers.google.com/sheets/api/reference/rest/v4/ValueInputOption?hl=ru (USER_ENTERED = as if user would enter through website)
    majorDimension = https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values?hl=ru#ValueRange (ROWS = to insert list=1 row by row in spreadsheet, COLUMNS - if in main List 1 separate list = 1 column)
    """
    def insert_sheet_values(self, spreadsheet_id: str, spreadsheet_sheet_name: str, values: List[list], start_raw: str, end_raw: str, major_dimension: str):
    # if initial data to insert is List[list], where 1 separate list = 1 row
        if major_dimension == 'ROWS':
            data = {
                'range': f"'{spreadsheet_sheet_name}'!{start_raw}:{end_raw}",
                'values': values,
                'majorDimension': major_dimension
            }
            body = {'valueInputOption': 'USER_ENTERED', 'data':data}
            self.sheet_service.spreadsheets().values().batchUpdate(spreadsheetId = spreadsheet_id, body = body).execute()
    # if initial data to insert is List[list], where 1 separate list = 1 column
        if major_dimension == 'COLUMNS':
            data = {
                'range': f"'{spreadsheet_sheet_name}'!{start_raw}:{end_raw}",
                'values': list(zip(*values)),
                'majorDimension': major_dimension
            }
            body = {'valueInputOption': 'USER_ENTERED', 'data':data}
            self.sheet_service.spreadsheets().values().batchUpdate(spreadsheetId = spreadsheet_id, body = body).execute()
    # insert_sheet_values(sheet_service, spreadsheet_id, 'games_post_wide', df_test.values.tolist(), 'R1C1', 'R5C4', 'COLUMNS')


    """
    return sheet_id (int) or None value for requested sheet_title
    """
    def get_sheet_id_by_title(self, spreadsheet_id: str, sheet_title: str):
        all_sheets = self.sheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()['sheets']
        sheet_id = None
        for sheet in all_sheets:
            if sheet['properties']['title'] == sheet_title:
                 sheet_id = sheet['properties']['sheetId']
                 break
        return sheet_id


    """
    basic filter - main filter for a sheet. There can be only one such filter for a sheet.
    Method allows to delete basic filter for requested sheet by its sheet_title
    """
    def delete_basic_filter_for_sheet(self, spreadsheet_id: str, sheet_title: str):
        body = {
            "requests":{
                "clearBasicFilter":{
                    "sheetId": self.get_sheet_id_by_title(spreadsheet_id, sheet_title)
                        }
                    }
                }
        self.sheet_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


    """
    basic filter - main filter for a sheet. There can be only one such filter for a sheet.
    Method allows to delete all basic filters from all sheets in spreadsheet_id
    """
    def delete_all_basic_filters(self, spreadsheet_id: str):
        all_sheets = self.sheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()['sheets']
        if all_sheets:
            for sheet in all_sheets:
                body = {
                    "requests":{
                        "clearBasicFilter":{
                            "sheetId": sheet['properties']['sheetId']
                        }
                    }
                }
                self.sheet_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()