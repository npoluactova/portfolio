# City bikes data project

This project demonstrates methods for extracting data from BigQuery, processing it with Pandas, and interacting with the Google Drive and Sheets APIs.

For each section there were developed different methods with examples of usage, which cover the questions below.

## Google BigQuery

- How to get credentials from json and add them as environmental variable
- How to store project params in YAML
- How to create BQ connection
- How to extract data from BQ

## Data processing via Pandas

- How to clean data (remove duplicates, nulls)
- How to aggregate data
- How to upload data to csv

## Google Spreadsheet Integration

Methods for Google Drive and Google Spreadsheet integration are implemented in Class `GoogleDriveandSpreadsheets`, that is imported in this project. 

- How to create new spreadsheet or to connect to already existing spreadsheet
- How to change data in spreadsheet
- How to connect to Google Drive
- How to upload files to Google Drive

Every method in GoogleDriveandSpreadsheets Class has a thorough description. You can look at it in `\utils\my_google_drive_and_sheet\google_drive_and_sheets.py`
