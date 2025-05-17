import pandas as pd
import yaml
import os
import json
from google.cloud import bigquery
from utils.my_google_drive_and_sheet.google_drive_and_sheets import GoogleDriveandSpreadsheets  # my class for google spreadsheets

# to get data from yaml with project config
def get_params_from_yaml(params_file):
    try:
        with open(params_file, "r") as file:
            params = yaml.safe_load(file)
        print(f'Successfully got params from {params_file}')
        return params
    except FileNotFoundError:
        print(f"Error: file {params_file} was not found")
    except yaml.YAMLError:
        print(f"Error: file {params_file} was incorrect")

# to get data from json with credentials
def get_params_from_json(params_file):
    try:
        with open(params_file, "r") as file:
            params = json.load(file)
        print(f'Successfully got params from {params_file}')
        return params
    except FileNotFoundError:
        print(f"Error: file {params_file} was not found")
    except json.JSONDecodeError:
        print(f"Error: file {params_file} was incorrect")

# to get data from bq via sql query as dataframe
def get_query_results(client: bigquery.Client, bq_table: str, sql_query: str) -> pd.DataFrame():
    print(f'Started reading {bq_table} from bigquery')
    result = client.query(sql_query).to_dataframe()
    print(f'Successfully loaded {bq_table} from bigquery, rows = {result.shape[0]}')
    return result

'''
main() method is used for all transformations:
- config parsing
- connection creation
- custom methods calls etc.
'''
def main():
    # get current dir, where creds and config are placed
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # system variable that bq client uses
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(current_dir, "bq_creds.json")
    config_yaml_path = os.path.join(current_dir, "config.yaml")
    config = get_params_from_yaml(params_file=config_yaml_path)

    # for Google Drive integration
    google_loader = GoogleDriveandSpreadsheets()  # create class instance to work with its methods

    # I. Extract data from BQ
    # project_id where billing for this app is set
    bigquery_source_project = config['bq_default_project']['bigquery_source_project']
    # construct a bq client object
    bq_client = bigquery.Client(project=bigquery_source_project)
    print(f'Successfully connected to BigQuery')
    df_citibike_trips = pd.DataFrame()
    df_citibike_stations = pd.DataFrame()
    # load data citibike_trip from bigquery to pd.dataframe
    if config['bq_tables'].get('citibike_trips'):
        bq_citibike_trips = ".".join(config['bq_tables'].get('citibike_trips').values())
        sql_citibike_trips = f"""
                                select * from {bq_citibike_trips}
                                where date_trunc(starttime, month) = '2016-08-01'
                            """
        df_citibike_trips = get_query_results(client=bq_client,
                                              bq_table=bq_citibike_trips,
                                              sql_query=sql_citibike_trips)

    # load data citibike_stations from bigquery to pd.dataframe
    if config['bq_tables'].get('citibike_stations'):
        bq_citibike_stations = ".".join(config['bq_tables'].get('citibike_stations').values())
        sql_citibike_stations = f"""
                                select * from {bq_citibike_stations}
                            """
        df_citibike_stations = get_query_results(client=bq_client,
                                                 bq_table=bq_citibike_stations,
                                                 sql_query=sql_citibike_stations)

    # II. Transform data using Pandas
    # drop duplicates
    df_citibike_trips.drop_duplicates(inplace=True)
    # drop duplicates in column that is used for join
    df_citibike_stations.drop_duplicates(subset=['station_id'], keep='first', inplace=True)
    # filter trips with non defined start station
    df_citibike_trips.dropna(subset=['start_station_id'])
    # add column with trip date
    df_citibike_trips['trip_date'] = df_citibike_trips['starttime'].dt.date
    # change column data type to str
    df_citibike_trips['start_station_id'] = df_citibike_trips['start_station_id'].astype(str)
    # trips are enriched with stations data
    citibike_trips_full = pd.merge(df_citibike_trips,
                                   df_citibike_stations,
                                   how='left',
                                   left_on='start_station_id',
                                   right_on='station_id')
    # fill non defined data with default value
    citibike_trips_full['rental_methods'] = citibike_trips_full['rental_methods'].fillna('not_defined')
    # aggregate data to calculate most popular stations
    citibike_trips_grouped = (citibike_trips_full.groupby(['trip_date', 'start_station_name', 'usertype', 'rental_methods'])
                              .agg(trip_count=('start_station_id', 'count'))
                              .reset_index())
    csv_file_path = 'citibike_trips_grouped.csv'
    csv_file_name = csv_file_path.split("/")[-1]
    citibike_trips_grouped.to_csv(csv_file_path, index=False)

    # III. Load data to Google Drive
    # parent folder
    gd_parent_folder = config['google_drive']['parent_folder']
    # path to folder where file will be uploaded (starting with parent)
    gd_folder_to_upload_path = config['google_drive']['upload_path']
    folder_id = google_loader.get_folder_id_in_path(gd_parent_folder, gd_folder_to_upload_path)
    if google_loader.upload_file(csv_file_path, folder_id):
        print(f'File {csv_file_name} was uploaded to google drive folder {gd_folder_to_upload_path}')
    # delete created csv
    os.remove(csv_file_path)

if __name__ == "__main__":
    main()


