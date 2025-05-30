import pandas as pd
import requests
import json
import yaml
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

def get_params_from_yaml(params_file):
    try:
        with open(params_file, "r") as file:
            params = yaml.safe_load(file)
        return params
    except FileNotFoundError:
        print(f"Error: file {params_file} was not found")
    except yaml.YAMLError:
        print(f"Error: file {params_file} was incorrect")

def get_params_from_json(params_file):
    try:
        with open(params_file, "r") as file:
            params = json.load(file)
        return params
    except FileNotFoundError:
        print(f"Error: file {params_file} was not found")
    except json.JSONDecodeError:
        print(f"Error: file {params_file} was incorrect")

# build engine for any db connection (postgres, sql)
def build_engine(drivername, db_params, creds):
    DATABASE_URL = URL.create(
        drivername=drivername,
        username=creds.get('user'),
        password=creds.get('password'),
        host=db_params.get('host'),
        port=db_params.get('port'),
        database=db_params.get('db_name')
    )
    # DATABASE_URL = f"postgresql://{creds.get('DB_USER')}:{creds.get('DB_PASSWORD')}@{db_params.get('DB_HOST')}:{db_params.get('DB_PORT')}/{db_params.get('DB_NAME')}"
    engine = create_engine(DATABASE_URL)
    return engine

def prepare_postgres(postgres_engine):
    with postgres_engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS tmp")) # where increment will be uploaded
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS openweathermap")) # where spotify data will be stored
        conn.execute(text("""
                CREATE TABLE IF NOT EXISTS openweathermap.weather_data (
                    city_name TEXT,
                    country_code CHAR(2),
                    lon NUMERIC(8, 4),
                    lat NUMERIC(8, 4),
                    dt TIMESTAMP,
                    weather_desc TEXT,
                    temp NUMERIC(5, 2),
                    temp_feels_like NUMERIC(5, 2),
                    temp_min NUMERIC(5, 2),
                    temp_max NUMERIC(5, 2),
                    humidity INTEGER,
                    wind_speed NUMERIC(4, 2),
                    PRIMARY KEY (country_code, lon, lat, dt)
                    )
            """))

# get city's latitude and longitude by city name
def get_geo_coordinates_by_name(api_key, city_list):
    url = "http://api.openweathermap.org/geo/1.0/direct"
    response = None
    output_city_data = []
    output_city_df = []
    try:
        for city in city_list:
            params = {
                "q": f"{city['city']}, {city['country']}",
                "appid": api_key,
                "limit": 5
            }
            response = requests.get(url=url, params=params)
            response.raise_for_status()
            output_city_data.append(response.json()[0]) # only first occurrence is valid
        output_city_df = pd.DataFrame(output_city_data)[['name', 'country', 'state', 'lat', 'lon']]
        return output_city_df
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        print("Response:", response.text)
    return None

# get current weather for city by latitude and longitude
def get_weather_data(api_key, city_df):
    url = "https://api.openweathermap.org/data/2.5/weather"
    output_weather_data = []
    response=None
    try:
        for index, row in city_df.iterrows():
            params = {
                "lat": row['lat'],
                "lon": row['lon'],
                "appid": api_key,
                "units": 'metric'
            }
            response = requests.get(url=url, params=params)
            response.raise_for_status()
            response_data = response.json()
            output_weather_data.append(response_data)

        # parse weather description list as one row for every city
        for entry in output_weather_data:
            weather = entry.get("weather", [])
            main_values = [w['main'] for w in weather]
            entry['weather_desc'] = ", ".join(main_values)

        # normalize nested data for all non-array data
        weather_df = pd.json_normalize(output_weather_data, sep='_')
        weather_df = weather_df[
            ['name', 'sys_country', 'coord_lon', 'coord_lat', 'dt', 'weather_desc', 'main_temp', 'main_feels_like',
             'main_temp_min', 'main_temp_max', 'main_humidity', 'wind_speed']]
        weather_df = weather_df.rename(columns={
            "name": "city_name",
            "sys_country": "country_code",
            "coord_lon": "lon",
            "coord_lat": "lat",
            "main_temp": "temp",
            "main_feels_like": "temp_feels_like",
            "main_temp_min": "temp_min",
            "main_temp_max": "temp_max",
            "main_humidity": "humidity"
        })
        weather_df['dt'] = pd.to_datetime(weather_df['dt'], unit='s')
        return weather_df
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        print("Response:", response.text)
    return None

# final method to call all spotify methods to extract data and write the result in postgres tables
def write_spotify_data_to_postgres(api_key, engine, city_list):
    city_data_geo = get_geo_coordinates_by_name(api_key=api_key,
                                                city_list=city_list)
    print(f'Successfully got geo coordinates for city list')

    weather_data = get_weather_data(api_key=api_key,
                                    city_df=city_data_geo)
    print(f'Successfully got weather data for city list')

    # create postgres weather_data_inc table with increment data
    weather_data.to_sql(
        "weather_data_inc",
        con=engine,
        schema="tmp",
        if_exists="replace",
        index=False
    )

    weather_data_table = 'openweathermap.weather_data'
    weather_inc_table = 'tmp.weather_data_inc'
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {weather_data_table}   (city_name,
                                                country_code,
                                                lon,
                                                lat,
                                                dt,
                                                weather_desc,
                                                temp,
                                                temp_feels_like,
                                                temp_min,
                                                temp_max,
                                                humidity,
                                                wind_speed)
            SELECT 
            city_name,
            country_code,
            lon,
            lat,
            dt,
            weather_desc,
            temp,
            temp_feels_like,
            temp_min,
            temp_max,
            humidity,
            wind_speed
            FROM {weather_inc_table}
            ON CONFLICT (country_code, lon, lat, dt) DO UPDATE
            SET
                city_name = EXCLUDED.city_name,
                weather_desc = EXCLUDED.weather_desc,
                temp = EXCLUDED.temp,
                temp_feels_like = EXCLUDED.temp_feels_like,
                temp_min = EXCLUDED.temp_min,
                temp_max = EXCLUDED.temp_max,
                humidity = EXCLUDED.humidity,
                wind_speed = EXCLUDED.wind_speed;
        """))
    print(f'Successfully wrote data to postgres table {weather_data_table}')

def main():
    # get current dir, where creds and config are placed
    current_dir = os.path.dirname(os.path.abspath(__file__))
    creds_json_path = os.path.join(current_dir, "creds.json")
    config_yaml_path = os.path.join(current_dir, "config.yaml")

    # get creds from json
    creds = get_params_from_json(params_file=creds_json_path)
    # openweathermap api key
    API_KEY = creds['openweathermap_api']['api_key']
    # get database config from common config.yaml
    config = get_params_from_yaml(params_file=config_yaml_path)
    db_config = config['database']
    city_list = config['city_list']

    print(f'Requested cities:')
    [print(f"{city['city']}, {city['country']}") for city in city_list]

    # build engine for postgres connection
    postgres_engine = build_engine(drivername="postgresql",
                                   db_params=db_config,
                                   creds=creds['postgres'])
    # prepare schemas and tables
    prepare_postgres(postgres_engine=postgres_engine)

    # Openweathermap to Postgres
    write_spotify_data_to_postgres(api_key=API_KEY,
                                   engine=postgres_engine,
                                   city_list=city_list)


if __name__ == "__main__":
    main()
'''    API_KEY = 'f5a6af54df5e1a14d18c2f670adcec3f'

    city_list = [{'city': 'Berlin', 'country':'DE'},
                 {'city': 'Almaty', 'country':'KZ'},
                 {'city': 'Astana', 'country':'KZ'},
                 {'city': 'Tokyo', 'country':'JP'},
                 {'city': 'Munich', 'country':'DE'}]
    print(f'Requested cities:')
    [print(f"{city['city']}, {city['country']}") for city in city_list]

    city_data_geo = get_geo_coordinates_by_name(api_key=API_KEY,
                                                city_list=city_list)
    print(f'Successfully got geo coordinates for city list')


    weather_data = get_weather_data(api_key=API_KEY,
                                    city_df=city_data_geo)
    print(f'Successfully got weather data for city list')

    weather_data.to_csv('weather.csv', index=False)
    print(f'Successfully saved weather data to csv')

    write_spotify_data_to_postgres(api_key, engine, city_list)'''