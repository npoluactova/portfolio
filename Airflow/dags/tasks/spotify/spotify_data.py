import pandas as pd
import requests
import base64
import os
import json
import yaml
import re
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
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS spotify")) # where spotify data will be stored
        conn.execute(text("""
                CREATE TABLE IF NOT EXISTS spotify.artists (
                    id TEXT,
                    name TEXT,
                    genres TEXT,
                    followers_total bigint,
                    popularity integer,
                    PRIMARY KEY (id, genres)
                );
            """))
        conn.execute(text("""
                CREATE TABLE IF NOT EXISTS spotify.albums (
                    id TEXT,
                    name TEXT,
                    total_tracks integer,
                    release_date date,
                    artist_id TEXT,
                    PRIMARY KEY (id, artist_id) 
                );
            """))

# method to get access token for Client Credentials Flow (to get generic spotify data about artists, tracks)
def get_spotify_access_token(client_id, client_secret):
    auth_url = "https://accounts.spotify.com/api/token"
    # encode - string to bytes, b64encode - bytes to base64, decode - bytes to string again to use in Authorization header
    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials"
    }

    response = None
    try:
        response = requests.post(url=auth_url, headers=headers, data=data)
        response.raise_for_status()  # Raise exception HTTPError if returns 4xx, 5xx codes
        return response.json().get("access_token")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        print("Response:", response.text)
    return None


# method to search information about albums, artists, playlists, tracks, shows, episodes or audiobooks that match a keyword string
def spotify_search(access_token, query_filter, item_type):
    search_url = "https://api.spotify.com/v1/search"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    #available_filters_list = ('artist', 'track', 'year', 'upc', 'tag:hipster', 'tag:new', 'isrc', 'genre')
    available_types_list = ('album', 'artist', 'playlist', 'track', 'show', 'episode', 'audiobook')

    if item_type not in available_types_list:
        raise ValueError(f"Invalid input query item type parameter: '{item_type}'. Possible values are {available_types_list}")

    params = {
        "q": query_filter,
        "type": item_type,
        "limit": 2
    }

    response = None
    try:
        response = requests.get(url=search_url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        print("Response:", response.text)
    return None

# get info all main params about artist as pandas df
def spotify_get_artist(access_token, artists_list):
    artists_data = []
    for artist in artists_list:
        artists_data.append(spotify_search(access_token=access_token,
                                           query_filter=artist,
                                           item_type='artist')["artists"]["items"][0] #return first match for every artist
                       )
    # get total followers as separate column
    for artist in artists_data:
        artist["followers_total"] = artist["followers"]["total"]

    # select only relevant params
    artist_df = pd.DataFrame(artists_data)[['id', 'name', 'genres', 'followers_total', 'popularity']]
    # for every genre create separate row for this artist
    artist_df = artist_df.explode('genres')
    return artist_df


# get albums for list of spotify artist id
def spotify_get_album(access_token, artist_id_list):
    albums = []
    for artist in artist_id_list:
        url = f"https://api.spotify.com/v1/artists/{artist}/albums"
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        params = {
            "id": artist,
            "include_groups": "album",
            "market": "US"
        }
        albums.append(requests.get(url=url, headers=headers, params=params).json()["items"])

    columns_to_keep = ['id', 'name', 'total_tracks', 'release_date', 'artist_id']
    flat_album_list = []
    for album_list in albums:
        for album in album_list:
            album['artist_id'] = [artist['id'] for artist in album.get('artists', [])]
            filtered_album = {key: album.get(key) for key in columns_to_keep}
            flat_album_list.append(filtered_album)
    albums_df = pd.DataFrame(flat_album_list).explode('artist_id')
    return albums_df

# method to clean date column to yyyy-mm-dd format
def normalize_date_regex(value):
    if pd.isna(value):
        return None
    value = str(value).strip() # clean all spaces
    if re.fullmatch(r"\d{4}", value):
        # 1994 -> 1994-01-01
        return f"{value}-01-01"
    elif re.fullmatch(r"\d{4}-\d{2}", value):
        # 1994-05 -> 1994-05-01
        return f"{value}-01"
    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        # 1994-02-02 -> 1994-02-02
        return value
    else:
        return None

# final method to call all spotify methods to extract data and write the result in postgres tables
def write_spotify_data_to_postgres(client_id, client_secret, engine, artists_list):
    access_token = get_spotify_access_token(client_id=client_id,
                                            client_secret=client_secret)
    print(f'Successfully got access token')

    artists_df = spotify_get_artist(access_token=access_token,
                                    artists_list=artists_list)
    artists_df.drop_duplicates(inplace=True)
    print(f'Successfully got artists data')

    albums_df = spotify_get_album(access_token=access_token, artist_id_list=artists_df['id'])
    albums_df['release_date'] = albums_df['release_date'].apply(normalize_date_regex)
    albums_df['release_date'] = pd.to_datetime(albums_df['release_date'], format='%Y-%m-%d', errors='coerce').dt.date
    albums_df.drop_duplicates(inplace=True)
    print(f'Successfully got albums data for artists list')

    # create postgres artists_inc table with increment data
    artists_df.to_sql(
        "artists_inc",
        con=engine,
        schema="tmp",
        if_exists="replace",
        index=False
    )
    # create postgres albums_inc table with increment data
    albums_df.to_sql(
        "albums_inc",
        con=engine,
        schema="tmp",
        if_exists="replace",
        index=False
    )

    artists_table = 'spotify.artists'
    artists_inc_table = 'tmp.artists_inc'
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {artists_table} (id, name, genres, followers_total, popularity)
            SELECT id, name, genres, followers_total, popularity FROM {artists_inc_table}
            ON CONFLICT (id, genres) DO UPDATE
            SET
                name = EXCLUDED.name,
                followers_total = EXCLUDED.followers_total,
                popularity = EXCLUDED.popularity;
        """))
    print(f'Successfully wrote data to postgres table {artists_table}')

    albums_table = 'spotify.albums'
    albums_inc_table = 'tmp.albums_inc'
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {albums_table} (id, name, total_tracks, release_date, artist_id)
            SELECT id, name, total_tracks, release_date, artist_id FROM {albums_inc_table}
            ON CONFLICT (id, artist_id) DO UPDATE
            SET
                name = EXCLUDED.name,
                total_tracks = EXCLUDED.total_tracks,
                release_date = EXCLUDED.release_date;
        """))
    print(f'Successfully wrote data to postgres table {albums_table}')

def main():
    # get current dir, where creds and config are placed
    current_dir = os.path.dirname(os.path.abspath(__file__))
    creds_json_path = os.path.join(current_dir, "creds.json")
    config_yaml_path = os.path.join(current_dir, "config.yaml")

    # get creds from json
    creds = get_params_from_json(params_file=creds_json_path)
    # spotify credentials for my_app
    CLIENT_ID = creds['spotify_api']['client_id']
    CLIENT_SECRET = creds['spotify_api']['client_secret']
    # get database config from common config.yaml
    config = get_params_from_yaml(params_file=config_yaml_path)
    db_config = config['database']
    artists_list = config['artists']

    print(f'Requested artists:')
    print(*artists_list, sep=", ")

    # build engine for postgres connection
    postgres_engine = build_engine(drivername="postgresql",
                                   db_params=db_config,
                                   creds=creds['postgres'])
    # prepare schemas and tables
    prepare_postgres(postgres_engine=postgres_engine)

    # Spotify to Postgres
    write_spotify_data_to_postgres(client_id=CLIENT_ID,
                                   client_secret=CLIENT_SECRET,
                                   engine=postgres_engine,
                                   artists_list=artists_list)

if __name__ == "__main__":
    main()





