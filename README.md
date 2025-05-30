# Analytics Engineer portfolio
Repository with different Data Analytics Engineer projects.

## Airflow

Folders structure:
```
   - dags
       - tasks
           - city_bikes
           - spotify
           - openweathermap
       - utils
```

`Dags` - examples of Airflow dags with BashOperator to run Python scripts.
- `bq_2_google_drive`: Dag that runs ETL script `city_bikes_data.py`
- `API_2_postgres`: Dag that sequentially runs ETL scripts `spotify_data.py` and `opeenweathermap_data.py`
- 
---

`Tasks` - examples of Python scripts with different ETL projects.
- `city_bikes_data.py`: ETL project to extract data from BQ, transform via Pandas and upload result csv files to Google Drive. Methods to integrate with Google Drive are implemented in custom class.
- `spotify_data.py`: ETL project to request data from Spotify API, transform via Pandas and merge result in tables in Postgres database.
- `opeenweathermap_data.py`: ETL project to request data from Openweathermap API, transform via Pandas and merge result in tables in Postgres database.
- 
---

`Utils` - examples of custom classes written in Python. 

For example, class `GoogleDriveandSpreadsheets` contains methods to integrate with Google Drive and Google Spreadsheets.

---

2. DBT

3. Research Projects
    - A/B tests 
    - 