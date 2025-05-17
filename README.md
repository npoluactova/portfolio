# Analytics Engineer portfolio
Repository with different Data Analytics Engineer projects.

## Airflow

Folders structure:
<pre>
   - dags
       - tasks
           - city_bikes
           - tbd
       - utils
</pre>

`Dags` - examples of Airflow dags with BashOperator to run Python scripts.

`Tasks` - examples of Python scripts with different ETL projects.
- `city_bikes_data`: ETL project to extract data from BQ, transform via Pandas and upload result csv files to Google Drive.

`Utils` - examples of custom classes written in Python. 

For example, class `GoogleDriveandSpreadsheets` comtains methods to integrate with Google Drive and Google Spreadsheets.


2. DBT

3. Research Projects
    - A/B tests 
    - 