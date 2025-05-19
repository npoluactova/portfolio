# Analytics Engineer portfolio
Repository with different Data Analytics Engineer projects.

## Airflow

Folders structure:
```
   - dags
       - tasks
           - city_bikes
           - tbd
       - utils
```

`Dags` - examples of Airflow dags with BashOperator to run Python scripts.

---

`Tasks` - examples of Python scripts with different ETL projects.
- `city_bikes_data`: ETL project to extract data from BQ, transform via Pandas and upload result csv files to Google Drive. Methods to integrate with Google Drive are implemented in custom class.

---

`Utils` - examples of custom classes written in Python. 

For example, class `GoogleDriveandSpreadsheets` contains methods to integrate with Google Drive and Google Spreadsheets.

---

2. DBT

3. Research Projects
    - A/B tests 
    - 