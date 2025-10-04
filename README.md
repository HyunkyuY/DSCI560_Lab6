# DSCI560_Lab6

## Initialize DB
Before running any program, please execute this sql first.

```mysql -u root -p < wells_schema.sql```

Make sure .env file exists with your DB credentials:
```.env```
```bash
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=yourpassword
DB_NAME=wells_db
```

## Run the Pipeline
```$ python test_pipeline.py```

This will:

- Fetch wells from DB
- Scrape extra info from DrillingEdge
- Clean data
- Update DB with the new fields

## Python Files & Functions

`db_utils.py`

- get_connection()
    - Creates a connection to the database using credentials in .env.
    - Used in: fetch_wells(), update_well()

- fetch_wells()
    - Selects all wells from DB (api_number, well_name_number).
    - Used in: test_pipeline.py (start of pipeline).

- update_well(api, name, data)
    - Updates the wells table with scraped + cleaned fields.
    - Used in: test_pipeline.py (end of pipeline).

`scraper.py`
- search_well(api_number, well_name, headless=True)
  - Uses Selenium to search wells on DrillingEdge.
  - Extracts fields:
      - status
      - type
      - city
      - lat, lon
      - oil_bbl, oil_desc
      - gas_bbl, gas_desc
  - Used in: test_pipeline.py (fetch raw web data).

`preprocess.py`

- preprocess_data(raw_data)
- Cleans raw scraped data:
    - Removes special chars / HTML tags if any
    - Normalizes missing values → N/A or 0
    - Standardizes formatting
- Used before updating DB.

`test_pipeline.py`

Main driver script:

1. Calls fetch_wells() → get well list.
2. Calls search_well() → scrape each well.
3. Calls preprocess_data() → clean scraped info.
4. Calls update_well() → update DB row.