Webapp: Wells Map

This small web application serves wells from the existing `wells` MySQL table and displays them on a Leaflet map.

Setup (dev):

1. Create a `.env` in the `webapp` folder with MySQL credentials (same variables used by preprocessing):

MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=your_user
MYSQL_PASSWORD=your_pass
MYSQL_DB=wells_db

2. Install deps in a virtualenv:

python3 -m pip install -r requirements.txt

3. Run the backend (dev):

python3 backend.py

4. Open http://localhost:5000/ in a browser. The map will request `/api/wells` which returns GeoJSON.

Production notes:
- For production serve static files via Apache/nginx and run the Flask app under gunicorn or uWSGI.
- Ensure DB firewall/credentials are secured.

CSV fallback
-----------
If you don't have a MySQL instance available, the backend will automatically fall back to reading `wells.csv` located in the repository root (one level above `webapp/`). This allows you to run and test the map locally without a DB. Ensure the CSV has numeric `latitude` and `longitude` (or `lat`/`lon`) columns.

To run without DB:

1. Make sure `wells.csv` is present at the repository root (it already exists in this repo).
2. Run the backend as above: `python3 backend.py`.
3. Open http://localhost:5000/ and markers will be shown for rows with valid coordinates.


