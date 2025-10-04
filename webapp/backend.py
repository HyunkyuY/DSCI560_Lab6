#!/usr/bin/env python3
from flask import Flask, jsonify, send_from_directory, abort
from dotenv import load_dotenv
import os
import mysql.connector
import csv
from pathlib import Path
import logging

load_dotenv()

app = Flask(__name__, static_folder='static', static_url_path='/static')

def get_conn():
    return mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', '3306')),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DB', 'wells_db')
    )

@app.route('/api/wells')
def api_wells():
    features = []

    # 1) Try DB
    try:
        conn = get_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM wells")
        rows = cur.fetchall()
        cur.close(); conn.close()

        for r in rows:
            # try multiple column name variants
            latv = r.get('lat') or r.get('latitude')
            lonv = r.get('lon') or r.get('longitude')
            try:
                lat = float(latv) if latv not in (None, '') else None
                lon = float(lonv) if lonv not in (None, '') else None
            except Exception:
                lat = lon = None
            if lat is None or lon is None:
                continue
            props = {k: (v if v is not None else '') for k, v in r.items()}
            features.append({
                'type': 'Feature',
                'geometry': { 'type': 'Point', 'coordinates': [lon, lat] },
                'properties': props
            })
    except Exception as e:
        logging.warning('DB read failed: %s', e)

    # 2) Fallback: read wells.csv in repo root if features empty
    if not features:
        csv_path = Path(__file__).resolve().parents[1] / 'wells.csv'
        if csv_path.exists():
            try:
                with csv_path.open(newline='', encoding='utf-8') as fh:
                    rdr = csv.DictReader(fh)
                    for r in rdr:
                        latv = r.get('latitude')
                        lonv = r.get('longitude')
                        try:
                            lat = float(latv) if latv not in (None, '', '0.0') else None
                            lon = float(lonv) if lonv not in (None, '', '0.0') else None
                        except Exception:
                            lat = lon = None
                        if lat is None or lon is None:
                            continue
                        props = {k: (v if v is not None else '') for k, v in r.items()}
                        features.append({
                            'type': 'Feature',
                            'geometry': { 'type': 'Point', 'coordinates': [lon, lat] },
                            'properties': props
                        })
            except Exception as e:
                logging.warning('CSV fallback read failed: %s', e)

    return jsonify({ 'type': 'FeatureCollection', 'features': features })

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

if __name__ == '__main__':
    # Simple dev server. For production, use Apache/nginx or gunicorn.
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '5000')), debug=True)
