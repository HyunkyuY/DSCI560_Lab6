import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    return conn

def fetch_wells(limit=None):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    query = "SELECT api_number, well_name_number From wells"
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)
    results = cursor.fetchall()
    
    conn.close()
    return results

def update_well(api_number, clean_data):
    conn = get_connection()
    cursor = conn.cursor()
    
    query = """
        UPDATE wells
        SET status=%s, type=%s, city=%s,
            lat=%s, lon=%s,
            oil_bbl=%s, oil_desc=%s,
            gas_bbl=%s, gas_desc=%s
        WHERE api_number=%s
    """
    
    values = (
        clean_data["status"],
        clean_data["type"],
        clean_data["city"],
        clean_data["lat"],
        clean_data["lon"],
        clean_data["oil_bbl"],
        clean_data["oil_desc"],
        clean_data["gas_bbl"],
        clean_data["gas_desc"],
        api_number
    )
    
    
    cursor.exevute(query, values)
    conn.commit()
    conn.close()