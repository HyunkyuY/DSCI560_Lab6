from bs4 import BeautifulSoup
import re
import pandas as pd


def clean_text(text):
    if not text:
        return "N/A"
    text = re.sub(r"<.*?>", "", text)
    text = re.sub(r"[^a-zA-Z0-9\s&]", "", text)
    return text.strip() if text else "N/A"

def clean_number(val):
    if not val:
        return 0
    val = re.sub(r"[^\d]", "", str(val))
    return int(val) if val.isdigit() else 0

def clean_float(val):
    if not val:
        return None
    try:
        return float(val.strip())
    except:
        return None

def preprocess_data(raw_data: dict):
    return {
        "status" : clean_text(raw_data.get("status")),
        "type": clean_text(raw_data.get("type")),
        "city": clean_text(raw_data.get("city")),
        "latitude": clean_float(raw_data.get("latitude")),
        "longitude": clean_float(raw_data.get("longitude")),
        "oil_bbl": clean_number(raw_data.get("oil_bbl")),
        "oil_desc": clean_text(raw_data.get("oil_desc")),
        "gas_bbl": clean_number(raw_data.get("gas_bbl")),
        "gas_desc": clean_text(raw_data.get("gas_desc"))
    }