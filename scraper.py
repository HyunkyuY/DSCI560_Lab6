from bs4 import BeautifulSoup
import re
import requests
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def normalize_name(name:str) -> str:
    if not name:
        return ""
    t = name.upper().replace("&", "AND")
    t = re.sub(r"\s+", " ", t).strip()
    return t


BASE_URL = "https://www.drillingedge.com/search"


def search_well(api_number, well_name, headless=True):
    if not api_number and not well_name:
        print("Missing both api_number and well_name, skip search.")
        return None
    
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
        
    driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=options)
    
    try:
        driver.get(BASE_URL)
        
        # find the search box
        # Search by well_name only
        well_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "well_name"))
        )
        well_input.clear()
        well_input.send_keys(well_name)
        well_input.send_keys(Keys.RETURN)
        
        
        # Wait for the search results
        links = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table tr td a"))
        )
        
        target_link = None
        name_norm = normalize_name(well_name)
        api_str = (api_number or "").strip()

        for link in links:
            link_text = link.text.strip().upper()
            link_norm = normalize_name(link_text)
            
            if name_norm in link_norm or link_norm in name_norm:
                if api_str and api_str in (link.get_attribute("href") or ""):
                    print(f"Found by Well Name & API match: {well_name} ({api_number})")
                elif api_str:
                    print(f"Well Name matched but API mismatch, fallback to Well Name: {well_name}")
                else:
                    print(f"Found by Well Name (no API check): {well_name}")

                target_link = link
                break
        
        if not target_link:
            print(f"No matching link found for api={api_number}, well_name={well_name}")
            return None
        
        # click the well name
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(target_link))
        target_link.click()
        
        # wait for well details page loaded
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table tr"))
        )
    
        
        # results table - status, type, closest city
        rows = driver.find_elements(By.CSS_SELECTOR, "table tr")
        data = {}
        for row in rows:
            ths = row.find_elements(By.TAG_NAME, "th")
            tds = row.find_elements(By.TAG_NAME, "td")
            for th, td in zip(ths, tds):
                key = th.text.strip()
                val = td.text.strip()
                data[key] = val
        
        
        # process lat and lon
        lat, lon = None, None
        if "Latitude / Longitude" in data:
            coords = data["Latitude / Longitude"].split(",")
            if len(coords) == 2:
                try:
                    lat = float(coords[0].strip())
                except:
                    lat = None
                try:
                    lon = float(coords[1].strip())
                except:
                    lon = None
        
        # Oil & Gas defaukt
        oil_val, oil_desc = 0, "N/A"
        gas_val, gas_desc = 0, "N/A"
        
        # blcok_state - oil_bbl, gas_bbl
        stats = driver.find_elements(By.CSS_SELECTOR, "p.block_stat")
        
        for stat in stats:
            text = stat.text.strip()
            try:
                span_val = stat.find_element(By.TAG_NAME, "span").text.strip()
            except:
                span_val = "0"
                
            num_str = span_val.replace(",", "").strip()
                
            if "Oil Produced" in text:
                oil_val = int(num_str) if num_str.isdigit() else 0
                oil_desc = text
            elif "Gas Produced" in text:
                gas_val = int(num_str) if num_str.isdigit() else 0
                gas_desc = text
        
        # result
        result = {
            "status": data.get("Well Status", "N/A"),
            "type": data.get("Well Type", "N/A"),
            "city": data.get("Closest City", "N/A"),
            "lat": lat,
            "lon": lon,
            "oil_bbl": oil_val,
            "oil_desc": oil_desc,
            "gas_bbl": gas_val,
            "gas_desc": gas_desc
        }
        
        
        return result
        
    except Exception as e:
        print(f"Error scraping well {api_number}: {e}")
        return None
    
    finally:
        driver.quit()