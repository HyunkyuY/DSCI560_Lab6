from bs4 import BeautifulSoup
import requests
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

BASE_URL = "https://www.drillingedge.com/search"


def search_well(api_number, well_name, headless=True):
    if not api_number and not well_name:
        print("Missing both api_number and well_name, skip search.")
        return None
    
    options = webdriver.FirefoxOptions()
    if headless:
        options.add_argument("--headless")
        
    driver = webdriver.Firefox(options=options)
    
    try:
        driver.get(BASE_URL)
        
        # find the search box
        # well name input
        if well_name:
            well_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "well_name"))  
            )
            well_input.send_keys(well_name)
        
        # api_number input
        if api_number:
            api_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "api_no"))
            )
        
            api_input.send_keys(api_number)
            api_input.send_keys(Keys.RETURN)
            
        else:
            # if there is no api_number, use well name to trigger searching
            well_input.send_keys(Keys.RETURN)
        
        
        # Wait for the search results and choose the first one
        result_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "td.well-name a"))
        )
        result_link.click()
        
        # Wait for page loaded
        WebDriverWait(driver, 10).until(
            EC.url_contains("/wells/")
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
        
        # blcok_state - oil_bbl, gas_bbl
        stats = driver.find_elements(By.CSS_SELECTOR, "p.block_stat")
        
        oil_val, gas_val = None, None
        
        for stat in stats:
            text = stat.text.strip()
            try:
                span_val = stat.find_element(By.TAG_NAME, "span").text.strip()
            except:
                span_val = "0"
                
            if "Oil Produced" in text:
                oil_val = int(span_val.replace(",", "")) if span_val.isdigit() else 0
                oil_desc = text
            elif "Gas Produced" in text:
                gas_val = int(span_val.replace(",", "")) if span_val.isdigit() else 0
                gas_desc = text
        
        # result
        result = {
            "status": data.get("Well Status", "N/A"),
            "type": data.get("Well Type", "N/A"),
            "city": data.get("Closest City", "N/A"),
            "oil_bbl": oil_val if oil_val is not None else 0,
            "oil_desc": oil_desc if oil_desc else "N/A",
            "gas_bbl": gas_val if gas_val is not None else 0,
            "gas_desc": gas_desc if gas_desc else "N/A"
        }
        
        
        return result
        
    except Exception as e:
        print(f"Error scraping well {api_number}: {e}")
        return None
    
    finally:
        driver.quit()