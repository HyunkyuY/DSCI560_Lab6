from db_utils import fetch_wells, update_well
from scraper import search_well

def test_pipeline(limit=None, headless=True):
    wells = fetch_wells()
    if limit:
        wells = wells[:limit]

    for w in wells:
        api = w.get("api_number")
        name = w.get("well_name_number")

        print(f"\nTesting {api} - {name}")

        try:
            raw_data = search_well(api, name, headless=headless)
        except Exception as e:
            print(f"Error during search_well for {name}: {e}")
            continue

        if not raw_data:
            print(f"Failed scraping {name}")
            continue

        print(f"   Raw: {raw_data}")

        # Clean data
        clean = {
            "status": raw_data.get("status") or "N/A",
            "type": raw_data.get("type") or "N/A",
            "city": raw_data.get("city") or "N/A",
            "lat": raw_data.get("lat"),
            "lon": raw_data.get("lon"),
            "oil_bbl": raw_data.get("oil_bbl") or 0,
            "oil_desc": raw_data.get("oil_desc") or "NA",
            "gas_bbl": raw_data.get("gas_bbl") or 0,
            "gas_desc": raw_data.get("gas_desc") or "NA",
        }
        print(f"   Clean: {clean}")

        # Update DB
        try:
            update_well(api, name, clean)
            print(f"Updated well {api or name}")
        except Exception as e:
            print(f"Failed DB update for {api or name}: {e}")

if __name__ == "__main__":
    # limit=5 → only test 5 wells to avoid too many website requests
    test_pipeline(limit=None, headless=False)