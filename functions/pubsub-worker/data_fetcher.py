import requests
from bs4 import BeautifulSoup
import sys
import re

BASE_WEB_URL = "https://gosurf.co.il"

def get_all_beaches():
    """Fetches all beaches from the GoSurf 'ALL SPOTS' page.
    Returns:
        list: A list of dicts representing beaches if the request is successful, None otherwise."""
    
    url = f"{BASE_WEB_URL}/surf-spots"
    print(f"Fetching all beaches from {url}...")

    try:
        reponse = requests.get(url, timeout=10)
        reponse.raise_for_status()

        soup = BeautifulSoup(reponse.text, 'html.parser')
        beaches = []

        # find the main container for the list
        list_containers = soup.find_all('div', class_='fw spots_a')
        if not list_containers:
            print("Could not find the beaches container in the HTML.", file=sys.stderr)
            return None
        for container in list_containers:
            for link in container.find_all('a', href=True):
                href = link.get('href')
            
                if href and '/forecast/' in href:
                    slug = href.split('/forecast/')[1].strip('/')
                    name = link.text.strip()

                    # Prevent duplicates just in case
                    if not any(b['slug'] == slug for b in beaches):
                        beaches.append({'name': name, 'slug': slug})
        return beaches
    
    except requests.RequestException as e:
        print(f"Error fetching beaches: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Unexpected error parsing beaches: {e}", file=sys.stderr)
        return None
    
def get_forecast(beach_slug):
    """
    Fetches a detailed 4-day forecast for specific hours (06, 09, 12).
    
    Args:
        beach_slug (str): The slug identifier (e.g., 'sdot-yam').
        
    Returns:
        list: A list of dictionaries (one for each day), containing 
              hourly data, or None on error.
    """
    if not beach_slug:
        print("Error: beach_slug is required.", file=sys.stderr)
        return None
            
    url = f"{BASE_WEB_URL}/forecast/{beach_slug}"
    print(f"Fetching detailed forecast for {beach_slug} from {url}...")
    
    # Define the hours we are interested in
    target_hours = ["06", "09", "12"]
    
    # This will hold the final data, e.g., [ {day_name: '...', hours: [...]}, ... ]
    forecast_data = []

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 1. Find all day containers (div.day.fw), but only take the first 4
        day_containers = soup.find_all('div', class_='day fw')
        
        if not day_containers:
            print("Error: Could not find any 'day fw' containers.", file=sys.stderr)
            return None

        # 2. Loop over each day container
        for day in day_containers:
            day_title_tag = day.find('h2')
            day_name = day_title_tag.text.strip() if day_title_tag else 'Unknown Day'
            
            day_result = {
                "day_name": day_name,
                "hourly_forecast": []
            }

            # 3. Find all time rows within this day
            time_rows = day.find_all('tr', class_='chart_tr')
            
            for row in time_rows:
                # 4. Check if this row is one of our target hours
                hour_tag = row.find('td', class_='hour_cont')
                if hour_tag and hour_tag.text.strip() in target_hours:
                    hour_text = hour_tag.text.strip()
                    
                    # 5. Extract the specific columns we want
                    wave_tag = row.find('td', class_='waves')
                    sea_desc_tag = row.find('td', class_='wave_height_desc') # Using your class name
                    wind_speed_tag = row.find('td', class_='wind')
                    wind_dir_tag = row.find('td', class_='wind_dir_desc')
                    
                    # Build the data object for this hour
                    hour_data = {
                        "time": hour_text,
                        "wave_height": wave_tag.text.strip() if wave_tag else 'N/A',
                        "sea_description": sea_desc_tag.text.strip() if sea_desc_tag else 'N/A',
                        "wind_speed": wind_speed_tag.text.strip() if wind_speed_tag else 'N/A',
                        "wind_direction": wind_dir_tag.text.strip() if wind_dir_tag else 'N/A',
                    }
                    
                    day_result["hourly_forecast"].append(hour_data)
            
            # Add this day's data to our main list
            if day_result["hourly_forecast"]:
                forecast_data.append(day_result)
        
        return forecast_data
            
    except requests.RequestException as e:
        print(f"Error fetching forecast for '{beach_slug}': {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error parsing forecast page for '{beach_slug}': {e}", file=sys.stderr)
        return None

# ----- Local testing block -----
if __name__ == "__main__":
    print("--- Running data_fetcher.py (Scraping Version 4) for testing ---")
    
    # 1. Test fetching all beaches (unchanged)
    beaches = get_all_beaches()
    if beaches:
        print(f"\nSuccessfully fetched {len(beaches)} beaches (full list).")
        print("Example beaches:")
        print(f" - Name: {beaches[0]['name']}, Slug: {beaches[0]['slug']}")
    else:
        print("\nFailed to fetch beaches.")
            
    print("-" * 20)

    # 2. Test fetching the new detailed forecast
    test_slug = "sdot-yam" 
    forecast = get_forecast(test_slug)
    
    if forecast:
        print(f"\nSuccessfully fetched detailed forecast for '{test_slug}':\n")
        
        # Loop through each day and print the data
        for day in forecast:
            print(f"--- {day['day_name']} ---")
            for hour_data in day['hourly_forecast']:
                print(f"  {hour_data['time']}:")
                print(f"    Wave: {hour_data['wave_height']}")
                print(f"    Sea: {hour_data['sea_description']}")
                print(f"    Wind: {hour_data['wind_speed']}, {hour_data['wind_direction']}")
            print("") # Newline for readability
    else:
        print(f"\nFailed to fetch detailed forecast for '{test_slug}'.")