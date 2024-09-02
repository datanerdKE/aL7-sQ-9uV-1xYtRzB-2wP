# Import Packages
from google.cloud import bigquery
import numpy as np
import os           
import time           
import datetime
import requests 
import pandas as pd    
import xml.etree.ElementTree as ET
import concurrent.futures
import warnings
warnings.filterwarnings("ignore")

# Initialize the BigQuery client
client = bigquery.Client()

# Function to extract text from XML element safely
def get_xml_text(parent, tag, attrib=None):
    element = parent.find(tag)
    if element is not None:
        if attrib:
            return element.get(attrib)
        return element.text
    return None

# Function to retrieve data from the OpenWeatherMap API
def get_weather(api_key, location):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': location,
        'appid': api_key,
        'units': 'metric',  # Use 'imperial' for Fahrenheit
    }

    try:
        response = requests.get(base_url, params=params)
        data = response.json()

        # Check if the request was successful
        if response.status_code == 200:

            # Extract additional features from XML (Optional, only if necessary)
            xml_url = f'http://api.openweathermap.org/data/2.5/weather?q={location}&mode=xml&appid={api_key}'
            xml_response = requests.get(xml_url)
            xml_root = ET.fromstring(xml_response.content)

            # Create a dictionary with the relevant weather information
            weather_data = {
                'City': location,
                'Time_of_Data_Calculation': pd.to_datetime(data['dt'], unit='s', utc=True),
                'Latitude': data['coord']['lat'],
                'Longitude': data['coord']['lon'],
                'Weather_ID': data['weather'][0]['id'],
                'Weather_Main': data['weather'][0]['main'],
                'Weather_Description': data['weather'][0]['description'],
                'Temperature': data['main']['temp'],
                'Feels_Like': data['main']['feels_like'],
                'Temp_Min': data['main']['temp_min'],
                'Temp_Max': data['main']['temp_max'],
                'Pressure': data['main']['pressure'],
                'Humidity': data['main']['humidity'],
                'Sea_Level': data['main'].get('sea_level'),
                'Ground_Level': data['main'].get('grnd_level'),
                'Visibility': data.get('visibility'),
                'Wind_Speed': data['wind']['speed'],
                'Wind_Degree': data['wind']['deg'],
                'Wind_Gust': data['wind'].get('gust'),
                'Cloudiness': data['clouds']['all'],
                'Cloudiness_Name': get_xml_text(xml_root, 'clouds', 'name'),
                'Rain_1h': data.get('rain', {}).get('1h'),
                'Rain_3h': data.get('rain', {}).get('3h'),
                'Snow_1h': data.get('snow', {}).get('1h'),
                'Snow_3h': data.get('snow', {}).get('3h'),
                'Country_Code': data['sys']['country'],
                'Sunrise_Time': pd.to_datetime(data['sys']['sunrise'], unit='s', utc=True),
                'Sunset_Time': pd.to_datetime(data['sys']['sunset'], unit='s', utc=True),
                'Timezone': data['timezone'],
                'City_ID': data['id'],
                'City_Name': data['name']
            }
            return weather_data
        else:
            print(f"{location} not found in the OpenWeatherMap Database.")
            return None

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    # Replace 'YOUR_API_KEY' with your actual OpenWeatherMap API key
    api_key = '30bc8c5f44c2f641d15a7f617af532c0'

    # List of locations (cities or counties) for which you want to get weather data
    locations = [
        'Baringo', 'Bomet', 'Bungoma', 'Busia', 'Mandeni, KE', 'Embu, KE', 'Garissa', 'Homa Bay', 'Isiolo', 'Kajiado',
        'Kakamega', 'Kericho', 'Kiambu', 'Kilifi', 'Kerugoya', 'Kisii', 'Kisumu', 'Kitui', 'Kwale, KE', 'Nanyuki',
        'Lamu', 'Machakos', 'Makueni', 'Mandera', 'Marsabit', 'Meru', 'Migori', 'Mombasa', "Murang'a", 'Nairobi',
        'Nakuru', 'Nandi, KE', 'Narok', 'Nyamira', 'Oljoro Orok', 'Nyeri', 'Maralal', 'Siaya, KE', 'Taveta',
        'Chogoria', 'Kitale', 'Lodwar', 'Eldoret', 'Vihiga', 'Wajir', 'Kapenguria'
    ]

    # Create an empty DataFrame to store the results
    all_weather_data = pd.DataFrame()

    # Use ThreadPoolExecutor to make concurrent API requests
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_weather, api_key, location) for location in locations]
        for future in concurrent.futures.as_completed(futures):
            weather_data = future.result()
            if weather_data:
                all_weather_data = pd.concat([all_weather_data, pd.DataFrame([weather_data])], ignore_index=True)

    # Define the BigQuery table ID
    table_id = 'project-adrian-julius-aluoch.central_database.openweathermap'

    # Load the data into the BigQuery table
    job = client.load_table_from_dataframe(all_weather_data, table_id)

    # Wait for the job to complete
    while job.state != 'DONE':
        time.sleep(2)
        job.reload()
        print(job.state)

    # Define SQL Query to Retrieve Real Estate Data from Google Cloud BigQuery
    sql = (
           'SELECT *'
           'FROM `central_database.openweathermap`'
           )
    
    # Run SQL Query
    data = client.query(sql).to_dataframe()

    # Check Total Number of Duplicate Records
    duplicated = data.duplicated(subset=['City',
       'Weather_ID', 'Weather_Main', 'Weather_Description', 'Temperature',
       'Feels_Like', 'Temp_Min', 'Temp_Max', 'Pressure', 'Humidity',
       'Sea_Level', 'Ground_Level', 'Visibility', 'Wind_Speed', 'Wind_Degree',
       'Wind_Gust', 'Cloudiness', 'Cloudiness_Name', 'Rain_1h', 'Rain_3h',
       'Snow_1h', 'Snow_3h']).sum()
    
    # Remove Duplicate Records
    data.drop_duplicates(subset=['City',
       'Weather_ID', 'Weather_Main', 'Weather_Description', 'Temperature',
       'Feels_Like', 'Temp_Min', 'Temp_Max', 'Pressure', 'Humidity',
       'Sea_Level', 'Ground_Level', 'Visibility', 'Wind_Speed', 'Wind_Degree',
       'Wind_Gust', 'Cloudiness', 'Cloudiness_Name', 'Rain_1h', 'Rain_3h',
       'Snow_1h', 'Snow_3h'],inplace=True)

    # Load the data into the BigQuery table
    job = client.load_table_from_dataframe(data, table_id)

    # Wait for the job to complete
    while job.state != 'DONE':
        time.sleep(2)
        job.reload()
        print(job.state)

    print("Data has been successfully retrieved, saved, and appended to the BigQuery table.")
