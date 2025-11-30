import requests
import json
import psycopg2
import os
from datetime import datetime

# ---- CONFIG ----
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
NEON_DB_URL = os.getenv("NEON_DB_URL")

# ---- LOAD CITY JSON ----
def load_city_data():
  with open("DB-Data/data-convertors/germany_city_data.json", "r", encoding="utf-8") as f:
    return json.load(f)

# ---- FETCH WEATHER ----
def fetch_weather(lat, lon):
  url = (
      f"https://api.openweathermap.org/data/2.5/weather?"
      f"lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
  )
  response = requests.get(url)
  response.raise_for_status()
  return response.json()

# ---- SAVE TO DATABASE ----
def save_weather_to_db(city_id, w):
  conn = psycopg2.connect(NEON_DB_URL)
  cur = conn.cursor()

  query = """
    INSERT INTO weather_data (
      datetime, city_id, sunrise, sunset, weather_icon, weather_description, snow_1h, rain_1h, visibility, temperature, feels_like, cloud,
      humidity, pressure, wind_deg, wind_speed
    )
    VALUES (
      NOW(), %s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s
    );
  """

  cur.execute(
    query,
    (
      city_id,
      w["sys"].get("sunrise"),
      w["sys"].get("sunset"),
      w["weather"][0]["icon"],
      w["weather"][0]["description"],
      w.get("snow", {}).get("1h"),
      w.get("rain", {}).get("1h"),
      w.get("visibility"),
      w["main"]["temp"],
      w["main"]["feels_like"],
      w["clouds"]["all"],
      w["main"]["humidity"],
      w["main"]["pressure"],
      w["wind"].get("deg"),
      w["wind"]["speed"],
    ),
  )

  conn.commit()
  cur.close()
  conn.close()


# ---- MAIN FUNCTION ----
def main():
  cities = load_city_data()

  for city in cities:
    print(f"Fetching weather for {city['city_id']}...")

    lat = city["latitude"]
    lon = city["longitude"]
    city_id = city["city_id"]  # Must match your city table IDs

    weather_json = fetch_weather(lat, lon)
    save_weather_to_db(city_id, weather_json)

  print("Weather data successfully saved!")


# ---- RUN ----
if __name__ == "__main__":
  main()