import requests
import time
import json
from datetime import datetime, timezone
from confluent_kafka import Producer

# ---------------- CONFIGURATION ----------------
API_KEY = "f4ef6c9daa98d5630f690db32af94273"

KAFKA_CONF = {
    'bootstrap.servers': 'weatherappeventhub.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': 'Endpoint=sb://weatherappeventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=diBBdEmD5eEniLK29eLUd4TcYYePly9Kn+AEhCOqaKc='
}

TOPIC = "weather-air-events"

CITIES = [
    ("Delhi", "IN"), ("Mumbai", "IN"), ("Bengaluru", "IN"),
    ("New York", "US"), ("Los Angeles", "US"), ("Chicago", "US"),
    ("London", "GB"), ("Manchester", "GB"),
    ("Paris", "FR"), ("Lyon", "FR"),
    ("Berlin", "DE"), ("Munich", "DE"),
    ("Tokyo", "JP"), ("Osaka", "JP"),
    ("Seoul", "KR"),
    ("Beijing", "CN"), ("Shanghai", "CN"),
    ("Sydney", "AU"), ("Melbourne", "AU"),
    ("Dubai", "AE"), ("Abu Dhabi", "AE"),
    ("Moscow", "RU"), ("Saint Petersburg", "RU"),
    ("São Paulo", "BR"), ("Rio de Janeiro", "BR"),
    ("Toronto", "CA"), ("Vancouver", "CA"),
    ("Mexico City", "MX"),
    ("Buenos Aires", "AR"),
    ("Cairo", "EG"),
    ("Istanbul", "TR"),
    ("Bangkok", "TH"),
    ("Singapore", "SG"),
    ("Johannesburg", "ZA"),
    ("Lagos", "NG"),
    ("Rome", "IT"), ("Milan", "IT"),
    ("Barcelona", "ES"), ("Madrid", "ES")
]

producer = Producer(**KAFKA_CONF)

# ---------------- FUNCTIONS ----------------
def fetch_weather(city, country):
    """Fetch weather data from OpenWeather API"""
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city},{country}&appid={API_KEY}&units=metric"
    resp = requests.get(url)
    data = resp.json()
    if data.get("cod") != 200:
        raise Exception(f"Weather API error: {data.get('message')}")
    return data


def fetch_air_quality(lat, lon):
    """Fetch air quality data from OpenWeather API"""
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
    resp = requests.get(url)
    data = resp.json()
    if "list" not in data:
        raise Exception("Air quality API error")
    return data


def build_record(city, country, weather_data, air_data):
    """Build unified weather + air quality record"""
    lat = weather_data["coord"]["lat"]
    lon = weather_data["coord"]["lon"]
    record = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "city": city,
        "country": country,
        "latitude": lat,
        "longitude": lon,
        "weather_main": weather_data["weather"][0]["main"],
        "weather_description": weather_data["weather"][0]["description"],
        "temperature": weather_data["main"]["temp"],
        "temp_min": weather_data["main"]["temp_min"],
        "temp_max": weather_data["main"]["temp_max"],
        "feels_like": weather_data["main"]["feels_like"],
        "pressure": weather_data["main"]["pressure"],
        "humidity": weather_data["main"]["humidity"],
        "visibility": weather_data.get("visibility"),
        "wind_speed": weather_data["wind"]["speed"],
        "wind_deg": weather_data["wind"]["deg"],
        "cloudiness": weather_data["clouds"]["all"],
        "sunrise": weather_data["sys"]["sunrise"],
        "sunset": weather_data["sys"]["sunset"],
        "aqi": air_data["list"][0]["main"]["aqi"],
        "co": air_data["list"][0]["components"]["co"],
        "no": air_data["list"][0]["components"]["no"],
        "no2": air_data["list"][0]["components"]["no2"],
        "o3": air_data["list"][0]["components"]["o3"],
        "so2": air_data["list"][0]["components"]["so2"],
        "pm2_5": air_data["list"][0]["components"]["pm2_5"],
        "pm10": air_data["list"][0]["components"]["pm10"],
        "nh3": air_data["list"][0]["components"]["nh3"]
    }
    return record


# ---------------- STREAMING LOOP ----------------
print(" Kafka Producer started... (Press Ctrl+C to stop)\n")

while True:
    for city, country in CITIES:
        try:
            weather_data = fetch_weather(city, country)
            lat, lon = weather_data["coord"]["lat"], weather_data["coord"]["lon"]
            air_data = fetch_air_quality(lat, lon)
            record = build_record(city, country, weather_data, air_data)

            # Send to Kafka
            producer.produce(TOPIC, key=city, value=json.dumps(record))
            producer.flush()

            # Print streaming data
            print(f"\n [{record['timestamp']}] City: {city}, {country}")
            print(f"    Weather: {record['weather_main']} ({record['weather_description']})")
            print(f"    Temp: {record['temperature']}°C | Feels Like: {record['feels_like']}°C")
            print(f"    Wind: {record['wind_speed']} m/s | Humidity: {record['humidity']}%")
            print(f"    AQI: {record['aqi']} | PM2.5: {record['pm2_5']} | PM10: {record['pm10']}")
            print("    Event successfully sent to Kafka topic:", TOPIC)

            # Pause to control API calls
            time.sleep(10)

        except Exception as e:
            print(f" Error processing {city}: {e}")
            continue
