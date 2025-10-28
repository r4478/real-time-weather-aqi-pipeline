pip install confluent-kafka
from confluent_kafka import Consumer, KafkaException
import json
import time

KAFKA_CONF = {
    'bootstrap.servers': 'weatherappeventhub.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': 'Endpoint=sb://weatherappeventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=diBBdEmD5eEniLK29eLUd4TcYYePly9Kn+AEhCOqaKc=',
    'group.id': 'weather-consumer-group',
    'auto.offset.reset': 'latest'
}

TOPIC = "weather-air-events"

def main():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([TOPIC])

    print(f" Connected to Kafka topic: {TOPIC}")
    print(" Listening for messages... (press Ctrl+C to exit)\n")

    try:
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                data = json.loads(msg.value().decode("utf-8"))
                print(" Received event:")
                print(f"   City: {data['city']}, {data['country']}")
                print(f"    Temp: {data['temperature']}°C | Feels Like: {data['feels_like']}°C")
                print(f"    Weather: {data['weather_main']} ({data['weather_description']})")
                print(f"    Wind: {data['wind_speed']} m/s | Humidity: {data['humidity']}%")
                print(f"    AQI: {data['aqi']} | PM2.5: {data['pm2_5']} | PM10: {data['pm10']}")
                print(f"    Timestamp: {data['timestamp']}")
                print("-" * 80)
            except json.JSONDecodeError:
                print(" Received non-JSON message:", msg.value())

    except KeyboardInterrupt:
        print("\n Stopping consumer...")
    finally:
        consumer.close()
        print(" Consumer closed successfully.")

if __name__ == "__main__":
    main()
