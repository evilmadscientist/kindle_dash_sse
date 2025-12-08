from flask import Flask, Response
import requests
import ntplib
from datetime import datetime, timezone
import pytz
import time

app = Flask(__name__)

PACIFIC_TZ = pytz.timezone("America/Los_Angeles")
NTP_SERVER = "time.cloudflare.com"

CAZ_ZONE = "CAZ006" # NOAA weather.gov zone for San Francisco, CAZ006 by default
WEATHER_URL = "https://api.weather.gov/zones/forecast/" + CAZ_ZONE + "/forecast"
HEADERS = {"User-Agent": "MyWeatherApp (you@example.com)"}

def get_pacific_time_from_ntp():
    client = ntplib.NTPClient()
    response = client.request(NTP_SERVER, version=3)
    utc_dt = datetime.fromtimestamp(response.tx_time, tz=timezone.utc)
    return utc_dt.astimezone(PACIFIC_TZ)

def get_weather_sf():
    res = requests.get(WEATHER_URL, headers=HEADERS, timeout=10)

    if not res.ok:
        return f"Error fetching weather: {res.status_code}"

    data = res.json()
    return data.get("properties", {}).get("periods", [{}])[0].get(
        "detailedForecast",
        "No forecast available."
    )

@app.route('/stream_time')
def stream_time():
    def event_stream():
        while True:
            try:
                pacific_time = get_pacific_time_from_ntp()
                msg = pacific_time.strftime("%I:%M:%S %p")
                yield f"data: {msg}\n\n"
            except Exception as e:
                yield f"data: Error: {e}\n\n"
            time.sleep(1)   # Update interval (1 second)

    return Response(event_stream(), mimetype="text/event-stream")

@app.route('/stream_date')
def stream_date():
    def event_stream():
        while True:
            try:
                pacific_time = get_pacific_time_from_ntp()
                msg = pacific_time.strftime("%a %b %d, %Y")
                yield f"data: {msg}\n\n"
            except Exception as e:
                yield f"data: Error: {e}\n\n"
            time.sleep(1)   # Update interval (1 second)

    return Response(event_stream(), mimetype="text/event-stream")


@app.route("/stream_weather")
def stream_weather():
    def event_stream():
        while True:
            try:
                forecast = get_weather_sf()
                yield f"data: {forecast}\n\n"
            except Exception as e:
                yield f"data: Error: {e}\n\n"
            time.sleep(600)  # Update interval (10 min)

    return Response(event_stream(), mimetype="text/event-stream")

@app.route("/")
def home():
    return open("index.html").read()

if __name__ == "__main__":
    print("Server running at http://127.0.0.1:5000")
    app.run(debug=True, threaded=True)
