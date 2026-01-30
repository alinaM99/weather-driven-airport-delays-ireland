import json
import requests
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient

# =========================
# CONFIG
# =========================

OPENWEATHER_API_KEY = "xyz"

AZURE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=alinadissstorage;"
    "AccountKey=xyz"
    "EndpointSuffix=core.windows.net"
)

CONTAINER_NAME = "weather-raw"
RAW_PREFIX = "raw/onecall/"

# Airport coordinates
AIRPORTS = {
    "DUB": (53.4213, -6.2701),
    "ORK": (51.8413, -8.4911),
    "SNN": (52.6900, -8.9248),
    "NOC": (53.9103, -8.8185),
}

# =========================
# MAIN
# =========================

def main():
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    for airport_code, (lat, lon) in AIRPORTS.items():
        url = (
            "https://api.openweathermap.org/data/3.0/onecall"
            f"?lat={lat}&lon={lon}"
            "&exclude=minutely,daily,alerts"
            "&units=metric"
            f"&appid={OPENWEATHER_API_KEY}"
        )

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        blob_name = f"{RAW_PREFIX}{airport_code}_{timestamp}.json"

        container_client.upload_blob(
            name=blob_name,
            data=json.dumps(data),
            overwrite=True
        )

        print(f"Uploaded raw weather data for {airport_code} -> {blob_name}")


if __name__ == "__main__":
    main()
