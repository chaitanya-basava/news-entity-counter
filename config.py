import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path('./.env'))

# news api configuration
CATEGORY = os.getenv("CATEGORY")
API_KEY = os.getenv("API_KEY")

# kafka configuration
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
INPUT_TOPIC = os.getenv("INPUT_TOPIC")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC")
