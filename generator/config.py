# This module centralizes the Event Generator configuration.

import os

from dotenv import load_dotenv


load_dotenv()


# Telemetry configuration

METRIC_RANGES = {
    "temperature": (10.0, 40.0),
    "humidity": (20.0, 90.0),
    "pressure": (950.0, 1050.0),
}

METRICS = list(METRIC_RANGES.keys())

SCHEMA_VERSION = 1

# Source distribution configuration

SUPPORTED_DISTRIBUTIONS = (
    "uniform",
    "skewed",
)

# In skewed mode, the first source is the hot source and generates 60% of the events.
HOT_SOURCE_PROBABILITY = 0.60


# API configuration

API_URL = os.getenv(
    "GENERATOR_API_URL",
    "http://localhost:8000/",
)

HTTP_TIMEOUT = 5.0


# Generator default parameters

DEFAULT_NUM_SOURCES = 10

DEFAULT_RATE = 10.0

DEFAULT_DURATION = 60.0

DEFAULT_DUPLICATE_RATE = 0.0

DEFAULT_DISTRIBUTION = "uniform"