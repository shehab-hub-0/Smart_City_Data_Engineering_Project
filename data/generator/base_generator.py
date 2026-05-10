import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Adjust import to work whether run from data or project root
try:
    from config import (
        SEED,
        CITY_ZONES,
        MISSING_RATE,
        DUPLICATE_RATE,
        TIMESTAMP_JITTER_SECONDS,
    )
except ImportError:
    from data.config import (
        SEED,
        CITY_ZONES,
        MISSING_RATE,
        DUPLICATE_RATE,
        TIMESTAMP_JITTER_SECONDS,
    )


class BaseDataGenerator:
    """
    Base class for all domain-specific data generators.
    Handles shared logic like:
    - timestamp generation (with realistic format noise)
    - location generation based on zones
    - injection of realistic data imperfections
    """

    def __init__(self, domain_name):
        self.domain_name = domain_name
        seed_value = (SEED + hash(domain_name)) % (2**32)
        random.seed(seed_value)
        np.random.seed(seed_value)
        self._history = []

    def _generate_timestamp(self, base_time=None):
        """Generates timestamps with realistic format noise (no future dates)."""
        if base_time is None:
            base_time = datetime.now()

        # Add realistic sensor clock-drift jitter
        jitter = timedelta(
            seconds=random.uniform(-TIMESTAMP_JITTER_SECONDS, TIMESTAMP_JITTER_SECONDS)
        )
        ts = base_time + jitter

        # 5% chance: different but VALID format (Spark Silver should parse these)
        if random.random() < 0.05:
            formats = [
                "%d-%m-%Y %H:%M:%S",  # European style
                "%Y/%m/%d %H:%M",  # Slash-separated
                "%m-%d-%Y",  # US style (date only)
            ]
            return ts.strftime(random.choice(formats))

        # Standard ISO-8601
        return ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _generate_location(self, zone_name):
        zone = CITY_ZONES[zone_name]
        lat = random.uniform(zone["lat"] - zone["radius"], zone["lat"] + zone["radius"])
        lon = random.uniform(zone["lon"] - zone["radius"], zone["lon"] + zone["radius"])
        return round(lat, 6), round(lon, 6)

    def _get_random_zone(self):
        return random.choice(list(CITY_ZONES.keys()))

    def generate_record(self, base_time=None):
        raise NotImplementedError

    def _inject_imperfections(self, record):
        """
        Injects REALISTIC data quality issues:
        - NULL values (sensor offline / missing reading)
        - Whitespace & casing noise in categorical fields
        """
        # 1. NULL injection (sensor dropout — most common real-world issue)
        for key in list(record.keys()):
            if (
                key not in ["id", "timestamp", "domain", "device_id"]
                and random.random() < MISSING_RATE
            ):
                record[key] = None

        # 2. Categorical noise: leading/trailing spaces, inconsistent casing
        for key, val in record.items():
            if isinstance(val, str) and key not in [
                "id",
                "timestamp",
                "domain",
                "location",
                "device_id",
            ]:
                if random.random() < 0.05:
                    record[key] = f"  {val}  "
                elif random.random() < 0.05:
                    record[key] = val.upper() if random.random() > 0.5 else val.lower()

        return record

    def generate_batch(self, count, base_time=None):
        """Generates a batch with realistic duplicates and imperfections."""
        if base_time is None:
            base_time = datetime.now()

        records = []
        for i in range(count):
            current_time = base_time + timedelta(seconds=i)

            # Duplicate injection (sensor re-sent the same reading)
            if self._history and random.random() < DUPLICATE_RATE:
                record = random.choice(self._history).copy()
            else:
                record = self.generate_record(base_time=current_time)
                record = self._inject_imperfections(record)
                self._history.append(record)
                if len(self._history) > 1000:
                    self._history.pop(0)

            records.append(record)

        return records

    def to_dataframe(self, records):
        return pd.DataFrame(records)
