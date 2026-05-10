import uuid
from generator.base_generator import BaseDataGenerator
from generator.air_quality import AirQualityGenerator
from generator.emergency import EmergencyGenerator
from generator.traffic import TrafficGenerator
from generator.weather import WeatherGenerator


class UnifiedGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__("unified")
        # Instantiate sub-generators to reuse their metrics logic
        self.generators = {
            "air_quality": AirQualityGenerator(),
            "emergency": EmergencyGenerator(),
            "traffic": TrafficGenerator(),
            "weather": WeatherGenerator(),
        }

    def generate_record(self, base_time=None):
        """
        Generates a flat unified record for easy consumption by CSV/DB.
        """
        zone = self._get_random_zone()
        lat, lon = self._generate_location(zone)
        timestamp = self._generate_timestamp(base_time)

        # Base information
        # We assign a device_id based on zone and domain to simulate multiple sensors per zone
        record = {
            "id": str(uuid.uuid4())[:8],
            "device_id": f"dev-{zone}-{str(uuid.uuid4())[:4]}",
            "timestamp": timestamp,
            "zone": zone,
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "metadata_source_type": "batch",  # Default for batch runs
        }

        # Add domain metrics with prefix
        for domain, gen in self.generators.items():
            metrics = gen.generate_metrics()
            for key, val in metrics.items():
                record[f"{domain}_{key}"] = val

        return record
