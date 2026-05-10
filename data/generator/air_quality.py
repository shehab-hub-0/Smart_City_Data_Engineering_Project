import random
from generator.base_generator import BaseDataGenerator

try:
    from config import OUTLIER_RATE
except ImportError:
    from data.config import OUTLIER_RATE


class AirQualityGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__("air_quality")

    def generate_metrics(self):
        """
        Returns air quality metrics with realistic noise.
        Normal: PM2.5 5–250, PM10 correlated, AQI 0–500
        Noise: sensor spike (high PM reading), NULL via base class
        """

        # PM2.5: 5–250 µg/m³
        # 3% chance: sensor spike (251–400 µg/m³ — sandstorm/fire nearby)
        if random.random() < OUTLIER_RATE:
            pm2_5 = round(random.uniform(251, 400), 2)
        else:
            pm2_5 = round(random.uniform(5, 250), 2)

        # PM10: typically 1.2–2x PM2.5
        pm10 = round(pm2_5 * random.uniform(1.2, 2.0), 2)

        # CO: 0.1–10 mg/m³
        co = round(random.uniform(0.1, 10), 2)

        # NO2: 5–200 µg/m³
        no2 = round(random.uniform(5, 200), 2)

        # O3: 10–180 µg/m³
        o3 = round(random.uniform(10, 180), 2)

        # Quality index: 0–500 AQI scale
        quality_index = min(500, max(0, round((pm2_5 / 250) * 500)))

        return {
            "pm2_5": pm2_5,
            "pm10": pm10,
            "co": co,
            "no2": no2,
            "o3": o3,
            "quality_index": quality_index,
        }

    def generate_record(self, base_time=None):
        zone = self._get_random_zone()
        lat, lon = self._generate_location(zone)
        metrics = self.generate_metrics()
        record = {
            "timestamp": self._generate_timestamp(base_time),
            "location": f"{lat},{lon}",
            "city_zone": zone,
            "domain": "air_quality",
        }
        record.update(metrics)
        return record
