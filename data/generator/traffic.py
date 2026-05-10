import random
from generator.base_generator import BaseDataGenerator

try:
    from config import OUTLIER_RATE
except ImportError:
    from data.config import OUTLIER_RATE


class TrafficGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__("traffic")

    def generate_metrics(self):
        """
        Returns traffic metrics with realistic noise.
        Normal: speed 5–130 km/h, vehicles 0–500
        Noise: GPS-reported speed glitch, sensor over-count
        """
        road_types = ["highway", "arterial", "local", "collector"]

        # Vehicle count: 0–500 (always integer)
        vehicle_count = random.randint(0, 500)

        # Avg speed: correlated with vehicle density
        # 3% chance: GPS glitch (reported speed slightly off — 131–160 km/h)
        if random.random() < OUTLIER_RATE:
            avg_speed = round(random.uniform(131, 160), 2)  # GPS overshoot
        else:
            if vehicle_count > 400:
                avg_speed = round(random.uniform(5, 25), 2)
            elif vehicle_count > 200:
                avg_speed = round(random.uniform(25, 65), 2)
            else:
                avg_speed = round(random.uniform(65, 130), 2)

        congestion = "high" if avg_speed < 25 else "medium" if avg_speed < 65 else "low"

        return {
            "vehicle_count": vehicle_count,
            "avg_speed": avg_speed,
            "congestion_level": congestion,
            "road_type": random.choice(road_types),
        }

    def generate_record(self, base_time=None):
        zone = self._get_random_zone()
        lat, lon = self._generate_location(zone)
        metrics = self.generate_metrics()
        record = {
            "timestamp": self._generate_timestamp(base_time),
            "location": f"{lat},{lon}",
            "city_zone": zone,
            "domain": "traffic",
        }
        record.update(metrics)
        return record
