import random
from generator.base_generator import BaseDataGenerator

try:
    from config import OUTLIER_RATE
except ImportError:
    from data.config import OUTLIER_RATE


class WeatherGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__("weather")

    def generate_metrics(self):
        """
        Returns weather metrics with realistic noise.
        Normal range: temp 10–48°C, humidity 15–90%
        Noise: slight out-of-range (sensor drift), NULL handled by base class
        """
        conditions = ["sunny", "cloudy", "rainy", "dusty", "partly_cloudy", "windy"]

        # Temperature: normal 10–48°C
        # 3% chance: slight sensor drift (e.g., 49–55°C or 5–9°C)
        if random.random() < OUTLIER_RATE:
            temperature = round(
                random.choice(
                    [
                        random.uniform(
                            49, 55
                        ),  # slightly above max (sensor overheating)
                        random.uniform(3, 9),  # slightly below normal (cold front)
                    ]
                ),
                2,
            )
        else:
            temperature = round(random.uniform(10, 48), 2)

        # Humidity: normal 15–90%
        # 3% chance: sensor drift (91–100% or 5–14%)
        if random.random() < OUTLIER_RATE:
            humidity = round(
                random.choice(
                    [
                        random.uniform(91, 100),
                        random.uniform(5, 14),
                    ]
                ),
                2,
            )
        else:
            humidity = round(random.uniform(15, 90), 2)

        # Wind speed: 0–60 km/h, 3% chance of storm (60–90 km/h)
        if random.random() < OUTLIER_RATE:
            wind_speed = round(random.uniform(60, 90), 2)
        else:
            wind_speed = round(random.uniform(0, 60), 2)

        return {
            "temperature": temperature,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "condition": random.choice(conditions),
        }

    def generate_record(self, base_time=None):
        zone = self._get_random_zone()
        lat, lon = self._generate_location(zone)
        metrics = self.generate_metrics()
        record = {
            "timestamp": self._generate_timestamp(base_time),
            "location": f"{lat},{lon}",
            "city_zone": zone,
            "domain": "weather",
        }
        record.update(metrics)
        return record
