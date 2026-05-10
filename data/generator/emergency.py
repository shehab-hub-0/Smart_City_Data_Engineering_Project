import random
from generator.base_generator import BaseDataGenerator

try:
    from config import OUTLIER_RATE
except ImportError:
    from data.config import OUTLIER_RATE


class EmergencyGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__("emergency")

    def generate_metrics(self):
        """
        Returns emergency metrics with realistic noise.
        Normal: response_time 120–3600s, severity 1–5
        Noise: outlier response times (very fast or very slow), NULL via base class
        """
        types = ["fire", "accident", "medical", "gas_leak", "structural"]
        statuses = ["dispatched", "en_route", "on_site", "resolved"]

        # Response time: normal 120–3600 seconds
        # 3% chance: outlier (very fast first-responder 30–119s, or delayed 3601–7200s)
        if random.random() < OUTLIER_RATE:
            response_time = random.choice(
                [
                    random.randint(30, 119),  # unusually fast
                    random.randint(3601, 7200),  # delayed (up to 2 hours)
                ]
            )
        else:
            response_time = random.randint(120, 3600)

        # Severity: normal 1–5
        severity = random.randint(1, 5)

        return {
            "type": random.choice(types),
            "severity": severity,
            "response_time": response_time,
            "status": random.choice(statuses),
        }

    def generate_record(self, base_time=None):
        zone = self._get_random_zone()
        lat, lon = self._generate_location(zone)
        metrics = self.generate_metrics()
        record = {
            "timestamp": self._generate_timestamp(base_time),
            "location": f"{lat},{lon}",
            "city_zone": zone,
            "domain": "emergency",
        }
        record.update(metrics)
        return record
