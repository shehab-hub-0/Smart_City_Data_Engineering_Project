import argparse
import sys
import logging
import random
from datetime import datetime

# Import generators

from generator.unified_generator import UnifiedGenerator

# Import writers
from outputs.csv_writer import CSVWriter

# Writers will be imported dynamically to prevent missing dependencies from blocking unused modes

import os

# Path Logic
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)
if current_dir not in sys.path:
    sys.path.append(current_dir)

# Import config
try:
    from config import SEED, NUM_RECORDS, LOG_FORMAT, LOG_LEVEL
except ImportError:
    from data.config import SEED, NUM_RECORDS, LOG_FORMAT, LOG_LEVEL

# Setup logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("SmartCityMain")


def run_generation(mode):
    """
    Main execution logic.
    """
    logger.info(f"Starting Smart City Data Generation in mode: {mode}")
    random.seed(SEED)

    # Initialize selected writer
    writer = None
    if mode == "csv":
        writer = CSVWriter()
    elif mode == "db":
        from outputs.db_writer import DBWriter

        writer = DBWriter()
    elif mode == "kafka":
        from outputs.kafka_producer import SmartCityKafkaProducer

        writer = SmartCityKafkaProducer()
    else:
        logger.error(f"Invalid mode: {mode}")
        sys.exit(1)

    try:
        start_time = datetime.now()

        if mode == "kafka":
            # --- Unified Mode for Streaming (CONTINUOUS) ---
            logger.info(
                "Starting CONTINUOUS Unified Streaming (Press Ctrl+C to stop)..."
            )
            gen = UnifiedGenerator()

            try:
                iteration = 1
                while True:
                    logger.info(f"Streaming Batch #{iteration}...")
                    records = gen.generate_batch(NUM_RECORDS, base_time=datetime.now())
                    writer.write("unified", records)
                    iteration += 1
                    # Small pause between batches if needed, though producer has its own delay
            except KeyboardInterrupt:
                logger.info("🛑 Streaming stopped by user.")
        else:
            # --- Unified Mode for Batch (CSV/DB) ---
            logger.info("Starting UNIFIED Batch Generation...")
            gen = UnifiedGenerator()

            logger.info(f"Generating {NUM_RECORDS} unified records...")
            records = gen.generate_batch(NUM_RECORDS, base_time=start_time)
            df = gen.to_dataframe(records)
            writer.write("unified", df)

        logger.info(f"Generation and export successfully completed!")

    except Exception as e:
        logger.error(f"An error occurred during generation: {e}")
        traceback_str = str(e)
        logger.debug(traceback_str)
    finally:
        if mode in ["db", "kafka"] and writer:
            writer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="🏙️ Smart City Data Generation System")
    parser.add_argument(
        "--mode",
        choices=["csv", "db", "kafka"],
        required=True,
        help="Output target: local csv, postgres database, or kafka streaming topic",
    )

    args = parser.parse_args()
    run_generation(args.mode)
