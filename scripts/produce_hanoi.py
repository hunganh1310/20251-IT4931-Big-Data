import os
import sys
import time

current_dir = os.getcwd()
sys.path.append(current_dir)

city = "hanoi"

from src.common import logger
from src.ingestion.produce_city import main


if __name__ == "__main__":
    try:
        while True:
            try:
                logger.info("Calling AQICN API for %s", city.upper())
                main(city=city)
                logger.info("Sleeping for 10 seconds")
                time.sleep(10)
            except Exception as e:
                logger.error(f"Error in producer loop: {e}")
                time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
