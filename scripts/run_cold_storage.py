import os
import sys
from multiprocessing import Process

current_dir = os.getcwd()
sys.path.append(current_dir)

from src.processing.spark_cold_storage import main
from src.common import logger


if __name__ == "__main__":
    cities = ["hanoi", "saigon", "danang", "haiphong", "cantho", "nhatrang", "vungtau"]

    logger.info(f"Starting cold storage archiver for all cities: {cities}")

    processes = []
    for city in cities:
        p = Process(target=main, args=(city,))
        p.start()
        processes.append(p)
        logger.info(f"Started cold storage process for {city}")

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("Stopping all cold storage processes")
        for p in processes:
            p.terminate()
            p.join()
