import os
import sys
from multiprocessing import Process

current_dir = os.getcwd()
sys.path.append(current_dir)

from src.processing.spark_stream import main
from src.common import logger


if __name__ == "__main__":
    cities = ["hanoi", "saigon", "danang", "haiphong", "cantho", "nhatrang", "vungtau"]

    logger.info(f"Starting Spark streaming for all cities: {cities}")

    processes = []
    for city in cities:
        p = Process(target=main, args=(city,))
        p.start()
        processes.append(p)
        logger.info(f"Started process for {city}")

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("Stopping all city streaming processes")
        for p in processes:
            p.terminate()
            p.join()
