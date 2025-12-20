import os
import signal
import sys
import threading
import time

current_dir = os.getcwd()
sys.path.append(current_dir)

from src.common import logger
from src.ingestion.produce_city import main as producer_main
from src.processing.analytics_hourly import main as analytics_main
from src.processing.archive import main as archive_main
from src.processing.realtime import main as realtime_main

city = "cantho"
shutdown_event = threading.Event()


def signal_handler(signum, frame):
    logger.info("Shutdown signal received. Stopping all processes...")
    shutdown_event.set()


def run_producer():
    """Run data producer for Can Tho"""
    try:
        while not shutdown_event.is_set():
            try:
                logger.info("Calling AQICN API for %s", city.upper())
                producer_main(city=city)
                logger.info("Producer: Sleeping for 300 seconds")
                if shutdown_event.wait(300):  # Sleep for 5 minutes or until shutdown
                    break
            except Exception as e:
                logger.error(f"Error in producer loop: {e}")
                if shutdown_event.wait(10):  # Sleep for 10 seconds or until shutdown
                    break
    except Exception as e:
        logger.error(f"Producer thread error: {e}")


def run_realtime():
    """Run realtime processing for Can Tho"""
    try:
        logger.info("Starting realtime processing for %s", city.upper())
        realtime_main(city=city)
    except Exception as e:
        logger.error(f"Realtime processing error: {e}")


def run_analytics():
    """Run hourly analytics for Can Tho"""
    try:
        while not shutdown_event.is_set():
            try:
                logger.info("Running hourly analytics for %s", city.upper())
                analytics_main(city=city)
                logger.info("Analytics: Sleeping for 3600 seconds")
                if shutdown_event.wait(3600):  # Sleep for 1 hour or until shutdown
                    break
            except Exception as e:
                logger.error(f"Error in analytics loop: {e}")
                if shutdown_event.wait(60):  # Sleep for 1 minute or until shutdown
                    break
    except Exception as e:
        logger.error(f"Analytics thread error: {e}")


def run_archive():
    """Run archive processing for Can Tho"""
    try:
        while not shutdown_event.is_set():
            try:
                logger.info("Running archive processing for %s", city.upper())
                archive_main(city=city)
                logger.info("Archive: Sleeping for 7200 seconds")
                if shutdown_event.wait(7200):  # Sleep for 2 hours or until shutdown
                    break
            except Exception as e:
                logger.error(f"Error in archive loop: {e}")
                if shutdown_event.wait(60):  # Sleep for 1 minute or until shutdown
                    break
    except Exception as e:
        logger.error(f"Archive thread error: {e}")


if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting all processes for %s", city.upper())

    # Create and start threads
    threads = []

    # Start producer thread
    producer_thread = threading.Thread(target=run_producer, name=f"Producer-{city}")
    producer_thread.daemon = True
    threads.append(producer_thread)

    # Start realtime processing thread
    realtime_thread = threading.Thread(target=run_realtime, name=f"Realtime-{city}")
    realtime_thread.daemon = True
    threads.append(realtime_thread)

    # Start analytics thread
    analytics_thread = threading.Thread(target=run_analytics, name=f"Analytics-{city}")
    analytics_thread.daemon = True
    threads.append(analytics_thread)

    # Start archive thread
    archive_thread = threading.Thread(target=run_archive, name=f"Archive-{city}")
    archive_thread.daemon = True
    threads.append(archive_thread)

    # Start all threads
    for thread in threads:
        thread.start()
        logger.info("Started thread: %s", thread.name)

    try:
        # Wait for shutdown signal
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        shutdown_event.set()

    # Wait for all threads to complete
    logger.info("Waiting for threads to complete...")
    for thread in threads:
        if thread.is_alive():
            thread.join(timeout=5)  # Wait up to 5 seconds for each thread

    logger.info("All processes for %s stopped", city.upper())
