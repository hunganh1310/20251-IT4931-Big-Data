
import os
import sys

current_dir = os.getcwd()
sys.path.append(current_dir)

from src.processing.spark_stream_aqicn import main


if __name__ == "__main__":
    main()
