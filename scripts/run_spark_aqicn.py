import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from src.processing.spark_stream_aqicn import main


if __name__ == "__main__":
    main()
