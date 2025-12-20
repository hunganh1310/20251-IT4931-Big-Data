# Use official Spark image with Python 3.11
FROM apache/spark:3.5.0-python3

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    gcc \
    g++ \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY data/ ./data/

# Set permissions for spark user
RUN chown -R spark:spark /app && \
    chmod -R 755 /app

# Create Spark directories for spark user
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars /tmp/spark_checkpoints && \
    chown -R spark:spark /home/spark/.ivy2 && \
    chmod -R 777 /home/spark/.ivy2 /tmp/spark_checkpoints

# Set Python path
ENV PYTHONPATH=/app
# Switch back to spark user for security
USER spark

# Default command (this will be overridden in K8s manifests)
# For K8s, you'll run different commands like:
#   - python3 scripts/run_realtime_hanoi.py
#   - python3 scripts/run_realtime_danang.py
#   - python3 scripts/run_archive_hanoi.py
#   - python3 scripts/run_analytics_hourly_hanoi.py
CMD ["echo", "Please specify a command to run. Example: python3 scripts/run_realtime_hanoi.py"]
