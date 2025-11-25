FROM python:3.9-slim

# Install Java (required for PySpark) and system dependencies
RUN apt-get update && \
    apt-get install -y default-jre gcc libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY scripts/ scripts/

# Set Python path
ENV PYTHONPATH=/app

CMD ["python", "scripts/run_cold_storage.py"]
