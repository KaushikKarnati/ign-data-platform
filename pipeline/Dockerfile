# ign-data-platform/pipeline/Dockerfile
# Extends the official Apache Airflow image with project dependencies.
#
# Why a custom image?
#   The base airflow image does not include boto3 or requests.
#   We install them at build time so they're baked in — faster startup,
#   no pip install on every container restart.
#
# Usage:
#   docker build -t ign-airflow:latest .
#   (docker-compose.yaml references this image automatically)

FROM apache/airflow:2.9.2-python3.11

# Switch to root briefly to install any system-level dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for pip installs (required by Airflow image)
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
