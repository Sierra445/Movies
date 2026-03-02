FROM python:3.10-slim

WORKDIR /app

# Install Python dependencies first for better Docker layer caching.
COPY etl/requirements.txt /app/etl/requirements.txt
RUN pip install --no-cache-dir -r /app/etl/requirements.txt

# Copy project files needed by the ETL job.
COPY etl /app/etl
COPY data /app/data

CMD ["python", "-u", "etl/etl.py"]
