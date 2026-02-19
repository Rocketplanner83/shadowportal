FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (cache layer)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application source into the image at build time
COPY . /app

# Note: do not create a user inside the image; run as UID 568 at runtime
EXPOSE 8799

# Run as unprivileged UID 568 (apps)
USER 568:568

CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:8799", "app:app"]
