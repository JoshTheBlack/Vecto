FROM python:3.12-slim

# Prevent Python from writing .pyc files and keep stdout unbuffered for Docker logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies (wkhtmltopdf for invoices, libpq-dev for postgres)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    wkhtmltopdf \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

# Expose port 8000 for Hypercorn
EXPOSE 8000

# Start Hypercorn using your ASGI config
CMD ["hypercorn", "config.asgi:application", "--bind", "0.0.0.0:8000"]