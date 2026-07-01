FROM python:3.13-slim

WORKDIR /app

# Copy requirements if they exist
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create non-root user
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app
USER appuser

# Default command - can be overridden in compose
CMD ["python", "-u", "main.py"]