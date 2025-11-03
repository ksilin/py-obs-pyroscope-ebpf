FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml ./
COPY README.md ./
COPY src/ ./src/
COPY scenarios/ ./scenarios/
COPY util_scripts/ ./util_scripts/
COPY demo.py ./
COPY continuous_demo.py ./
COPY .env.example ./

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Create .env from example if not mounted
RUN cp .env.example .env

# Default command runs all scenarios
CMD ["python", "demo.py", "--scenario", "all", "--messages", "1000"]
