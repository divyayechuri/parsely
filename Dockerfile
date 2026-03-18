# ─────────────────────────────────────────────────────
# Parsely Dockerfile
# ─────────────────────────────────────────────────────
# Multi-purpose image for running:
#   - Streamlit app (default)
#   - Pipeline scripts
#   - Tests
#
# Build:  docker build -t parsely .
# Run:    docker run -p 8501:8501 parsely
# ─────────────────────────────────────────────────────

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (Docker layer caching — dependencies
# are reinstalled only when requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source code
COPY src/ src/
COPY dbt/ dbt/
COPY data/ data/
COPY tests/ tests/
COPY .env.example .env.example

# Expose Streamlit port
EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Default command: run the Streamlit app
CMD ["streamlit", "run", "src/app/streamlit_app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]
