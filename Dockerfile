FROM python:3.11-slim

# Install system dependencies (git is required to fetch NousResearch/Hermes-Agent from GitHub)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    libpq-dev \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Pre-install Playwright OS dependencies as root
RUN pip install --no-cache-dir playwright && \
    playwright install-deps chromium

# Create a non-root user (Hugging Face default requirement for security)
RUN useradd -m -u 1000 user
USER user
ENV PATH="/home/user/.local/bin:$PATH"

WORKDIR /app

# Copy requirements and install
COPY --chown=user requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Download Chromium browser as the non-root user
RUN playwright install chromium

# Copy the rest of the application files
COPY --chown=user . /app

# Expose port 7860 (Hugging Face's default routing port)
EXPOSE 7860

# Set environment variable to make Flask bind to 7860
ENV PORT=7860

# Start app.py
CMD ["python", "app.py"]
