FROM python:3.10-slim

# Install system dependencies (git is required to fetch NousResearch/Hermes-Agent from GitHub)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user (Hugging Face default requirement for security)
RUN useradd -m -u 1000 user
USER user
ENV PATH="/home/user/.local/bin:$PATH"

WORKDIR /app

# Copy requirements and install
COPY --chown=user requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY --chown=user . /app

# Expose port 7860 (Hugging Face's default routing port)
EXPOSE 7860

# Set environment variable to make Flask bind to 7860
ENV PORT=7860

# Start app.py
CMD ["python", "app.py"]
