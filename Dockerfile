# Use a lean, official Python runtime as a parent image
FROM python:3.11-slim

# Set an environment variable to prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory in the container
WORKDIR /app

# --- INSTALL SYSTEM DEPENDENCIES FOR PLAYWRIGHT ---
RUN apt-get update && apt-get install -y \
    # Core deps for Chromium/Playwright
    libnss3 libnspr4 libdbus-1-3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 libatspi2.0-0 libxrandr2 \
    libxss1 libxcursor1 libxi6 libxtst6 ca-certificates fonts-liberation libappindicator3-1 \
    libasound2 libxshmfence-dev libdrm2 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libcairo-gobject2 libgtk-3-0 \
    --no-install-recommends \
    # Cleanup to keep the image small
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container first to leverage Docker layer caching
COPY requirements.txt .

# Install Python packages from the requirements file
RUN pip install --no-cache-dir -r requirements.txt

# Install the Playwright browser binaries (with all deps)
RUN playwright install --with-deps chromium

# Copy your application code into the container
COPY proof_bot/ ./proof_bot/
COPY proof_bot/sic_selector_colossus.py .   
COPY selectors.json .
COPY .env .

# Command to run your application when the container starts
CMD ["python", "-m", "proof_bot.main"]