# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install system dependencies (if needed)
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     && rm -rf /var/lib/apt/lists/*

# Copy only the requirements files first to leverage Docker cache
COPY requirements.txt ./
COPY requirements/ ./requirements/

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Now install your project in editable mode
RUN pip install -e .
# Expose the port for Streamlit (default 8501)
EXPOSE 8501

# Default command to run Streamlit app (adjust as needed)
ENV PYTHONPATH=/app
CMD ["streamlit", "run", "streamlit_apps/app_main.py"]