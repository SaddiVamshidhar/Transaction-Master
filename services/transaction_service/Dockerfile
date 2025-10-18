# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install netcat for health checks
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1
ENV PYTHONPATH=/app

# Install poetry
RUN pip install poetry

# Copy only requirements to leverage Docker cache
COPY pyproject.toml ./

# Install dependencies
RUN poetry install --no-root --only main

# Copy the rest of the application
COPY . .

# Copy scripts to a location accessible in the container
COPY scripts /app/scripts
RUN chmod +x /app/scripts/*.sh

# Expose the port the app runs on
EXPOSE 8000

# The command from docker-compose.yml will override this,
# but it's good practice to have a default command.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]