#!/bin/bash

# This script runs inside the container, so it uses poetry run

# Exit immediately if a command exits with a non-zero status.
set -e

echo "Running database migrations..."
poetry run alembic upgrade head
echo "Migrations applied successfully."