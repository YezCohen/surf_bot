#!/bin/bash

# --- This script must run as root ---
set -e # Exit immediately if a command exits with a non-zero status.

echo "VM setup script started..."

# 1. Install Dependencies (Docker, gcloud CLI)
# ------------------------------------------------
apt-get update
apt-get install -y \
    docker.io \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    google-cloud-sdk # We need this SDK to access Secret Manager

# Start and enable Docker
systemctl start docker
systemctl enable docker
echo "Docker installed and started."

# 2. Create Docker Volume
# ------------------------------------------------
docker volume create postgres-data
echo "Docker volume 'postgres-data' ensured."

# 3. Fetch the Postgres Password from Secret Manager
# ------------------------------------------------
# Define the secret name and version (latest)
SECRET_NAME="surf-bot-postgres-password"
SECRET_VERSION="latest"

echo "Fetching secret: ${SECRET_NAME}..."

# Use the 'gcloud' command (installed above) to access the secret.
# This works because the VM's Service Account has permission.
# We save the password into a variable.
DB_PASSWORD=$(gcloud secrets versions access ${SECRET_VERSION} --secret=${SECRET_NAME} --format='get(payload.data)' | base64 --decode)

if [ -z "${DB_PASSWORD}" ]; then
    echo "ERROR: Failed to fetch DB password from Secret Manager. Exiting."
    exit 1
fi

echo "Successfully fetched DB password."

# 4. Run the Postgres Container
# ------------------------------------------------
DB_USER="surf_bot_user"
DB_NAME="surf_bot_db"

# Stop and remove old container if it exists
if [ "$(docker ps -q -f name=surf-postgres)" ]; then
    echo "Stopping existing 'surf-postgres' container..."
    docker stop surf-postgres
fi
if [ "$(docker ps -aq -f name=surf-postgres)" ]; then
    echo "Removing existing 'surf-postgres' container..."
    docker rm surf-postgres
fi

echo "Starting new 'surf-postgres' container..."
# Use the $DB_PASSWORD variable fetched from Secret Manager
docker run -d \
  --name surf-postgres \
  -e POSTGRES_USER=${DB_USER} \
  -e POSTGRES_PASSWORD=${DB_PASSWORD} \
  -e POSTGRES_DB=${DB_NAME} \
  -v postgres-data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --restart always \
  postgres:15

echo "Postgres container is up and running."
echo "VM Setup is complete!"