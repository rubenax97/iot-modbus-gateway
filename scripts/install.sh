#!/bin/bash
# Install Modbus Gateway

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Installing from ${PROJECT_DIR}"

# Copy to /opt
if [ -d "/opt/modbus-gateway" ]; then
    echo "Updating existing installation..."
    cp -r ${PROJECT_DIR}/gateway ${PROJECT_DIR}/app ${PROJECT_DIR}/requirements.txt /opt/modbus-gateway/
else
    echo "New installation..."
    cp -r ${PROJECT_DIR} /opt/modbus-gateway
fi

# Run deployment
cd /opt/modbus-gateway
./scripts/deploy.sh