#!/bin/bash
# Update Modbus Gateway

set -e

APP_NAME="modbus-gateway"
APP_DIR="/opt/${APP_NAME}"

if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (sudo)"
    exit 1
fi

echo "Updating..."

# Backup config
if [ -f "${APP_DIR}/app/config/config.json" ]; then
    cp ${APP_DIR}/app/config/config.json /tmp/config.json.backup
fi

# Copy new files
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cp -r ${PROJECT_DIR}/gateway ${PROJECT_DIR}/app ${PROJECT_DIR}/requirements.txt ${APP_DIR}/

# Restore config
if [ -f "/tmp/config.json.backup" ]; then
    cp /tmp/config.json.backup ${APP_DIR}/app/config/config.json
    rm /tmp/config.json.backup
fi

# Update dependencies
cd ${APP_DIR}
source venv/bin/activate
pip install --upgrade -r requirements.txt

# Restart
systemctl restart ${APP_NAME}

echo "Update complete!"
systemctl status ${APP_NAME}