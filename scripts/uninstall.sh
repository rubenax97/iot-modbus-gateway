#!/bin/bash
# Uninstall Modbus Gateway

set -e

APP_NAME="modbus-gateway"
APP_DIR="/opt/${APP_NAME}"

if [ "$EUID" -ne 0 ]; then 
    echo "Please run as root (sudo)"
    exit 1
fi

echo "Stopping service..."
systemctl stop ${APP_NAME} 2>/dev/null || true
systemctl disable ${APP_NAME} 2>/dev/null || true

echo "Removing files..."
rm -f /etc/systemd/system/${APP_NAME}.service
rm -f /etc/logrotate.d/${APP_NAME}
rm -rf ${APP_DIR}

systemctl daemon-reload

echo "Uninstall complete!"