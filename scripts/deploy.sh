#!/bin/bash
# Deploy Modbus Gateway to remote Linux server via SSH (with SSH keys)

set -e

APP_NAME="modbus-gateway"
REMOTE_DIR="/tmp/${APP_NAME}-deploy"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== Modbus Gateway Deployment ===${NC}"
echo ""

# Get target information
read -p "Target IP/hostname: " TARGET_HOST
read -p "SSH user [root]: " SSH_USER
SSH_USER=${SSH_USER:-root}

echo -e "${YELLOW}Deploying to ${SSH_USER}@${TARGET_HOST}${NC}"
echo ""

# Create remote directory and copy files in one go
echo -e "${YELLOW}Copying files to target...${NC}"
ssh "${SSH_USER}@${TARGET_HOST}" "rm -rf ${REMOTE_DIR} && mkdir -p ${REMOTE_DIR}"
scp -r ../gateway ../app ../requirements.txt "${SSH_USER}@${TARGET_HOST}:${REMOTE_DIR}/"

# Deploy and start
echo -e "${YELLOW}Deploying and starting service...${NC}"
ssh "${SSH_USER}@${TARGET_HOST}" << 'EOF'
set -e
APP_NAME="modbus-gateway"
REMOTE_DIR="/tmp/${APP_NAME}-deploy"

# Create directories
mkdir -p /opt/${APP_NAME} /var/log/${APP_NAME}

# Copy files
cp -r ${REMOTE_DIR}/* /opt/${APP_NAME}/

# Setup Python
cd /opt/${APP_NAME}
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set permissions
chown -R root:root /opt/${APP_NAME}
chmod -R 755 /opt/${APP_NAME}

# Create systemd service
cat > /etc/systemd/system/${APP_NAME}.service << 'SERVICE'
[Unit]
Description=Modbus Gateway Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/${APP_NAME}
ExecStart=/opt/${APP_NAME}/venv/bin/python3 -m app.main /opt/${APP_NAME}/app/config/config.json
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
SERVICE

# Start service
systemctl daemon-reload
systemctl enable ${APP_NAME}
systemctl start ${APP_NAME}

# Cleanup
rm -rf ${REMOTE_DIR}

echo "Service started successfully!"
EOF

echo ""
echo -e "${GREEN}=== Deployment complete! ===${NC}"
echo ""
echo "Target: ${SSH_USER}@${TARGET_HOST}"
echo ""
echo "Commands:"
echo "  systemctl status ${APP_NAME}  # Check service status"
echo "  systemctl restart ${APP_NAME} # Restart service"
echo "  tail -f /var/log/${APP_NAME}/app.log  # View logs"