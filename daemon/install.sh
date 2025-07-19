#!/bin/bash
set -e

# StratusVault Daemon Installation Script
# This script sets up the StratusVault daemon as a systemd service

DAEMON_USER="stratusvault"
DAEMON_GROUP="stratusvault"
DAEMON_HOME="/var/lib/stratusvault"
LOG_DIR="/var/log/stratusvault"
CONFIG_DIR="/etc/stratusvault"
BINARY_PATH="/usr/local/bin/stratusvault-daemon"
SERVICE_FILE="/etc/systemd/system/stratusvault-daemon.service"

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

echo "Installing StratusVault Daemon..."

# Create system user and group
if ! id "$DAEMON_USER" &>/dev/null; then
    echo "Creating system user $DAEMON_USER..."
    useradd --system --home-dir "$DAEMON_HOME" --create-home \
            --shell /bin/false --comment "StratusVault Daemon" \
            "$DAEMON_USER"
fi

# Create directories
echo "Creating directories..."
mkdir -p "$DAEMON_HOME" "$LOG_DIR" "$CONFIG_DIR"

# Set ownership and permissions
chown "$DAEMON_USER:$DAEMON_GROUP" "$DAEMON_HOME" "$LOG_DIR"
chmod 750 "$DAEMON_HOME" "$LOG_DIR"
chmod 755 "$CONFIG_DIR"

# Copy binary (assuming it's built and available)
if [[ -f "./stratusvault-daemon" ]]; then
    echo "Installing daemon binary..."
    cp "./stratusvault-daemon" "$BINARY_PATH"
    chown root:root "$BINARY_PATH"
    chmod 755 "$BINARY_PATH"
else
    echo "Warning: Binary not found. Please build and copy stratusvault-daemon to $BINARY_PATH"
fi

# Create default configuration if it doesn't exist
if [[ ! -f "$CONFIG_DIR/daemon.json" ]]; then
    echo "Creating default configuration..."
    cat > "$CONFIG_DIR/daemon.json" << 'EOF'
{
  "server_address": "localhost:50051",
  "poll_interval": "30s",
  "state_file": "/var/lib/stratusvault/daemon.state",
  "log_file": "/var/log/stratusvault/daemon.log",
  "max_retries": 3,
  "retry_delay": "5s",
  "chunk_size": 32768,
  "concurrent_files": 5,
  "watched_files": []
}
EOF
    chown root:root "$CONFIG_DIR/daemon.json"
    chmod 644 "$CONFIG_DIR/daemon.json"
fi

# Install systemd service file
echo "Installing systemd service..."
cat > "$SERVICE_FILE" << 'EOF'
[Unit]
Description=StratusVault File Synchronization Daemon
Documentation=https://github.com/MaxMcAdam/StratusVault
After=network.target
Wants=network.target

[Service]
Type=simple
User=stratusvault
Group=stratusvault
WorkingDirectory=/var/lib/stratusvault
ExecStart=/usr/local/bin/stratusvault-daemon /etc/stratusvault/daemon.json
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=30
KillMode=mixed
KillSignal=SIGTERM
TimeoutStopSec=30

# Security settings
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ReadWritePaths=/var/lib/stratusvault /var/log/stratusvault
ProtectHome=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictSUIDSGID=yes
RestrictRealtime=yes
RestrictNamespaces=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
SystemCallArchitectures=native

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096
MemoryMax=512M
TasksMax=1024

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=stratusvault-daemon

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and enable service
echo "Configuring systemd service..."
systemctl daemon-reload
systemctl enable stratusvault-daemon

# Create log rotation configuration
echo "Setting up log rotation..."
cat > "/etc/logrotate.d/stratusvault" << 'EOF'
/var/log/stratusvault/*.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 0640 stratusvault stratusvault
    postrotate
        systemctl reload stratusvault-daemon
    endscript
}
EOF

echo "Installation complete!"
echo ""
echo "Next steps:"
echo "1. Edit /etc/stratusvault/daemon.json to configure your watched files"
echo "2. Start the service: systemctl start stratusvault-daemon"
echo "3. Check status: systemctl status stratusvault-daemon"
echo "4. View logs: journalctl -u stratusvault-daemon -f"
echo ""
echo "Configuration file: $CONFIG_DIR/daemon.json"
echo "Log file: $LOG_DIR/daemon.log"
echo "State file: $DAEMON_HOME/daemon.state"