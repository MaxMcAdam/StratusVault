#!/bin/bash

# StratusVault Daemon Management Script
# Provides easy management of the StratusVault daemon service

SERVICE_NAME="stratusvault-daemon"
CONFIG_FILE="/etc/stratusvault/daemon.json"
LOG_FILE="/var/log/stratusvault/daemon.log"
STATE_FILE="/var/lib/stratusvault/daemon.state"

show_usage() {
    echo "Usage: $0 {start|stop|restart|status|logs|config|add-file|remove-file|list-files|validate}"
    echo ""
    echo "Commands:"
    echo "  start       - Start the daemon"
    echo "  stop        - Stop the daemon"
    echo "  restart     - Restart the daemon"
    echo "  status      - Show daemon status"
    echo "  logs        - Show daemon logs (tail -f)"
    echo "  config      - Edit configuration file"
    echo "  add-file    - Add a file to watch list"
    echo "  remove-file - Remove a file from watch list"
    echo "  list-files  - List currently watched files"
    echo "  validate    - Validate configuration file"
    echo ""
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        echo "This operation requires root privileges"
        exit 1
    fi
}

start_daemon() {
    check_root
    echo "Starting StratusVault daemon..."
    systemctl start "$SERVICE_NAME"
    systemctl --no-pager status "$SERVICE_NAME"
}

stop_daemon() {
    check_root
    echo "Stopping StratusVault daemon..."
    systemctl stop "$SERVICE_NAME"
    echo "Daemon stopped"
}

restart_daemon() {
    check_root
    echo "Restarting StratusVault daemon..."
    systemctl restart "$SERVICE_NAME"
    systemctl --no-pager status "$SERVICE_NAME"
}

show_status() {
    systemctl --no-pager status "$SERVICE_NAME"
    echo ""
    echo "Configuration: $CONFIG_FILE"
    echo "Log file: $LOG_FILE"
    echo "State file: $STATE_FILE"
    
    if [[ -f "$STATE_FILE" ]]; then
        echo ""
        echo "Last state update: $(stat -c %y "$STATE_FILE")"
    fi
}

show_logs() {
    echo "Showing daemon logs (Press Ctrl+C to exit)..."
    journalctl -u "$SERVICE_NAME" -f
}

edit_config() {
    check_root
    if command -v nano &> /dev/null; then
        nano "$CONFIG_FILE"
    elif command -v vim &> /dev/null; then
        vim "$CONFIG_FILE"
    elif command -v vi &> /dev/null; then
        vi "$CONFIG_FILE"
    else
        echo "No suitable editor found. Please edit $CONFIG_FILE manually."
        exit 1
    fi
    
    echo "Configuration updated. Restart the daemon to apply changes."
}

add_file() {
    check_root
    echo "Add file to watch list"
    echo "===================="
    
    read -p "File ID (from server): " file_id
    read -p "File name: " file_name
    read -p "Local path: " local_path
    read -p "Enable monitoring? (y/n): " enable_input
    
    if [[ "$enable_input" =~ ^[Yy]$ ]]; then
        enabled=true
    else
        enabled=false
    fi
    
    # Create backup
    cp "$CONFIG_FILE" "$CONFIG_FILE.bak"
    
    # Add file to configuration (simple approach - would need jq for production)
    echo "Please manually add the following to the watched_files array in $CONFIG_FILE:"
    echo ""
    echo "    {"
    echo "      \"id\": \"$file_id\","
    echo "      \"name\": \"$file_name\","
    echo "      \"local_path\": \"$local_path\","
    echo "      \"last_token\": 0,"
    echo "      \"last_size\": 0,"
    echo "      \"enabled\": $enabled"
    echo "    }"
    echo ""
    echo "Then restart the daemon with: $0 restart"
}

remove_file() {
    check_root
    echo "Remove file from watch list"
    echo "=========================="
    
    if ! command -v jq &> /dev/null; then
        echo "jq is required for this operation. Please install jq or edit $CONFIG_FILE manually."
        exit 1
    fi
    
    # List current files
    echo "Current watched files:"
    jq -r '.watched_files[] | "\(.id) - \(.name) (\(.local_path))"' "$CONFIG_FILE"
    echo ""
    
    read -p "Enter file ID to remove: " file_id
    
    # Create backup
    cp "$CONFIG_FILE" "$CONFIG_FILE.bak"
    
    # Remove file
    jq --arg id "$file_id" '.watched_files = [.watched_files[] | select(.id != $id)]' "$CONFIG_FILE" > "$CONFIG_FILE.tmp"
    mv "$CONFIG_FILE.tmp" "$CONFIG_FILE"
    
    echo