#!/bin/bash
# Install dbt docs server as systemd service
# Run as root or via sudo

set -e

echo "Installing dbt documentation service..."

# Copy systemd-compatible environment file (no 'export' keywords)
cp /srv/projects/dbt-asc/setup/snowflake_env_systemd.template /home/amit/.snowflake_env_systemd
chown amit:datausers /home/amit/.snowflake_env_systemd
chmod 600 /home/amit/.snowflake_env_systemd

# Copy service file to systemd directory
cp /srv/projects/dbt-asc/setup/dbt-docs.service /etc/systemd/system/

# Reload systemd to pick up new service
systemctl daemon-reload

# Enable service to start on boot
systemctl enable dbt-docs.service

# Start service
systemctl start dbt-docs.service

# Show status
systemctl status dbt-docs.service

echo ""
echo "✅ dbt docs service installed and running"
echo ""
echo "Useful commands:"
echo "  sudo systemctl status dbt-docs   # Check status"
echo "  sudo systemctl stop dbt-docs     # Stop service"
echo "  sudo systemctl start dbt-docs    # Start service"
echo "  sudo systemctl restart dbt-docs  # Restart service"
echo "  sudo journalctl -u dbt-docs -f   # View logs (follow mode)"
echo "  sudo journalctl -u dbt-docs -n 50  # View last 50 log lines"
echo ""
echo "Documentation available via:"
echo "  SSH tunnel: ssh -L 8082:localhost:8082 amit@control.accesscharity.org.uk"
echo "  Then visit: http://localhost:8082"
