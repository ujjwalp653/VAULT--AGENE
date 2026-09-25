#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/vault}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
NODE_COUNT="${NODE_COUNT:-8}"
NODE_START_PORT="${NODE_START_PORT:-8101}"

sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip nginx git

mkdir -p "$APP_DIR"
cd "$APP_DIR"

if [ ! -d .venv ]; then
  "$PYTHON_BIN" -m venv .venv
fi
source .venv/bin/activate
pip install --upgrade pip
pip install -r backend/requirements.txt

mkdir -p backend/data

sudo tee /etc/systemd/system/vault-coordinator.service >/dev/null <<UNIT
[Unit]
Description=VAULT Coordinator
After=network.target

[Service]
User=$USER
WorkingDirectory=$APP_DIR/backend
Environment=NODE_COUNT=$NODE_COUNT
Environment=NODE_START_PORT=$NODE_START_PORT
Environment=NODE_BASE_URL=http://127.0.0.1
Environment=DATA_ROOT=$APP_DIR/backend/data
Environment=DB_PATH=$APP_DIR/backend/data/vault.db
Environment=PYTHONUNBUFFERED=1
ExecStart=$APP_DIR/.venv/bin/python -m uvicorn vault.main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

for i in $(seq 1 "$NODE_COUNT"); do
  port=$((NODE_START_PORT+i-1))
  sudo tee "/etc/systemd/system/vault-node-${i}.service" >/dev/null <<UNIT
[Unit]
Description=VAULT Storage Node $i
After=network.target

[Service]
User=$USER
WorkingDirectory=$APP_DIR/backend
Environment=PYTHONUNBUFFERED=1
ExecStart=$APP_DIR/.venv/bin/python -m vault.node_agent --id $i --port $port --host 127.0.0.1 --storage $APP_DIR/backend/data
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT
done

sudo systemctl daemon-reload
sudo systemctl enable --now vault-coordinator.service
for i in $(seq 1 "$NODE_COUNT"); do sudo systemctl enable --now "vault-node-${i}.service"; done

echo "VAULT backend installed. Coordinator: http://127.0.0.1:8000"
echo "Use Nginx + TLS or Cloudflare Tunnel to expose HTTPS publicly."
