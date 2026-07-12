#!/usr/bin/env bash
set -euo pipefail
APP_DIR="/opt/a_share_quant_web"
DATA_DIR="/var/lib/quant-web"
ENV_FILE="/etc/quant-web.env"
SERVICE_FILE="/etc/systemd/system/quant-web.service"

if ! id quantweb >/dev/null 2>&1; then
  sudo useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin quantweb
fi
sudo install -d -o root -g root -m 0755 "$APP_DIR"
sudo install -d -o quantweb -g quantweb -m 0700 "$DATA_DIR" "$DATA_DIR/logs" "$DATA_DIR/a_share_daily_reports" "$DATA_DIR/.a_share_cache"
if [ -f "$APP_DIR/opening_watchlist.json" ] && [ ! -f "$DATA_DIR/opening_watchlist.json" ]; then
  sudo cp "$APP_DIR/opening_watchlist.json" "$DATA_DIR/opening_watchlist.json"
  sudo chown quantweb:quantweb "$DATA_DIR/opening_watchlist.json"
  sudo chmod 0600 "$DATA_DIR/opening_watchlist.json"
fi

cd "$APP_DIR"
if [ ! -x .venv/bin/python ]; then
  sudo python3 -m venv .venv
fi
sudo .venv/bin/python -m pip install --upgrade pip
sudo .venv/bin/python -m pip install -r requirements.txt

if [ ! -f "$ENV_FILE" ]; then
  TOKEN="$(python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
)"
  sudo sh -c "cat > '$ENV_FILE'" <<EOF
QUANT_WEB_HOST=0.0.0.0
QUANT_WEB_PORT=8766
QUANT_WEB_TOP=80
QUANT_WEB_MAX_STOCKS=1200
QUANT_WEB_FULL=1
QUANT_WEB_WORKERS=8
QUANT_WEB_SCHEDULE=15:35,21:00
QUANT_WEB_OPENING_SCHEDULE=09:35,09:50,10:15,10:30
QUANT_WEB_DATA_DIR=$DATA_DIR
QUANT_WEB_AUTO_SYNC_MIN_SCORE=72
QUANT_WEB_AUTO_SYNC_MAX_FAILURE_RATE=0.10
QUANT_WEB_MAX_CONCURRENT_REQUESTS=64
QUANT_WEB_TOKEN=$TOKEN
EOF
fi
sudo chown root:root "$ENV_FILE"
sudo chmod 0600 "$ENV_FILE"

# 源码和虚拟环境保持 root 只写，运行账户只能写数据目录。
sudo chown -R root:root "$APP_DIR"
sudo chmod 0755 "$APP_DIR"
sudo find "$APP_DIR" -type d -exec chmod 0755 {} +
sudo find "$APP_DIR" -type f -exec chmod go-w {} +

sudo cp quant-web.service "$SERVICE_FILE"
sudo chown root:root "$SERVICE_FILE"
sudo chmod 0644 "$SERVICE_FILE"
sudo systemctl daemon-reload
sudo systemctl enable --now quant-web
sudo systemctl restart quant-web
sudo systemctl status quant-web --no-pager
