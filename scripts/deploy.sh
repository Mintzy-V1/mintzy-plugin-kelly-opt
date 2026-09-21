#!/usr/bin/env bash
# Deploy the repo checkout to the app dir + S3. Runs on the self-hosted VM runner.
set -euo pipefail

REPO_DIR="${1:?usage: deploy.sh <repo_checkout_dir>}"
APP_DIR="${APP_DIR:-/home/admin/mintzy-plugin}"
S3_BUCKET="${S3_BUCKET:-mintzy-plugin}"
BACKUP_DIR="${BACKUP_DIR:-/home/admin/mintzy-plugin-backups}"

if [ ! -f "$REPO_DIR/api_server.py" ]; then
    echo "ERROR: $REPO_DIR does not look like the repo checkout (api_server.py missing)" >&2
    exit 1
fi

mkdir -p "$APP_DIR" "$BACKUP_DIR"

# 1) Backup current code (excluding venv, logs, runtime data)
TS="$(date +%Y%m%d-%H%M%S)"
tar czf "$BACKUP_DIR/mintzy-plugin-$TS.tar.gz" \
    --exclude='venv' --exclude='.git' --exclude='__pycache__' --exclude='*.pyc' \
    --exclude='logs' --exclude='*.log' --exclude='*.csv' --exclude='.env' \
    -C "$APP_DIR" . 2>/dev/null || true
# Keep only last 5 backups
ls -1t "$BACKUP_DIR"/mintzy-plugin-*.tar.gz 2>/dev/null | tail -n +6 | xargs -r rm -f

# 2) Sync code into app dir (preserve .env, venv, logs, runtime data on the VM)
tar czf - \
    --exclude='.git' --exclude='.gitignore' --exclude='.env' --exclude='.env.*' \
    --exclude='venv' --exclude='logs' --exclude='__pycache__' --exclude='*.pyc' \
    --exclude='.pytest_cache' --exclude='.claude' --exclude='.DS_Store' \
    --exclude='*.log' --exclude='*.csv' \
    -C "$REPO_DIR" . | tar xzf - -C "$APP_DIR"
find "$APP_DIR" -name '._*' -delete

# 3) Remove stale monolith files that no longer exist in the modular repo
rm -f "$APP_DIR/auto_trader.py" \
      "$APP_DIR/auto_trader_exposure_expansion.py" \
      "$APP_DIR/auto_trader_exposure_expansion_org.py"

# 4) Restart the app via systemd
if systemctl list-unit-files | grep -q '^mintzy-plugin.service'; then
    sudo systemctl restart mintzy-plugin
else
    echo "WARNING: mintzy-plugin.service not found on VM — app not restarted" >&2
fi

# 5) Health check
for i in $(seq 1 15); do
    if curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
        echo "Health check OK (attempt $i)"
        break
    fi
    [ "$i" -eq 15 ] && { echo "ERROR: app unhealthy after restart" >&2; exit 1; }
    sleep 2
done

# 6) Sync code to S3 (mirror the repo, delete stale objects)
aws s3 sync "$REPO_DIR" "s3://$S3_BUCKET" --delete \
    --exclude ".git/*" --exclude ".gitignore" --exclude ".env" --exclude ".env.*" \
    --exclude "venv/*" --exclude "logs/*" --exclude "__pycache__/*" --exclude "*.pyc" \
    --exclude ".pytest_cache/*" --exclude ".claude/*" --exclude "*.log" --exclude "*.csv" \
    --exclude ".DS_Store"

echo "DEPLOY OK"