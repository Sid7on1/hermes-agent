#!/bin/bash
set -e

# Make sure it's run as root
if [ "$EUID" -ne 0 ]; then
  echo "❌ Error: Please run as root (sudo ./setup_gce.sh)"
  exit 1
fi

echo "=========================================================="
echo "      🚀 Hermes Agent Google Compute Engine Setup"
echo "=========================================================="

# 1. Update and install packages
echo "📦 [1/5] Updating packages and installing dependencies..."
apt-get update -y
apt-get install -y python3 python3-pip python3-venv git build-essential libpq-dev curl

# 2. Clone or Update repo
REPO_DIR="/opt/hermes-agent"
echo "📂 [2/5] Configuring project directory at $REPO_DIR..."
if [ -d "$REPO_DIR" ]; then
  echo "👉 Directory exists. Fetching latest updates from GitHub..."
  cd "$REPO_DIR"
  git fetch --all
  git reset --hard origin/main
else
  echo "👉 Cloning repository..."
  git clone https://github.com/Sid7on1/hermes-agent.git "$REPO_DIR"
  cd "$REPO_DIR"
fi

# 3. Create venv and install dependencies
echo "🐍 [3/5] Building Python virtual environment..."
python3 -m venv venv
./venv/bin/pip install --upgrade pip
echo "📥 Installing packages from requirements.txt..."
./venv/bin/pip install -r requirements.txt

# 4. Generate Env File
ENV_FILE="/etc/hermes.env"
if [ -f "$ENV_FILE" ]; then
  echo "🔒 [4/5] Found existing environment file at $ENV_FILE. Keeping configuration."
else
  echo "🔒 [4/5] Provisioning secure environment file..."
  echo "Please input your credentials below:"
  echo "----------------------------------------------------------"
  
  read -p "🔑 Enter SUPABASE_URL: " sb_url
  read -sp "🔑 Enter SUPABASE_KEY (service_role): " sb_key
  echo ""
  read -p "🔑 Enter TELEGRAM_BOT_TOKEN: " tg_token
  read -p "🔑 Enter TELEGRAM_ALLOWED_USERS (comma separated IDs): " tg_users
  
  echo "----------------------------------------------------------"
  echo "Enter your active NVIDIA NIM keys (leave blank to skip):"
  read -sp "🔑 NVIDIA_NIM_KEY_1: " nim_1; echo ""
  read -sp "🔑 NVIDIA_NIM_KEY_2: " nim_2; echo ""
  read -sp "🔑 NVIDIA_NIM_KEY_3: " nim_3; echo ""
  read -sp "🔑 NVIDIA_NIM_KEY_4: " nim_4; echo ""
  read -sp "🔑 NVIDIA_NIM_KEY_5: " nim_5; echo ""
  read -sp "🔑 NVIDIA_NIM_KEY_6: " nim_6; echo ""
  echo "----------------------------------------------------------"

  cat << EOF > "$ENV_FILE"
SUPABASE_URL=$sb_url
SUPABASE_KEY=$sb_key
TELEGRAM_BOT_TOKEN=$tg_token
TELEGRAM_ALLOWED_USERS=$tg_users
PORT=8080
PYTHONUNBUFFERED=1
EOF

  [ -n "$nim_1" ] && echo "NVIDIA_NIM_KEY_1=$nim_1" >> "$ENV_FILE"
  [ -n "$nim_2" ] && echo "NVIDIA_NIM_KEY_2=$nim_2" >> "$ENV_FILE"
  [ -n "$nim_3" ] && echo "NVIDIA_NIM_KEY_3=$nim_3" >> "$ENV_FILE"
  [ -n "$nim_4" ] && echo "NVIDIA_NIM_KEY_4=$nim_4" >> "$ENV_FILE"
  [ -n "$nim_5" ] && echo "NVIDIA_NIM_KEY_5=$nim_5" >> "$ENV_FILE"
  [ -n "$nim_6" ] && echo "NVIDIA_NIM_KEY_6=$nim_6" >> "$ENV_FILE"

  # Optional Email Config
  read -p "✉️ Configure AOL Mail capability now? (y/n): " configure_email
  if [[ "$configure_email" =~ ^[Yy]$ ]]; then
    read -p "   ↳ Enter EMAIL_ADDRESS: " email_addr
    read -sp "   ↳ Enter EMAIL_PASSWORD (App Password): " email_pass; echo ""
    read -p "   ↳ Enter EMAIL_IMAP_HOST (default: imap.aol.com): " email_imap; email_imap=${email_imap:-imap.aol.com}
    read -p "   ↳ Enter EMAIL_IMAP_PORT (default: 993): " imap_port; imap_port=${imap_port:-993}
    read -p "   ↳ Enter EMAIL_SMTP_HOST (default: smtp.aol.com): " email_smtp; email_smtp=${email_smtp:-smtp.aol.com}
    read -p "   ↳ Enter EMAIL_SMTP_PORT (default: 587): " smtp_port; smtp_port=${smtp_port:-587}

    cat << EOF >> "$ENV_FILE"
EMAIL_ADDRESS=$email_addr
EMAIL_PASSWORD=$email_pass
EMAIL_IMAP_HOST=$email_imap
EMAIL_IMAP_PORT=$imap_port
EMAIL_SMTP_HOST=$email_smtp
EMAIL_SMTP_PORT=$smtp_port
EOF
  fi

  chmod 600 "$ENV_FILE"
  echo "🛡️  Secure env variables written and restricted (chmod 600) at $ENV_FILE"
fi

# 5. Create Systemd Service
echo "⚙️ [5/5] Registering systemd background daemon..."
cat << EOF > /etc/systemd/system/hermes.service
[Unit]
Description=Hermes AI Agent Daemon
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$REPO_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$REPO_DIR/venv/bin/python app.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Reload and launch service
systemctl daemon-reload
systemctl enable hermes
systemctl restart hermes

echo "=========================================================="
echo " 🎉 Hermes setup completed successfully on GCE VM!"
echo "=========================================================="
echo " 👉 Monitor live logstream: sudo journalctl -u hermes -f"
echo " 👉 Check dashboard status: curl http://127.0.0.1:8080/health"
echo "=========================================================="
