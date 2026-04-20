# VPS Deployment Guide

## 1. Prepare server
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip nginx
mkdir -p /opt/trading-bot
cd /opt/trading-bot
```

Copy the project files into `/opt/trading-bot`.

## 2. Python environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 3. Configure environment
```bash
cp .env.example .env
nano .env
```

## 4. Test locally on the server
```bash
source venv/bin/activate
python main.py
```

Open `http://SERVER_IP:5000/`.

## 5. Run as a service
Create `/etc/systemd/system/trading-bot.service`:

```ini
[Unit]
Description=Trading Bot Dashboard
After=network.target

[Service]
User=root
WorkingDirectory=/opt/trading-bot
Environment=PATH=/opt/trading-bot/venv/bin
ExecStart=/opt/trading-bot/venv/bin/python /opt/trading-bot/main.py
Restart=always

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl daemon-reload
sudo systemctl enable trading-bot
sudo systemctl start trading-bot
sudo systemctl status trading-bot
```

## 6. Nginx reverse proxy
Create `/etc/nginx/sites-available/trading-bot`:

```nginx
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Enable it:
```bash
sudo ln -s /etc/nginx/sites-available/trading-bot /etc/nginx/sites-enabled/trading-bot
sudo nginx -t
sudo systemctl restart nginx
```

Now open `http://SERVER_IP/`.
