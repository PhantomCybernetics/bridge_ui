# Phantom Bridge UI

This package contains web UI for the Phantom Bridge and a web server to serve it.

The web server itself does not connect to anything, it merely servers semi-static HTML and JavaScript to users. The UI in the web browser establishes connection to Bridge Server via Socket.io, and to the Bridge Client node on a robot via WebRTC P2P connection.

You can fork this repository and host it yourself to customize the default UI provided. The configuration file specifies which Bridge Server shall the client connect to.

![Infrastructure map](https://raw.githubusercontent.com/PhantomCybernetics/phntm_bridge_docs/refs/heads/main/img/Architecture_UI_Server.png)

# Install Bridge UI

### Install Bun

Follow [instructions from bun.sh](https://bun.sh/docs/installation)

Last tested 1.2.18

### Clone this repo and install dependencies

```bash
cd ~
git clone git@github.com:PhantomCybernetics/phntm_bridge_ui.git phntm_bridge_ui
cd phntm_bridge_ui
bun install
```

### Create config file

Create new config file e.g. `~/phntm_bridge_ui/config.jsonc`, use [./config.example.jsonc](./config.example.jsonc) as a starting point.

### Add system service to your systemd

```bash
sudo vim /etc/systemd/system/phntm_bridge_ui.service
```

...and paste:

```
[Unit]
Description=phntm bridge_ui service
After=network.target

[Service]
ExecStart=/home/ubuntu/phntm_bridge_ui/run.sh
Restart=always
User=root
Environment=NODE_ENV=production
WorkingDirectory=/home/ubuntu/phntm_bridge_ui/
StandardOutput=append:/var/log/phntm_bridge_ui.log
StandardError=append:/var/log/phntm_bridge_ui.err.log

[Install]
WantedBy=multi-user.target
```

Reload systemctl daemon

```bash
sudo systemctl daemon-reload
```

### Launch:

```bash
sudo systemctl start phntm_bridge_ui.service
sudo systemctl enable phntm_bridge_ui.service # will launch on boot
```
