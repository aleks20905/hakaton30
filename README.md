# Production Planning Dashboard

Flask + pandas dashboard for production planning.

## Nix

### Development shell

```bash
nix develop
python app.py
```

### NixOS module

Enable in your NixOS config:

```nix
#import the flake
curtisDashboard.url = "github:aleks20905/hakaton30/main";


imports = [ inputs.curtisDashboard.nixosModules.default ];

services.curtisDashboard = {
    enable = true;
    port = 8080;
    openFirewall = true;
    secretKeyFile = "/etc/curtisDashboard/secrets";
};
```

sudo mkdir -p /etc/curtisDashboard
sudo bash -c "echo SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))') > /etc/curtisDashboard/secrets"
sudo chmod 600 /etc/curtisDashboard/secrets

## Python venv (alternative)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python app.py
```

Debug mode:

```bash
FLASK_DEBUG=true python app.py
```

## Tests

```bash
python -m unittest discover
```
