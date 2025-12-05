#!/usr/bin/env bash
set -e

echo "==> Removing old Docker sources (if any)..."
sudo rm -f /etc/apt/sources.list.d/docker.list
sudo rm -f /etc/apt/keyrings/docker.gpg

echo "==> Updating package index..."
sudo apt update

echo "==> Installing prerequisites..."
sudo apt install -y ca-certificates curl gnupg software-properties-common

echo "==> Setting up Docker GPG keyring..."
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo "==> Adding Docker repository for Ubuntu 24.04 (noble)..."
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu noble stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "==> Updating package index (after adding Docker repo)..."
sudo apt update

echo "==> Installing Docker Engine, CLI, containerd, and Compose plugin..."
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo "==> Verifying installations..."
docker --version
docker compose version

echo "==> Docker installation complete."

# Optionally: add current user to docker group (uncomment if desired)
# echo "==> Adding $USER to docker group..."
# sudo usermod -aG docker $USER
# echo "==> You need to log out and back in or run 'newgrp docker' for this to take effect."

echo "==> Done."