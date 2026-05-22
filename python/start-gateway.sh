# 1. Cleanup old broken container
docker rm -f openshell-gateway 2>/dev/null || true

# 2. Create state directory (if it doesn't exist)
mkdir -p ~/openshell-state

# 3. Get the host docker group ID (important!)
DOCKER_GID=$(getent group docker | cut -d: -f3)

echo "Using Docker GID: $DOCKER_GID"

# 4. Start gateway with ALL the necessary fixes
docker run -d \
  --name openshell-gateway \
  --restart unless-stopped \
  -p 127.0.0.1:18080:8080 \
  -v ~/openshell-state:/var/openshell:z \
  -v /var/run/docker.sock:/var/run/docker.sock:z \
  -e OPENSHELL_DRIVERS=docker \
  -e OPENSHELL_DB_URL=sqlite:/var/openshell/openshell.db \
  -e OPENSHELL_DISABLE_TLS=true \
  --group-add $DOCKER_GID \
  --user root \
  ghcr.io/nvidia/openshell/gateway:latest

echo "✅ Gateway started on port 18080"
sleep 8
docker logs openshell-gateway --tail 30