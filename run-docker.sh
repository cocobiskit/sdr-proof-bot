#!/usr/bin/env bash
set -euo pipefail

# This command is critical: it ensures the script runs from its own location.
cd "$(dirname "$0")"

IMAGE_NAME="proof-bot"

# Parse mode: main (default) or colossus (diagnostic)
MODE="${1:-main}"  # e.g., ./run-docker.sh colossus
if [[ "$MODE" != "main" && "$MODE" != "colossus" ]]; then
  echo "Usage: ./run-docker.sh [main|colossus]"
  exit 1
fi

# --- THE NUKE OPTION (Only for main; skip for colossus to preserve state) ---
if [[ "$MODE" == "main" ]]; then
  echo "🔥 Nuking all Docker build caches, images, and containers to ensure a clean slate..."
  docker system prune -a -f
fi

# Ensure host directories for data persistence exist
mkdir -p screenshots exports logs  # NEW: logs for colossus

echo "🚀 Building Docker image '$IMAGE_NAME' from scratch..."
docker build -t "$IMAGE_NAME" .

echo "--- Running the PROOF BOT ($MODE mode) inside the container ---"

# ADDED FIX: Forcefully remove any old container with the same name.
docker rm -f "${IMAGE_NAME}-run" || true

# Run command based on mode
if [[ "$MODE" == "colossus" ]];
then
  echo " Running SIC Selector Colossus diagnostic..."
  CMD_ARGS=("python" "sic_selector_colossus.py")  # Outputs to logs/sic_colossus.log + report.md
else
  echo " Running main bot..."
  CMD_ARGS=("python" "-m" "proof_bot.main")
fi

# We mount directories to get the output (screenshots, CSVs, logs) back on our host machine
docker run --rm -it \
  -v "$(pwd)/screenshots:/app/screenshots" \
  -v "$(pwd)/exports:/app/exports" \
  -v "$(pwd)/logs:/app/logs" \
  --shm-size=2g \
  --name "${IMAGE_NAME}-run" \
  "$IMAGE_NAME" \
  "${CMD_ARGS[@]}"

if [[ "$MODE" == "colossus" ]]; then
  echo "✅ Colossus complete! Check logs/sic_colossus.log and sic_colossus_report.md for findings."
  echo "   Updated selectors.json with top SIC selectors. Re-run main bot to test."
else
  echo "✅ Bot run has completed successfully."
fi