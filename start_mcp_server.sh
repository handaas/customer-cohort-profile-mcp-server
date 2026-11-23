#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")"
if [ -x mcp_env/bin/python ]; then
  exec mcp_env/bin/python server/mcp_server.py "${1:-stdio}"
fi
exec python3 server/mcp_server.py "${1:-stdio}"
