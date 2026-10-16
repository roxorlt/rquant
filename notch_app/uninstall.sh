#!/bin/bash
pkill -x RQuantNotch 2>/dev/null || true
rm -rf /Applications/RQuantNotch.app
security delete-generic-password -s rquant-wuhu -a token >/dev/null 2>&1 || true
echo "Removed (also remove from Login Items if listed)."
