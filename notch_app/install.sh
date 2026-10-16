#!/bin/bash
# Install RQuantNotch to /Applications, store token in Keychain, launch (registers login item).
# Usage: ./install.sh [--replay]           (prompts for token if not already in Keychain)
set -euo pipefail
cd "$(dirname "$0")"
[ -d build/RQuantNotch.app ] || ./build.sh
pkill -x RQuantNotch 2>/dev/null || true
rm -rf /Applications/RQuantNotch.app
cp -R build/RQuantNotch.app /Applications/
if ! security find-generic-password -s rquant-wuhu -a token >/dev/null 2>&1; then
  if [ -f ./wuhu_token ]; then TOK=$(tr -d " \n" < ./wuhu_token); else read -rsp "rQuant 五虎 token: " TOK; echo; fi
  security add-generic-password -U -s rquant-wuhu -a token -T /Applications/RQuantNotch.app -w "$TOK"
  unset TOK; rm -f ./wuhu_token
fi
if [ "${1:-}" = "--replay" ]; then defaults write com.rquant.notch replay -bool YES; echo "Replay/demo mode ON (turn off: ./install.sh again without --replay)";
else defaults delete com.rquant.notch replay 2>/dev/null || true; fi
open /Applications/RQuantNotch.app
echo "Installed. It registers itself as a login item (System Settings > General > Login Items)."
