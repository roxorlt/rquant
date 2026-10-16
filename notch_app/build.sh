#!/bin/bash
# Build RQuantNotch.app (macOS 14+). Run on the Mac from this folder.
set -euo pipefail
cd "$(dirname "$0")"
if ! xcode-select -p >/dev/null 2>&1 || ! command -v swiftc >/dev/null 2>&1; then
  echo "Xcode Command Line Tools missing. Run: xcode-select --install  (then re-run ./build.sh)"; exit 1
fi
sw_vers -productVersion | awk -F. '$1<14{print "macOS 14+ required"; exit 1}'

BIN=""
# Prefer SwiftPM when a full platform SDK is available; else swiftc + CLT SDK.
if swift build -c release --arch arm64 --arch x86_64 >/tmp/rquant-notch-spm.log 2>&1 \
   || swift build -c release >/tmp/rquant-notch-spm.log 2>&1; then
  BIN=$(swift build -c release --show-bin-path 2>/dev/null)/RQuantNotch
fi
if [ ! -x "${BIN:-}" ]; then
  BIN=$(find .build -path '*release/RQuantNotch' -type f 2>/dev/null | head -1 || true)
fi
if [ ! -x "${BIN:-}" ]; then
  echo "SwiftPM unavailable ($(tail -2 /tmp/rquant-notch-spm.log 2>/dev/null | tr '\n' ' ')); building with swiftc..."
  SDK="${SDKROOT:-$(xcrun --sdk macosx --show-sdk-path 2>/dev/null || true)}"
  [ -d "$SDK" ] || SDK="/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk"
  mkdir -p .bin
  # shellcheck disable=SC2086
  swiftc -sdk "$SDK" -target arm64-apple-macosx14.0 -O -parse-as-library \
    Sources/RQuantNotch/*.swift -o .bin/RQuantNotch
  BIN=.bin/RQuantNotch
fi
[ -x "$BIN" ] || { echo "Build failed: no RQuantNotch binary"; exit 1; }

APP=build/RQuantNotch.app
rm -rf "$APP"; mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp "$BIN" "$APP/Contents/MacOS/RQuantNotch"
cat > "$APP/Contents/Info.plist" <<'PL'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>com.rquant.notch</string>
<key>CFBundleName</key><string>RQuantNotch</string>
<key>CFBundleExecutable</key><string>RQuantNotch</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleShortVersionString</key><string>1.0.1</string>
<key>CFBundleVersion</key><string>2</string>
<key>LSMinimumSystemVersion</key><string>14.0</string>
<key>LSUIElement</key><true/>
<key>NSAppTransportSecurity</key><dict>
  <key>NSExceptionDomains</key><dict><key>82.156.0.68</key><dict>
    <key>NSExceptionAllowsInsecureHTTPLoads</key><true/></dict></dict>
  <key>NSAllowsArbitraryLoads</key><true/></dict>
</dict></plist>
PL
codesign --force --deep -s - "$APP"
echo "Built $APP ($(du -h "$APP/Contents/MacOS/RQuantNotch" | awk '{print $1}'))"
