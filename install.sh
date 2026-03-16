#!/usr/bin/env bash

set -e

REPO="phannaly/etcd-ui"
BINARY="etcd-ui"

echo "Installing $BINARY..."

OS=$(uname -s | tr '[:upper:]' '[:lower:]')

case "$OS" in
linux) OS="linux" ;;
darwin) OS="darwin" ;;
*)
  echo "Unsupported OS: $OS"
  exit 1
  ;;
esac

ARCH=$(uname -m)

case "$ARCH" in
x86_64) ARCH="amd64" ;;
aarch64) ARCH="arm64" ;;
arm64) ARCH="arm64" ;;
*)
  echo "Unsupported architecture: $ARCH"
  exit 1
  ;;
esac

echo "Detected platform: $OS/$ARCH"

VERSION=$(curl -s https://api.github.com/repos/$REPO/releases/latest \
  | sed -n 's/.*"tag_name": "\(.*\)".*/\1/p')

VERSION=${VERSION#v}
if [ -z "$VERSION" ]; then
  echo "Failed to detect latest version."
  echo "Make sure a GitHub release exists."
  exit 1
fi

echo "Latest version: $VERSION"

FILE="${BINARY}_${VERSION}_${OS}_${ARCH}.tar.gz"
URL="https://github.com/$REPO/releases/download/v${VERSION}/${FILE}"

TMP_DIR=$(mktemp -d)

echo "Downloading $URL"
curl -L "$URL" -o "$TMP_DIR/$FILE"

echo "Extracting..."
tar -xzf "$TMP_DIR/$FILE" -C "$TMP_DIR"

echo "Installing to /usr/local/bin..."

sudo mv "$TMP_DIR/$BINARY" /usr/local/bin/$BINARY
sudo chmod +x /usr/local/bin/$BINARY

echo ""
echo "Installation complete!"
echo ""
echo "Run:"
echo "  $BINARY"
