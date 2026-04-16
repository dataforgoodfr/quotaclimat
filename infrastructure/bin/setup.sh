#!/bin/bash

set -e

echo "🔧 Starting setup..."

if ! command -v node &> /dev/null
then
    error "Please install node >=18 before running this script."
    exit 1
fi
# Make sure node version is >18
NODE_VERSION=$(node -v)
if [ "$(echo $NODE_VERSION | cut -c 2-3)" -lt 18 ]; then
    error "Please install node version 18 or higher before running this script."
    exit 1
fi

# Technically bitwarden-cli is available on brew,
# but it still depends on node, so we will install it using npm.
if ! command -v bw &> /dev/null
then
    info "Installing bw cli"
    npm install -g @bitwarden/cli
fi

# Check for Homebrew
if ! command -v brew >/dev/null 2>&1; then
  echo "❌ Homebrew not found. Please install Homebrew first: https://brew.sh/"
  exit 1
fi

# Install jq if not installed
if ! command -v jq >/dev/null 2>&1; then
  echo "📦 Installing jq..."
  brew install jq
else
  echo "✅ jq is already installed"
fi

# Install gum if not installed
if ! command -v gum >/dev/null 2>&1; then
  echo "📦 Installing gum..."
  brew install gum
else
  echo "✅ gum is already installed"
fi

# Install mise if not installed
if ! command -v mise >/dev/null 2>&1; then
  echo "📦 Installing mise..."
  brew install mise
else
  echo "✅ mise is already installed"
fi

# Install all tools pinned in mise.toml (opentofu, terragrunt)
echo "📦 Installing tools from mise.toml..."
mise install

echo ""
echo "⚠️  Make sure mise is activated in your shell profile, otherwise tools won't be on your PATH:"
echo "    echo 'eval \"\$(mise activate zsh)\"' >> ~/.zshrc   # for zsh"
echo "    echo 'eval \"\$(mise activate bash)\"' >> ~/.bashrc  # for bash"

echo "✅ Setup complete!"
