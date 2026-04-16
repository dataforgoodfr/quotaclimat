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

# Ensure mise is activated in the shell RC file so tools are on the PATH.
if [ -n "$ZSH_VERSION" ] || [ "$SHELL" = "/bin/zsh" ] || [ "$SHELL" = "/usr/bin/zsh" ]; then
  RC_FILE="$HOME/.zshrc"
  MISE_ACTIVATE_LINE='eval "$(mise activate zsh)"'
else
  RC_FILE="$HOME/.bashrc"
  MISE_ACTIVATE_LINE='eval "$(mise activate bash)"'
fi

if ! grep -qF "mise activate" "$RC_FILE" 2>/dev/null; then
  echo "Adding mise activation to $RC_FILE..."
  echo "" >> "$RC_FILE"
  echo "$MISE_ACTIVATE_LINE" >> "$RC_FILE"
  # shellcheck disable=SC1090
  source "$RC_FILE"
  echo "mise activated in $RC_FILE"
else
  echo "mise already activated in $RC_FILE"
fi

# Install all tools pinned in mise.toml (opentofu, terragrunt)
echo "Installing tools from mise.toml..."
mise install

echo "✅ Setup complete!"
