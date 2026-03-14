#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}===> Rebuilding Frontend...${NC}"
cd frontend
npm run build
cd ..

echo -e "${BLUE}===> Refreshing Analytical Cache...${NC}"
cargo run --bin ma_store --quiet -- rebuild-cache

echo -e "${GREEN}===> Build complete. Starting server on port 3000...${NC}"
cargo run --bin ma_store -- serve --port 3000
