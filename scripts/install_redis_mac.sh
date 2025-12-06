#!/bin/bash
# Install Redis Stack (includes RediSearch, RedisJSON, etc.)

# Stop regular redis if running
brew services stop redis 2>/dev/null

# Tap the Redis Stack cask and install
brew tap redis-stack/redis-stack
brew install redis-stack

# Start Redis Stack
brew services start redis-stack
