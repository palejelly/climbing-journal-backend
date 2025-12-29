#!/bin/bash
# 1. Update and install ffmpeg on the Azure Linux instance
apt-get update && apt-get install -y ffmpeg

# 2. Start your application (standard for Azure Python apps)
# Adjust 'app:app' if your entry point file/variable is named differently
gunicorn --bind=0.0.0.0 --timeout 600 app:app