#!/bin/bash
set -e

echo ">>> Building frontend..."
cd frontend
npm install
npm run build
cd ..
echo ">>> Frontend build complete."

exec uvicorn app:app --host 0.0.0.0 --port 8000
