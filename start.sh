#!/bin/bash
# Write service account JSON from environment variable
if [ -n "$GOOGLE_SERVICE_ACCOUNT_JSON" ]; then
    echo "$GOOGLE_SERVICE_ACCOUNT_JSON" > /app/service_account.json
    echo "Service account JSON written to /app/service_account.json"
else
    echo "WARNING: GOOGLE_SERVICE_ACCOUNT_JSON not set"
fi

# Start the FastAPI server
exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
