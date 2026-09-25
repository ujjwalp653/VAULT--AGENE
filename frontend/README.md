# VAULT Frontend

The dashboard is vanilla HTML/CSS/JavaScript and can be served directly by the FastAPI coordinator.

## Recommended local run

Run `start_local.bat` from the project root and open `http://127.0.0.1:8000/`.

## Vite development

If you run the frontend separately on `http://127.0.0.1:5173`, `config.js` automatically points API/WebSocket traffic to the FastAPI backend on port `8000`.

This avoids the common problem where the UI loads but all buttons appear to do nothing because requests were accidentally sent to Vite instead of FastAPI.
