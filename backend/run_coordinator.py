import uvicorn
from vault.config import settings
from vault.main import app

if __name__ == "__main__":
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="info")
