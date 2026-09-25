from dataclasses import dataclass
import os
from pathlib import Path


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv("APP_NAME", "VAULT")
    host: str = os.getenv("COORDINATOR_HOST", "0.0.0.0")
    port: int = env_int("COORDINATOR_PORT", 8000)
    node_host: str = os.getenv("NODE_HOST", "127.0.0.1")
    node_count: int = env_int("NODE_COUNT", 8)
    node_start_port: int = env_int("NODE_START_PORT", 8101)
    node_base_url: str = os.getenv("NODE_BASE_URL", "http://127.0.0.1")
    db_path: str = os.getenv("DB_PATH", "./data/vault.db")
    data_root: str = os.getenv("DATA_ROOT", "./data")
    chunk_size: int = env_int("CHUNK_SIZE_BYTES", 8 * 1024 * 1024)
    max_upload: int = env_int("MAX_UPLOAD_BYTES", 512 * 1024 * 1024)
    default_rf: int = env_int("DEFAULT_REPLICATION_FACTOR", 3)
    default_w: int = env_int("DEFAULT_WRITE_QUORUM", 2)
    default_r: int = env_int("DEFAULT_READ_QUORUM", 1)
    heartbeat_interval: int = env_int("HEARTBEAT_INTERVAL", 2)
    failure_timeout: int = env_int("FAILURE_TIMEOUT", 6)
    repair_interval: int = env_int("REPAIR_INTERVAL", 3)
    rebalance_interval: int = env_int("REBALANCE_INTERVAL", 20)
    integrity_interval: int = env_int("INTEGRITY_INTERVAL", 30)
    rebalance_threshold: float = env_float("REBALANCE_THRESHOLD", 0.15)
    http_timeout: float = env_float("HTTP_TIMEOUT", 5)
    cors_origins_raw: str = os.getenv("CORS_ORIGINS", "*")

    @property
    def cors_origins(self):
        return [x.strip() for x in self.cors_origins_raw.split(",") if x.strip()]

    def node_url(self, node_id: int) -> str:
        return f"{self.node_base_url}:{self.node_start_port + node_id - 1}"

    def ensure_dirs(self):
        Path(self.data_root).mkdir(parents=True, exist_ok=True)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)


settings = Settings()
