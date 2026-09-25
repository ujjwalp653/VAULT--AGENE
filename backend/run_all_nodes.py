import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
count = int(os.getenv("NODE_COUNT", "8"))
start = int(os.getenv("NODE_START_PORT", "8101"))
storage = os.getenv("DATA_ROOT", str(ROOT / "data"))

procs = []
try:
    for node_id in range(1, count + 1):
        p = subprocess.Popen([
            sys.executable, "-m", "vault.node_agent",
            "--id", str(node_id),
            "--port", str(start + node_id - 1),
            "--host", "0.0.0.0",
            "--storage", storage,
        ], cwd=str(ROOT))
        procs.append(p)
    print(f"Started {len(procs)} VAULT node agents. Press Ctrl+C to stop.")
    for p in procs:
        p.wait()
except KeyboardInterrupt:
    pass
finally:
    for p in procs:
        p.terminate()
