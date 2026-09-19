import os


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


# Database file location. A docker volume is mounted on its directory.
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/webhooks.db")

# HTTP behaviour when delivering to subscriber callbacks.
DELIVERY_TIMEOUT_SECONDS = _int("DELIVERY_TIMEOUT_SECONDS", 10)
MAX_ATTEMPTS = _int("MAX_ATTEMPTS", 5)
# Delay before attempt N (N >= 2): BASE * 2 ** (N - 2) seconds.
RETRY_BASE_DELAY_SECONDS = float(os.getenv("RETRY_BASE_DELAY_SECONDS", 1))
RETRY_MAX_DELAY_SECONDS = float(os.getenv("RETRY_MAX_DELAY_SECONDS", 300))

# Dispatcher worker loop.
DISPATCH_INTERVAL_SECONDS = float(os.getenv("DISPATCH_INTERVAL_SECONDS", 0.2))
DISPATCH_CONCURRENCY = _int("DISPATCH_CONCURRENCY", 16)

# API server.
HOST = os.getenv("HOST", "0.0.0.0")
PORT = _int("PORT", 8080)
