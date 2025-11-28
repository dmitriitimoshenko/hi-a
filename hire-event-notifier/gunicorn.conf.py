import os

bind = f"0.0.0.0:{os.getenv('PORT', '8086')}"

workers = int(os.getenv("WORKERS", "1"))

threads = int(os.getenv("THREADS", "1"))

worker_class = "uvicorn.workers.UvicornWorker"

timeout = int(os.getenv("TIMEOUT", "60"))
graceful_timeout = int(os.getenv("GRACEFUL_TIMEOUT", "30"))
keepalive = int(os.getenv("KEEPALIVE", "30"))

accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info")

preload_app = False
