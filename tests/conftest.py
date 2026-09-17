"""Ensure tests never touch production Mongo/Redis/broker.

Set env vars to unreachable local endpoints BEFORE any repository module import,
so the connection attempts fail fast instead of reaching production hosts.
"""
import os

os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:1")
os.environ.setdefault("REDIS_HOST", "127.0.0.1")
os.environ.setdefault("REDIS_PORT", "6390")
os.environ.setdefault("MINTZY_SECRET_KEY", "test-secret-key")
os.environ.setdefault("SECRET", "test-plugin-secret")
os.environ.setdefault("ENV", "test")
