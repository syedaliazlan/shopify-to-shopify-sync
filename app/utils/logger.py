# app/utils/logger.py
import os, sys, time

LEVELS = {"ERROR": 40, "WARN": 30, "INFO": 20, "DEBUG": 10, "NONE": 100}
LOG_LEVEL = LEVELS.get(os.getenv("LOG_LEVEL", "INFO").upper(), 20)

def _ts():
    return time.strftime("%H:%M:%S")

def log(level: str, msg: str):
    if LEVELS[level] >= LOG_LEVEL:
        print(f"[{_ts()}][{level}] {msg}", file=sys.stdout if LEVELS[level] < 40 else sys.stderr)

def debug(msg): log("DEBUG", msg)
def info(msg):  log("INFO", msg)
def warn(msg):  log("WARN", msg)
def error(msg): log("ERROR", msg)
