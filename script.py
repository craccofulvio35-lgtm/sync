import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import re
import os
import time
import sys
import glob
import csv
import base64
from datetime import datetime
import concurrent.futures
import random
import math

# =========================
# CONFIG & OTTIMIZZAZIONI GLOBALI
# =========================

TURUM_USER  = os.getenv("TURUM_USER")
TURUM_PASS  = os.getenv("TURUM_PASS")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")

# Aggiungi un controllo per assicurarti che le variabili siano state caricate
if not all([TURUM_USER, TURUM_PASS, SHOPIFY_TOKEN]):
    print("ERRORE: Le credenziali non sono state trovate nelle variabili d'ambiente. Uscita.")
    sys.exit(1)
    
SHOPIFY_STORE    = "8bz6nn-13.myshopify.com"
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-10")
SHOPIFY_API_URL  = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
SHOPIFY_REST_URL = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_API_VERSION}"

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

LOCATION_ID    = ""
PUBLICATION_ID = ""
MAX_NEW_PRODUCTS_PER_RUN = 1850
AUTO_CREATE_MISSING_VARIANTS = os.getenv("AUTO_CREATE_MISSING_VARIANTS", "true").strip().lower() in ("1", "true", "yes", "y", "on")

HANDLE_RE = re.compile(r'[^a-z0-9]+', re.IGNORECASE)

SHOP_SESSION = requests.Session()
RETRY_STRATEGY = Retry(total=1, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY, pool_connections=20, pool_maxsize=50)
SHOP_SESSION.mount("https://", ADAPTER)

# =========================
# DUAL LOGGING SYSTEM (TXT + CSV)
# =========================

START_TIME = time.perf_counter()
TS = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILENAME = f"sync_report_{TS}.txt"
CSV_FILENAME = f"sync_changes_{TS}.csv"

LOG_FILE_HANDLE = None
CSV_FILE_HANDLE = None
CSV_WRITER = None

def console_log(msg, end="\n"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", end=end)

def init_logs():
    global LOG_FILE_HANDLE, CSV_FILE_HANDLE, CSV_WRITER
    # TXT: Audit completo
    LOG_FILE_HANDLE = open(LOG_FILENAME, "w", encoding="utf-8")
    LOG_FILE_HANDLE.write("=== AUDIT COMPLETO SINCRONIZZAZIONE ===\n\n")
    LOG_FILE_HANDLE.flush()
    
    # CSV: Solo modifiche/nuovi (Excel ready)
    CSV_FILE_HANDLE = open(CSV_FILENAME, "w", newline="", encoding="utf-8-sig")
    CSV_WRITER = csv.writer(CSV_FILE_HANDLE)
    CSV_WRITER.writerow(["Evento", "Nome Prodotto", "SKU", "Stock_Turum", "Stock_Shopify", "Prezzo_Turum", "Prezzo_Shopify", "Prezzo_Finale", "Note"])
    CSV_FILE_HANDLE.flush()

def close_logs():
    global LOG_FILE_HANDLE, CSV_FILE_HANDLE
    if LOG_FILE_HANDLE and not LOG_FILE_HANDLE.closed: 
        LOG_FILE_HANDLE.flush()
        LOG_FILE_HANDLE.close()
    if CSV_FILE_HANDLE and not CSV_FILE_HANDLE.closed: 
        CSV_FILE_HANDLE.flush()
        CSV_FILE_HANDLE.close()

def log_txt(event, name, sku, t_stock="N/A", s_stock="N/A", s_changed="NO", 
            t_price="N/A", s_price="N/A", f_price="N/A", p_changed="NO", note=""):
    if not LOG_FILE_HANDLE or LOG_FILE_HANDLE.closed: return
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] [{event}] {name[:45]}... | SKU: {sku} | "
    if event not in ["SKIP", "ERROR", "GHOST"]:
        line += f"Turum Price: â‚¬{t_price} | Shopify Before: â‚¬{s_price} | Final Calc: â‚¬{f_price} | Price Updated: {p_changed} | "
        line += f"Turum Stock: {t_stock} | Shopify Before: {s_stock} | Stock Updated: {s_changed}"
    if note: line += f" | NOTE: {note}"
    
    LOG_FILE_HANDLE.write(line + "\n")
