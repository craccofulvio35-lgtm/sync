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

def log_info(note):
    if not LOG_FILE_HANDLE or LOG_FILE_HANDLE.closed:
        return
    timestamp = datetime.now().strftime("%H:%M:%S")
    LOG_FILE_HANDLE.write(f"[{timestamp}] [INFO] {note}\n")
    LOG_FILE_HANDLE.flush()

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
    LOG_FILE_HANDLE.flush()  # ðŸ”’ Flush immediato

def log_csv(event, name, sku, t_stock, s_stock, t_price, s_price, f_price, note):
    if not CSV_WRITER: return
    CSV_WRITER.writerow([event, name[:50], sku, t_stock, s_stock, t_price, s_price, f_price, note])
    CSV_FILE_HANDLE.flush()  # ðŸ”’ Flush immediato

def cleanup_old_logs(days=7):
    now = time.time()
    deleted = 0
    for f in glob.glob("sync_*_*.txt") + glob.glob("sync_*_*.csv"):
        if os.stat(f).st_mtime < now - (days * 86400): 
            os.remove(f)
            deleted += 1
    if deleted > 0: console_log(f"ðŸ§¹ Pulizia automatica: Eliminati {deleted} vecchi report.")

# =========================
# API SHOPIFY (ANTI-CRASH + BACKOFF ADATTIVO)
# =========================

def handle_rate_limit(attempt):
    base = 2 ** attempt
    jitter = random.uniform(0, math.sqrt(base))
    sleep_time = min(base + jitter, 8)
    console_log(f"â¸ï¸ Rate limit/Throttle: attesa {sleep_time:.1f}s")
    time.sleep(sleep_time)

def shopify_post(payload, retries=5):
    for attempt in range(retries):
        try:
            r = SHOP_SESSION.post(SHOPIFY_API_URL, headers=HEADERS, json=payload, timeout=30)
            if r.status_code == 429: 
                handle_rate_limit(attempt); continue
            data = r.json()
            if not isinstance(data, dict):
                if attempt == retries - 1:
                    return {}
                time.sleep(1)
                continue
            if "errors" in data:
                if any(e.get("extensions", {}).get("code") == "THROTTLED" for e in data["errors"]):
                    handle_rate_limit(attempt); continue
            available = data.get("extensions", {}).get("cost", {}).get("throttleStatus", {}).get("currentlyAvailable", 1000)
            if available < 100: time.sleep(1.5)
            return data
        except Exception:
            if attempt == retries - 1: return {}
            time.sleep(2)
    return {}

def shopify_rest(method, endpoint, payload=None, retries=3):
    url = f"{SHOPIFY_REST_URL}/{endpoint}"
    for attempt in range(retries):
        try:
            if method == "POST": r = SHOP_SESSION.post(url, headers=HEADERS, json=payload, timeout=30)
            elif method == "GET": r = SHOP_SESSION.get(url, headers=HEADERS, timeout=30)
            else: return None
            if r.status_code == 429: handle_rate_limit(attempt); continue
            return r
        except Exception: time.sleep(2)
    return None

# =========================
# FETCH DA TURUM & SHOPIFY HELPERS
# =========================

def get_turum_data():
    try:
        console_log("Login su Turum in corso...")
        r_login = SHOP_SESSION.post("https://api.b2b.turum.pl/v1/account/login", 
                                  json={"username": TURUM_USER, "password": TURUM_PASS}, timeout=15)
        r_login.raise_for_status()
        token = r_login.json().get("access_token")
        console_log("Scaricamento catalogo Turum...")
        r_data = SHOP_SESSION.get("https://api.b2b.turum.pl/v1/products_full_list_new", 
                                headers={"Authorization": f"Bearer {token}"}, timeout=60)
        r_data.raise_for_status()
        products = r_data.json().get("data", [])
        if not products: console_log("ERRORE CRITICO: 0 prodotti da Turum."); sys.exit(1)
        return products
    except Exception as e: console_log(f"ERRORE DI RETE TURUM: {e}"); sys.exit(1)

def get_shopify_location():
    data = shopify_post({'query': '{ locations(first: 5, query: "active:true") { edges { node { id } } } }'})
    edges = data.get("data", {}).get("locations", {}).get("edges", [])
    return edges[0]["node"]["id"] if edges else None

def get_online_store_publication_id():
    edges = shopify_post({'query': '{ publications(first: 20) { edges { node { id name } } } }'}).get("data", {}).get("publications", {}).get("edges", [])
    for e in edges:
        node = e.get("node", {})
        node_name = (node.get("name") or "").lower()
        if any(w in node_name for w in ["online", "negozio", "web"]):
            return node.get("id")
    return (edges[0].get("node", {}) or {}).get("id") if edges else None

def publish_to_online_store(product_id):
    if PUBLICATION_ID: 
        shopify_post({
            "query": "mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) { publishablePublish(id: $id, input: $input) { userErrors { message } } }", 
            "variables": {
                "id": product_id, 
                "input": [{"publicationId": PUBLICATION_ID}]
            }
        })

def update_product_status(product_id, status):
    # âœ… FIX: Formattazione corretta delle parentesi graffe
    shopify_post({
        "query": "mutation productUpdate($input: ProductInput!) { productUpdate(input: $input) { userErrors { message } } }", 
        "variables": {
            "input": {
                "id": product_id, 
                "status": status
            }
        }
    })

_collection_cache = {}
_handle_product_cache = {}
_product_variants_cache = {}
_missing_create_failure_reasons = {}
def preload_collections_cache():
    try:
        r = shopify_rest("GET", "custom_collections.json?limit=250&fields=id,title")
        if r and r.status_code == 200:
            for c in r.json().get("custom_collections", []): _collection_cache[c["title"]] = c["id"]
    except Exception: pass

def get_or_create_collection(title):
    if title in _collection_cache: return _collection_cache[title]
    r = shopify_rest("GET", f"custom_collections.json?title={requests.utils.quote(title)}&limit=1")
    if r and r.status_code == 200 and r.json().get("custom_collections"):
        cid = (r.json()["custom_collections"][0] or {}).get("id")
        if cid:
            _collection_cache[title] = cid
            return cid
    r = shopify_rest("POST", "custom_collections.json", {"custom_collection": {"title": title, "published": True}})
    if r and r.status_code == 201:
        cid = (r.json().get("custom_collection", {}) or {}).get("id")
        if cid:
            _collection_cache[title] = cid
            return cid
    return None

def add_product_to_collection(product_numeric_id, collection_id):
    if collection_id: shopify_rest("POST", "collects.json", {"collect": {"product_id": product_numeric_id, "collection_id": collection_id}})

def add_image_worker(product_id, image_url, alt):
    if not image_url or "not_found" in image_url: return
    numeric_id = product_id.split("/")[-1]
    for _ in range(3):
        try:
            img_resp = SHOP_SESSION.get(image_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            if img_resp.status_code == 200:
                encoded = base64.b64encode(img_resp.content).decode("utf-8")
                shopify_rest("POST", f"products/{numeric_id}/images.json", {"image": {"attachment": encoded, "alt": alt}})
                return
        except Exception:
            time.sleep(2)

def parse_shopify_user_errors(data, root_key):
    user_errors = data.get("data", {}).get(root_key, {}).get("userErrors", [])
    notes = []
    for err in user_errors:
        field = ".".join(map(str, err.get("field") or []))
        message = err.get("message", "Errore sconosciuto")
        notes.append(f"{field}: {message}" if field else message)
    return notes

def add_failure_reasons(notes):
    for note in notes:
        key = (note or "Errore sconosciuto").strip()
        _missing_create_failure_reasons[key] = _missing_create_failure_reasons.get(key, 0) + 1

def format_errors_for_log(data, root_key, default_note):
    top_errors = data.get("errors", [])
    notes = []
    for err in top_errors[:3]:
        if isinstance(err, dict):
            msg = err.get("message")
            if msg:
                notes.append(str(msg))
        elif err:
            notes.append(str(err))
    notes.extend([n for n in parse_shopify_user_errors(data, root_key)[:3] if n])
    if notes:
        return "; ".join(notes)
    raw = json.dumps(data, ensure_ascii=False)[:400]
    return f"{default_note} | Raw: {raw}" if raw else default_note

def get_variant_size(variant):
    return str(variant.get("eu_size", "") or variant.get("size", "")).strip()

def build_variant_sku(base_sku, variant):
    size_val = get_variant_size(variant)
    return f"{base_sku}-{size_val}" if size_val else base_sku

def calc_final_price(raw_price):
    return round(float(raw_price or 0) * 1.22 * 1.10, 2)

def build_variant_payload(base_sku, variant, option_name):
    size_val = get_variant_size(variant)
    return {
        "price": str(calc_final_price(variant.get("price", 0))),
        "sku": build_variant_sku(base_sku, variant),
        "optionValues": [{"optionName": option_name, "name": size_val or "N/A"}],
        "inventoryQuantities": [{"name": "available", "quantity": int(variant.get("stock", 0)), "locationId": LOCATION_ID}]
    }

def build_product_handle(name, base_sku):
    handle_slug = HANDLE_RE.sub('-', name.lower()).strip('-')
    sku_slug = HANDLE_RE.sub('-', base_sku.lower()).strip('-')
    return f"{handle_slug}-{sku_slug}"

def get_product_id_by_handle(handle):
    if handle in _handle_product_cache:
        return _handle_product_cache[handle]
    query = """
    query($handle: String!) {
      productByHandle(handle: $handle) { id handle }
    }
    """
    data = shopify_post({"query": query, "variables": {"handle": handle}})
    node = data.get("data", {}).get("productByHandle", {}) or {}
    pid = node.get("id") if node.get("handle") == handle else None
    _handle_product_cache[handle] = pid
    return pid

def get_product_variant_skus(product_id):
    if product_id in _product_variants_cache:
        return set(_product_variants_cache[product_id])
    query = """
    query($id: ID!) {
      product(id: $id) {
        variants(first: 250) {
          edges { node { sku } }
        }
      }
    }
    """
    data = shopify_post({"query": query, "variables": {"id": product_id}})
    edges = data.get("data", {}).get("product", {}).get("variants", {}).get("edges", [])
    skus = set()
    for edge in edges:
        sku = ((edge or {}).get("node", {}) or {}).get("sku")
        if sku:
            skus.add(sku)
    _product_variants_cache[product_id] = set(skus)
    return skus

# =========================
# AGGIORNAMENTI IN BLOCCO E CREAZIONE
# =========================

def bulk_inventory_update(updates_list):
    if not updates_list: return
    for i in range(0, len(updates_list), 100):
        chunk = updates_list[i:i + 100]
        res = shopify_post({
            "query": "mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) { inventorySetOnHandQuantities(input: $input) { userErrors { message } } }", 
            "variables": {
                "input": {
                    "reason": "correction", 
                    "setQuantities": chunk
                }
            }
        })
        errors = parse_shopify_user_errors(res, "inventorySetOnHandQuantities")
        if errors:
            console_log(f"  -> ERROR stock chunk {i+1}-{i+len(chunk)}: {'; '.join(errors[:3])}")
        console_log(f"  -> Inviato pacchetto stock {i+len(chunk)}/{len(updates_list)}...")

def bulk_price_update(product_id, variants_prices):
    res = shopify_post({
        "query": "mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) { productVariantsBulkUpdate(productId: $productId, variants: $variants) { userErrors { message } } }", 
        "variables": {
            "productId": product_id, 
            "variants": variants_prices
        }
    })
    errors = parse_shopify_user_errors(res, "productVariantsBulkUpdate")
    if errors:
        console_log(f"  -> ERROR prezzo su prodotto {product_id.split('/')[-1]}: {'; '.join(errors[:3])}")

def create_missing_variants(product_id, base_sku, name, missing_variants, option_name):
    if not missing_variants:
        return set()
    payload = [build_variant_payload(base_sku, v, option_name) for v in missing_variants]
    res = shopify_post({
        "query": "mutation productVariantsBulkCreate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) { productVariantsBulkCreate(productId: $productId, variants: $variants) { productVariants { id sku } userErrors { field message } } }",
        "variables": {"productId": product_id, "variants": payload}
    })
    errors = parse_shopify_user_errors(res, "productVariantsBulkCreate")
    if errors:
        add_failure_reasons(errors)
        # Fallback robusto: se il bulk fallisce, prova variante-per-variante.
        created_skus = set()
        for mv in missing_variants:
            single_payload = [build_variant_payload(base_sku, mv, option_name)]
            single_res = shopify_post({
                "query": "mutation productVariantsBulkCreate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) { productVariantsBulkCreate(productId: $productId, variants: $variants) { productVariants { id sku } userErrors { field message } } }",
                "variables": {"productId": product_id, "variants": single_payload}
            })
            single_errors = parse_shopify_user_errors(single_res, "productVariantsBulkCreate")
            if single_errors:
                add_failure_reasons(single_errors)
                log_txt("ERROR", name, build_variant_sku(base_sku, mv), note=f"Creazione variante fallita: {'; '.join(single_errors[:2])}")
                continue
            single_created = single_res.get("data", {}).get("productVariantsBulkCreate", {}).get("productVariants", []) or []
            for sc in single_created:
                sku = sc.get("sku")
                if sku:
                    created_skus.add(sku)
        if created_skus:
            cached = _product_variants_cache.get(product_id, set())
            _product_variants_cache[product_id] = set(cached).union(created_skus)
        else:
            log_txt("ERROR", name, base_sku, note=f"Creazione varianti mancanti fallita (bulk+single): {'; '.join(errors[:3])}")
        return created_skus
    created = res.get("data", {}).get("productVariantsBulkCreate", {}).get("productVariants", []) or []
    created_skus = {v.get("sku") for v in created if v.get("sku")}
    if created_skus:
        cached = _product_variants_cache.get(product_id, set())
        _product_variants_cache[product_id] = set(cached).union(created_skus)
    return created_skus

def get_shopify_inventory():
    console_log("Download inventario Shopify globale in corso...")
    inv, status_map, cursor, has_next = {}, {}, None, True
    while has_next:
        q = f'query($cursor: String) {{ productVariants(first: 250, after: $cursor) {{ pageInfo {{ hasNextPage endCursor }} edges {{ node {{ id sku price product {{ id status tags }} inventoryItem {{ id inventoryLevel(locationId: "{LOCATION_ID}") {{ quantities(names: ["available"]) {{ quantity }} }} }} }} }} }} }}'
        vdata = shopify_post({"query": q, "variables": {"cursor": cursor}}).get("data", {}).get("productVariants", {})
        for e in vdata.get("edges", []):
            n = e.get("node", {}) or {}
            sku = n.get("sku")
            if not sku: continue
            product = n.get("product", {}) or {}
            p_id = product.get("id")
            if not p_id:
                continue
            status_map[p_id] = product.get("status", "DRAFT")
            qty = 0
            inventory_item = n.get("inventoryItem", {}) or {}
            inv_level = inventory_item.get("inventoryLevel")
            if inv_level:
                quantities = inv_level.get("quantities", []) or []
                if quantities:
                    qty = quantities[0].get("quantity", 0) or 0
            inv[sku] = {
                "variant_id": n.get("id"),
                "product_id": p_id,
                "inv_id": inventory_item.get("id"),
                "qty": qty,
                "price": float(n.get("price", 0) or 0),
                "is_turum": "Turum" in (product.get("tags") or [])
            }
        has_next, cursor = vdata.get("pageInfo", {}).get("hasNextPage", False), vdata.get("pageInfo", {}).get("endCursor")
    return inv, status_map

def create_product(name, item, variants):
    p_type, o_name = ("Scarpe", "Taglia EU") if any(c.isdigit() for c in str(variants[0].get("eu_size", "") or variants[0].get("size", ""))) else ("Abbigliamento", "Taglia")
    bsku, brand = item.get("sku", "NOSKU"), item.get("brand", "Custom")
    product_handle = build_product_handle(name, bsku)
    
    vars_shopify, option_values, seen_option_values = [], [], set()
    for v in variants:
        size_val = get_variant_size(v) or "N/A"
        vars_shopify.append(build_variant_payload(bsku, v, o_name))
        if size_val not in seen_option_values:
            option_values.append({"name": size_val})
            seen_option_values.add(size_val)

    v = {"input": {"title": name, "handle": product_handle, "vendor": brand, "productType": p_type, "status": "ACTIVE", "tags": ["Turum", "turum-sync", p_type, brand], "productOptions": [{"name": o_name, "values": option_values}], "variants": vars_shopify}}
    return shopify_post({"query": "mutation productSet($input: ProductSetInput!) { productSet(input: $input) { product { id } userErrors { field message } } }", "variables": v})

def shopify_error_note(res):
    if not isinstance(res, dict):
        return "Risposta Shopify non valida o vuota durante creazione prodotto"
    return format_errors_for_log(res, "productSet", "Errore API Shopify creazione prodotto")

# =========================
# MAIN EXECUTION
# =========================

def main():
    global LOCATION_ID, PUBLICATION_ID
    init_logs()
    
    try:
        LOCATION_ID = get_shopify_location()
        PUBLICATION_ID = get_online_store_publication_id()
        if not LOCATION_ID: console_log("ERRORE: LOCATION_ID non trovato."); sys.exit(1)
        console_log(f"Shopify API in uso: {SHOPIFY_API_VERSION}")
        console_log(f"AUTO_CREATE_MISSING_VARIANTS: {AUTO_CREATE_MISSING_VARIANTS}")
        console_log(f"Script in esecuzione: {os.path.abspath(__file__)}")
        log_info(f"Shopify API in uso: {SHOPIFY_API_VERSION}")
        log_info(f"AUTO_CREATE_MISSING_VARIANTS: {AUTO_CREATE_MISSING_VARIANTS}")
        log_info(f"Script in esecuzione: {os.path.abspath(__file__)}")

        preload_collections_cache()
        products = get_turum_data()
        shopify_db, product_status_map = get_shopify_inventory()

        console_log(f"Trovati {len(products)} prodotti su Turum.")
        console_log(f"Trovate {len(shopify_db)} varianti totali su Shopify.")
        print("=" * 60)

        stock_updates, prices_updates = [], {}
        turum_skus_seen = set()
        stats = {"new": 0, "existing": 0, "stock_changed": 0, "price_changed": 0, "drafted": 0, "activated": 0, "missing_created": 0}
        missing_candidates_total = 0
        
        pending_publish_ids, pending_collection_assigns, pending_image_uploads = [], [], []

        for idx, item in enumerate(products, 1):
            name, base_sku, variants = item.get("name", "").strip(), item.get("sku", "NOSKU"), item.get("variants", [])
            sys.stdout.write(f"\r\033[K[{datetime.now().strftime('%H:%M:%S')}] [{idx}/{len(products)}] Elaborazione: {name[:40]}..."); sys.stdout.flush()

            if not item.get("image") or "not_found" in item.get("image"):
                log_txt("SKIP", name, base_sku, note="Nessuna immagine fornita da Turum"); continue
            if not variants: continue

            for v in variants: turum_skus_seen.add(build_variant_sku(base_sku, v))

            if any(build_variant_sku(base_sku, v) in shopify_db for v in variants):
                stats["existing"] += 1; p_total_stock, p_id = 0, None
                missing_variants = []
                option_name = "Taglia EU" if any(c.isdigit() for c in str(variants[0].get("eu_size", "") or variants[0].get("size", ""))) else "Taglia"

                for v in variants:
                    sku = build_variant_sku(base_sku, v)
                    t_stock = int(v.get("stock", 0))
                    t_price_raw = float(v.get("price", 0) or 0)
                    f_price = calc_final_price(t_price_raw)

                    if sku not in shopify_db:
                        log_txt("MISSING", name, sku, note="Variante su Turum ma non su Shopify")
                        missing_variants.append(v)
                        continue

                    shop_d = shopify_db[sku]
                    p_id, p_total_stock = shop_d["product_id"], p_total_stock + t_stock
                    s_changed, p_changed = False, False

                    # âœ… Fix casting esplicito per qty
                    s_qty_now = int(shop_d["qty"] or 0)
                    if s_qty_now != t_stock:
                        stock_updates.append({"inventoryItemId": shop_d["inv_id"], "locationId": LOCATION_ID, "quantity": t_stock})
                        stats["stock_changed"] += 1; s_changed = True
                        
                    if round(shop_d["price"], 2) != round(f_price, 2):
                        if p_id not in prices_updates: prices_updates[p_id] = []
                        prices_updates[p_id].append({"id": shop_d["variant_id"], "price": str(f_price)})
                        stats["price_changed"] += 1; p_changed = True

                    # ðŸ“ Logga su TXT sempre, su CSV solo se c'Ã¨ un cambiamento
                    log_txt("UPDATE" if (s_changed or p_changed) else "OK", name, sku, 
                            t_stock, s_qty_now, "SI" if s_changed else "NO", 
                            t_price_raw, shop_d["price"], f_price, "SI" if p_changed else "NO")
                    
                    if s_changed or p_changed: 
                        log_csv("UPDATE", name, sku, t_stock, s_qty_now, t_price_raw, shop_d["price"], f_price, 
                                f"Stock {'SI' if s_changed else 'NO'} | Price {'SI' if p_changed else 'NO'}")

                if p_id and missing_variants and AUTO_CREATE_MISSING_VARIANTS:
                    missing_candidates_total += len(missing_variants)
                    created_skus = create_missing_variants(p_id, base_sku, name, missing_variants, option_name)
                    if created_skus:
                        stats["missing_created"] += len(created_skus)
                        for mv in missing_variants:
                            mv_sku = build_variant_sku(base_sku, mv)
                            if mv_sku not in created_skus:
                                continue
                            log_txt("NEW", name, mv_sku, note="Variante mancante creata su prodotto esistente")
                            log_csv("NEW", name, mv_sku, mv.get("stock",0), 0, mv.get("price",0), 0, calc_final_price(mv.get("price",0)), "Variante mancante creata")
                elif missing_variants and not AUTO_CREATE_MISSING_VARIANTS:
                    missing_candidates_total += len(missing_variants)
                    log_txt("SKIP", name, base_sku, note=f"Auto-creazione varianti mancanti disattivata ({len(missing_variants)} varianti)")
                elif missing_variants and not p_id:
                    missing_candidates_total += len(missing_variants)
                    log_txt("ERROR", name, base_sku, note=f"Impossibile creare varianti mancanti: product_id non disponibile ({len(missing_variants)} varianti)")

                if p_id and p_id in product_status_map:
                    if p_total_stock == 0 and product_status_map[p_id] == "ACTIVE":
                        update_product_status(p_id, "DRAFT"); stats["drafted"] += 1
                        log_txt("STATUS", name, "ALL", note="Prodotto esaurito -> Messo in BOZZA")
                        log_csv("STATUS", name, "ALL", 0, 0, "-", "-", "-", "Esaurito -> BOZZA")
                    elif p_total_stock > 0 and product_status_map[p_id] == "DRAFT":
                        update_product_status(p_id, "ACTIVE"); stats["activated"] += 1
                        log_txt("STATUS", name, "ALL", note="Prodotto tornato in stock -> Messo ATTIVO")
                        log_csv("STATUS", name, "ALL", p_total_stock, 0, "-", "-", "-", "In Stock -> ATTIVO")

            else:
                if stats["new"] >= MAX_NEW_PRODUCTS_PER_RUN: continue
                pre_handle = build_product_handle(name, base_sku)
                existing_pid_by_handle = get_product_id_by_handle(pre_handle)
                if existing_pid_by_handle:
                    existing_product_skus = get_product_variant_skus(existing_pid_by_handle)
                    option_name = "Taglia EU" if any(c.isdigit() for c in str(variants[0].get("eu_size", "") or variants[0].get("size", ""))) else "Taglia"
                    missing_for_existing = [v for v in variants if build_variant_sku(base_sku, v) not in existing_product_skus]
                    if missing_for_existing:
                        missing_candidates_total += len(missing_for_existing)
                    created_skus = create_missing_variants(existing_pid_by_handle, base_sku, name, missing_for_existing, option_name)
                    if created_skus:
                        stats["missing_created"] += len(created_skus)
                        for mv in missing_for_existing:
                            mv_sku = build_variant_sku(base_sku, mv)
                            if mv_sku not in created_skus:
                                continue
                            log_txt("NEW", name, mv_sku, note="Variante creata su prodotto già esistente (handle match)")
                            log_csv("NEW", name, mv_sku, mv.get("stock",0), 0, mv.get("price",0), 0, calc_final_price(mv.get("price",0)), "Variante creata su handle esistente")
                    continue
                res = create_product(name, item, variants)
                product_set = (res.get("data", {}) or {}).get("productSet") or {}
                product_node = product_set.get("product") or {}
                pid = product_node.get("id")
                if pid:
                    pending_publish_ids.append(pid)
                    pending_image_uploads.append({"pid": pid, "image": item.get("image"), "name": name})
                    p_type = "Scarpe" if any(c.isdigit() for c in str(variants[0].get("eu_size", "") or variants[0].get("size", ""))) else "Abbigliamento"
                    coll_p = get_or_create_collection(p_type); coll_b = get_or_create_collection(item.get("brand", "Custom"))
                    numeric_pid = int(pid.split("/")[-1])
                    pending_collection_assigns.extend([{"p_id": numeric_pid, "c_id": coll_p}, {"p_id": numeric_pid, "c_id": coll_b}])
                    
                    stats["new"] += 1
                    for v in variants: 
                        size_new = str(v.get('eu_size', '') or v.get('size', '')).strip()
                        sku_new = f"{base_sku}-{size_new}" if size_new else base_sku
                        log_txt("NEW", name, sku_new, note="Creato ex-novo in Shopify (Batch)")
                        log_csv("NEW", name, sku_new, v.get("stock",0), 0, v.get("price",0), 0, round(float(v.get("price",0))*1.22*1.10,2), "Prodotto nuovo")
                else: 
                    err_note = shopify_error_note(res)
                    # Recovery automatico: in caso di handle già esistente, riconcilia le varianti sul prodotto trovato.
                    if "handle" in err_note.lower() and "already in use" in err_note.lower():
                        existing_pid_by_handle = get_product_id_by_handle(pre_handle)
                        if existing_pid_by_handle:
                            existing_product_skus = get_product_variant_skus(existing_pid_by_handle)
                            option_name = "Taglia EU" if any(c.isdigit() for c in str(variants[0].get("eu_size", "") or variants[0].get("size", ""))) else "Taglia"
                            missing_for_existing = [v for v in variants if build_variant_sku(base_sku, v) not in existing_product_skus]
                            created_skus = create_missing_variants(existing_pid_by_handle, base_sku, name, missing_for_existing, option_name)
                            if created_skus:
                                stats["missing_created"] += len(created_skus)
                                for mv in missing_for_existing:
                                    mv_sku = build_variant_sku(base_sku, mv)
                                    if mv_sku not in created_skus:
                                        continue
                                    log_txt("NEW", name, mv_sku, note="Recovery da handle duplicato: variante creata")
                                    log_csv("NEW", name, mv_sku, mv.get("stock",0), 0, mv.get("price",0), 0, calc_final_price(mv.get("price",0)), "Recovery handle duplicato")
                                continue
                    log_txt("ERROR", name, base_sku, note=err_note)

        print(); console_log("Fase 1 completata. Analisi Ghost e invio Bulk..."); print("=" * 60)

        if prices_updates:
            console_log(f"Aggiornamento prezzi simultaneo per {len(prices_updates)} prodotti...")
            for p_id, v_list in prices_updates.items(): bulk_price_update(p_id, v_list)

        if pending_publish_ids:
            console_log(f"Pubblicazione in blocco di {len(pending_publish_ids)} prodotti...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as exec:
                futures = [exec.submit(publish_to_online_store, pid) for pid in pending_publish_ids]; concurrent.futures.wait(futures)

        if pending_image_uploads:
            console_log(f"Caricamento immagini per {len(pending_image_uploads)} prodotti...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as exec:
                futures = [exec.submit(add_image_worker, i["pid"], i["image"], i["name"]) for i in pending_image_uploads]; concurrent.futures.wait(futures)

        if pending_collection_assigns:
            console_log(f"Assegnazione in blocco a {len(pending_collection_assigns)} collezioni...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exec:
                futures = [exec.submit(add_product_to_collection, a["p_id"], a["c_id"]) for a in pending_collection_assigns]; concurrent.futures.wait(futures)

        # ðŸ‘» ANALISI GHOST
        ghost_count, ghosts_stocked = 0, 0
        for sku, data in shopify_db.items():
            if data["is_turum"] and sku not in turum_skus_seen:
                ghost_count += 1
                s_qty = int(data.get("qty", 0) or 0)
                note = ""
                if s_qty > 0:
                    stock_updates.append({"inventoryItemId": data["inv_id"], "locationId": LOCATION_ID, "quantity": 0})
                    ghosts_stocked += 1; note = f"Stock azzerato (Prima: {s_qty})."
                else: note = "GiÃ  a 0 su Shopify."
                log_txt("GHOST", "PRODOTTO RIMOSSO", sku, note=note)

        total_stock_sent = stats['stock_changed'] + ghosts_stocked
        if stock_updates:
            console_log(f"Invio di {total_stock_sent} aggiornamenti giacenze ({stats['stock_changed']} normali + {ghosts_stocked} ghost)...")
            bulk_inventory_update(stock_updates)

        # ðŸ“Š RIEPILOGO FINALE
        elapsed_seconds = time.perf_counter() - START_TIME
        mins, secs = divmod(int(elapsed_seconds), 60)
        cleanup_old_logs(days=7)

        print("\n" + "=" * 60); console_log("RIEPILOGO FINALE"); print("=" * 60)
        print(f"  â³ Tempo di esecuzione:       {mins} min e {secs} sec")
        print(f"  ðŸ“¦ Nuovi prodotti creati:     {stats['new']}")
        print(f"  ðŸ”„ Variazioni Stock inviate:  {total_stock_sent}")
        print(f"  ðŸ’¶ Variazioni Prezzi:         {stats['price_changed']}")
        print(f"  ðŸ‘» Varianti 'Ghost' azzerate: {ghost_count} (di cui {ghosts_stocked} con stock attivo)")
        print(f"  ðŸ›Œ Prodotti messi in Bozza:   {stats['drafted']}")
        print(f"  â˜€ï¸ Prodotti Riattivati:       {stats['activated']}")
        print(f"  ðŸ§© Varianti mancanti create:   {stats['missing_created']}")
        print(f"  ðŸ“ Varianti mancanti candidate: {missing_candidates_total}")
        if _missing_create_failure_reasons:
            print("  ðŸ§ª Top errori creazione varianti:")
            for reason, count in sorted(_missing_create_failure_reasons.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"     - {count}x {reason[:120]}")
        print(f"\n  ðŸ“„ Log Audit (TXT):         {LOG_FILENAME}")
        print(f"  ðŸ“Š Log Modifiche (CSV):     {CSV_FILENAME}")
        print("=" * 60)

    finally: close_logs()

if __name__ == "__main__":
    main()

