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
SHOPIFY_API_URL  = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"
SHOPIFY_REST_URL = f"https://{SHOPIFY_STORE}/admin/api/2024-01"

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json"
}

LOCATION_ID    = ""
PUBLICATION_ID = ""
MAX_NEW_PRODUCTS_PER_RUN = 1850

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
        line += f"Turum Price: €{t_price} | Shopify Before: €{s_price} | Final Calc: €{f_price} | Price Updated: {p_changed} | "
        line += f"Turum Stock: {t_stock} | Shopify Before: {s_stock} | Stock Updated: {s_changed}"
    if note: line += f" | NOTE: {note}"
    
    LOG_FILE_HANDLE.write(line + "\n")
    LOG_FILE_HANDLE.flush()  # 🔒 Flush immediato

def log_csv(event, name, sku, t_stock, s_stock, t_price, s_price, f_price, note):
    if not CSV_WRITER: return
    CSV_WRITER.writerow([event, name[:50], sku, t_stock, s_stock, t_price, s_price, f_price, note])
    CSV_FILE_HANDLE.flush()  # 🔒 Flush immediato

def cleanup_old_logs(days=7):
    now = time.time()
    deleted = 0
    for f in glob.glob("sync_*_*.txt") + glob.glob("sync_*_*.csv"):
        if os.stat(f).st_mtime < now - (days * 86400): 
            os.remove(f)
            deleted += 1
    if deleted > 0: console_log(f"🧹 Pulizia automatica: Eliminati {deleted} vecchi report.")

# =========================
# API SHOPIFY (ANTI-CRASH + BACKOFF ADATTIVO)
# =========================

def handle_rate_limit(attempt):
    base = 2 ** attempt
    jitter = random.uniform(0, math.sqrt(base))
    sleep_time = min(base + jitter, 8)
    console_log(f"⏸️ Rate limit/Throttle: attesa {sleep_time:.1f}s")
    time.sleep(sleep_time)

def shopify_post(payload, retries=5):
    for attempt in range(retries):
        try:
            r = SHOP_SESSION.post(SHOPIFY_API_URL, headers=HEADERS, json=payload, timeout=30)
            if r.status_code == 429: 
                handle_rate_limit(attempt); continue
            data = r.json()
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
        if any(w in e["node"]["name"].lower() for w in ["online", "negozio", "web"]): return e["node"]["id"]
    return edges[0]["node"]["id"] if edges else None

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
    # ✅ FIX: Formattazione corretta delle parentesi graffe
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
        cid = r.json()["custom_collections"][0]["id"]; _collection_cache[title] = cid; return cid
    r = shopify_rest("POST", "custom_collections.json", {"custom_collection": {"title": title, "published": True}})
    if r and r.status_code == 201: cid = r.json()["custom_collection"]["id"]; _collection_cache[title] = cid; return cid
    return None

def add_product_to_collection(product_numeric_id, collection_id):
    if collection_id: shopify_rest("POST", "collects.json", {"collect": {"product_id": product_numeric_id, "collection_id": collection_id}})

# =========================
# AGGIORNAMENTI IN BLOCCO E CREAZIONE
# =========================

def bulk_inventory_update(updates_list):
    if not updates_list: return
    for i in range(0, len(updates_list), 100):
        chunk = updates_list[i:i + 100]
        # ✅ FIX: Formattazione corretta delle parentesi graffe
        shopify_post({
            "query": "mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) { inventorySetOnHandQuantities(input: $input) { userErrors { message } } }", 
            "variables": {
                "input": {
                    "reason": "correction", 
                    "setQuantities": chunk
                }
            }
        })
        console_log(f"  -> Inviato pacchetto stock {i+len(chunk)}/{len(updates_list)}...")

def bulk_price_update(product_id, variants_prices):
    shopify_post({
        "query": "mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) { productVariantsBulkUpdate(productId: $productId, variants: $variants) { userErrors { message } } }", 
        "variables": {
            "productId": product_id, 
            "variants": variants_prices
        }
    })

def get_shopify_inventory():
    console_log("Download inventario Shopify globale in corso...")
    inv, status_map, cursor, has_next = {}, {}, None, True
    while has_next:
        q = f'query($cursor: String) {{ productVariants(first: 250, after: $cursor) {{ pageInfo {{ hasNextPage endCursor }} edges {{ node {{ id sku price product {{ id status tags }} inventoryItem {{ id inventoryLevel(locationId: "{LOCATION_ID}") {{ quantities(names: ["available"]) {{ quantity }} }} }} }} }} }} }}'
        vdata = shopify_post({"query": q, "variables": {"cursor": cursor}}).get("data", {}).get("productVariants", {})
        for e in vdata.get("edges", []):
            n = e["node"]; sku = n.get("sku")
            if not sku: continue
            p_id = n["product"]["id"]; status_map[p_id] = n["product"]["status"]
            qty = 0
            if n.get("inventoryItem", {}).get("inventoryLevel"): qty = n["inventoryItem"]["inventoryLevel"]["quantities"][0]["quantity"]
            inv[sku] = {"variant_id": n["id"], "product_id": p_id, "inv_id": n["inventoryItem"]["id"], "qty": qty, "price": float(n["price"]), "is_turum": "Turum" in n["product"]["tags"]}
        has_next, cursor = vdata.get("pageInfo", {}).get("hasNextPage", False), vdata.get("pageInfo", {}).get("endCursor")
    return inv, status_map

def create_product(name, item, variants):
    p_type, o_name = ("Scarpe", "Taglia EU") if any(c.isdigit() for c in str(variants[0].get("eu_size", "") or variants[0].get("size", ""))) else ("Abbigliamento", "Taglia")
    bsku, brand = item.get("sku", "NOSKU"), item.get("brand", "Custom")
    handle_slug = HANDLE_RE.sub('-', name.lower()).strip('-'); sku_slug = HANDLE_RE.sub('-', bsku.lower()).strip('-')
    
    vars_shopify, option_values = [], []
    for v in variants:
        size_val = str(v.get('eu_size', '') or v.get('size', '')).strip()
        vars_shopify.append({"price": str(round(float(v.get("price", 0)) * 1.22 * 1.10, 2)), "sku": f"{bsku}-{size_val}" if size_val else bsku, "optionValues": [{"optionName": o_name, "name": size_val or "N/A"}], "inventoryQuantities": [{"name": "available", "quantity": int(v.get("stock", 0)), "locationId": LOCATION_ID}]})
        option_values.append({"name": size_val or "N/A"})

    images_payload = []
    if item.get("image") and "not_found" not in item.get("image", ""): images_payload.append({"src": item["image"], "altText": name})

    v = {"input": {"title": name, "handle": f"{handle_slug}-{sku_slug}", "vendor": brand, "productType": p_type, "status": "ACTIVE", "tags": ["Turum", "turum-sync", p_type, brand], "productOptions": [{"name": o_name, "values": option_values}], "variants": vars_shopify, "images": images_payload}}
    return shopify_post({"query": "mutation productSet($input: ProductSetInput!) { productSet(input: $input) { product { id } } }", "variables": v})

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

        preload_collections_cache()
        products = get_turum_data()
        shopify_db, product_status_map = get_shopify_inventory()

        console_log(f"Trovati {len(products)} prodotti su Turum.")
        console_log(f"Trovate {len(shopify_db)} varianti totali su Shopify.")
        print("=" * 60)

        stock_updates, prices_updates = [], {}
        turum_skus_seen = set()
        stats = {"new": 0, "existing": 0, "stock_changed": 0, "price_changed": 0, "drafted": 0, "activated": 0}
        
        pending_publish_ids, pending_collection_assigns = [], []

        for idx, item in enumerate(products, 1):
            name, base_sku, variants = item.get("name", "").strip(), item.get("sku", "NOSKU"), item.get("variants", [])
            sys.stdout.write(f"\r\033[K[{datetime.now().strftime('%H:%M:%S')}] [{idx}/{len(products)}] Elaborazione: {name[:40]}..."); sys.stdout.flush()

            if not item.get("image") or "not_found" in item.get("image"):
                log_txt("SKIP", name, base_sku, note="Nessuna immagine fornita da Turum"); continue
            if not variants: continue

            for v in variants: turum_skus_seen.add(f"{base_sku}-{str(v.get('eu_size','') or v.get('size','')).strip()}")

            if any(f"{base_sku}-{str(v.get('eu_size','') or v.get('size','')).strip()}" in shopify_db for v in variants):
                stats["existing"] += 1; p_total_stock, p_id = 0, None

                for v in variants:
                    size = str(v.get('eu_size', '') or v.get('size', '')).strip()
                    sku = f"{base_sku}-{size}" if size else base_sku
                    t_stock = int(v.get("stock", 0))
                    t_price_raw = float(v.get("price", 0))
                    f_price = round(t_price_raw * 1.22 * 1.10, 2)

                    if sku not in shopify_db: log_txt("MISSING", name, sku, note="Variante su Turum ma non su Shopify"); continue

                    shop_d = shopify_db[sku]
                    p_id, p_total_stock = shop_d["product_id"], p_total_stock + t_stock
                    s_changed, p_changed = False, False

                    # ✅ Fix casting esplicito per qty
                    s_qty_now = int(shop_d["qty"] or 0)
                    if s_qty_now != t_stock:
                        stock_updates.append({"inventoryItemId": shop_d["inv_id"], "locationId": LOCATION_ID, "quantity": t_stock})
                        stats["stock_changed"] += 1; s_changed = True
                        
                    if round(shop_d["price"], 2) != round(f_price, 2):
                        if p_id not in prices_updates: prices_updates[p_id] = []
                        prices_updates[p_id].append({"id": shop_d["variant_id"], "price": str(f_price)})
                        stats["price_changed"] += 1; p_changed = True

                    # 📝 Logga su TXT sempre, su CSV solo se c'è un cambiamento
                    log_txt("UPDATE" if (s_changed or p_changed) else "OK", name, sku, 
                            t_stock, s_qty_now, "SI" if s_changed else "NO", 
                            t_price_raw, shop_d["price"], f_price, "SI" if p_changed else "NO")
                    
                    if s_changed or p_changed: 
                        log_csv("UPDATE", name, sku, t_stock, s_qty_now, t_price_raw, shop_d["price"], f_price, 
                                f"Stock {'SI' if s_changed else 'NO'} | Price {'SI' if p_changed else 'NO'}")

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
                res = create_product(name, item, variants)
                
                # Estraiamo l'ID, ma anche eventuali messaggi di errore da Shopify
                pid = res.get("data", {}).get("productSet", {}).get("product", {}).get("id")
                user_errors = res.get("data", {}).get("productSet", {}).get("userErrors",[])
                top_errors = res.get("errors",[])

                if pid:
                    pending_publish_ids.append(pid)
                    p_type = "Scarpe" if any(c.isdigit() for c in str(variants[0].get("size", ""))) else "Abbigliamento"
                    coll_p = get_or_create_collection(p_type); coll_b = get_or_create_collection(item.get("brand", "Custom"))
                    numeric_pid = int(pid.split("/")[-1])
                    pending_collection_assigns.extend([{"p_id": numeric_pid, "c_id": coll_p}, {"p_id": numeric_pid, "c_id": coll_b}])
                    
                    stats["new"] += 1
                    for v in variants: 
                        sku_new = f"{base_sku}-{v.get('size','')}"
                        log_txt("NEW", name, sku_new, note="Creato ex-novo in Shopify (Batch)")
                        log_csv("NEW", name, sku_new, v.get("stock",0), 0, v.get("price",0), 0, round(float(v.get("price",0))*1.22*1.10,2), "Prodotto nuovo")
                else:
                    # Raccogliamo il vero motivo dell'errore
                    error_details = "Errore Sconosciuto"
                    if user_errors:
                        error_details = " | ".join([e.get("message", "") for e in user_errors])
                    elif top_errors:
                        error_details = " | ".join([e.get("message", "") for e in top_errors])
                        
                    log_txt("ERROR", name, base_sku, note=f"Errore Shopify: {error_details}")
                    
        print(); console_log("Fase 1 completata. Analisi Ghost e invio Bulk..."); print("=" * 60)

        if prices_updates:
            console_log(f"Aggiornamento prezzi simultaneo per {len(prices_updates)} prodotti...")
            for p_id, v_list in prices_updates.items(): bulk_price_update(p_id, v_list)

        if pending_publish_ids:
            console_log(f"Pubblicazione in blocco di {len(pending_publish_ids)} prodotti...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as exec:
                futures = [exec.submit(publish_to_online_store, pid) for pid in pending_publish_ids]; concurrent.futures.wait(futures)

        if pending_collection_assigns:
            console_log(f"Assegnazione in blocco a {len(pending_collection_assigns)} collezioni...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exec:
                futures = [exec.submit(add_product_to_collection, a["p_id"], a["c_id"]) for a in pending_collection_assigns]; concurrent.futures.wait(futures)

        # 👻 ANALISI GHOST
        ghost_count, ghosts_stocked = 0, 0
        for sku, data in shopify_db.items():
            if data["is_turum"] and sku not in turum_skus_seen:
                ghost_count += 1
                s_qty = int(data.get("qty", 0) or 0)
                note = ""
                if s_qty > 0:
                    stock_updates.append({"inventoryItemId": data["inv_id"], "locationId": LOCATION_ID, "quantity": 0})
                    ghosts_stocked += 1; note = f"Stock azzerato (Prima: {s_qty})."
                else: note = "Già a 0 su Shopify."
                log_txt("GHOST", "PRODOTTO RIMOSSO", sku, note=note)

        total_stock_sent = stats['stock_changed'] + ghosts_stocked
        if stock_updates:
            console_log(f"Invio di {total_stock_sent} aggiornamenti giacenze ({stats['stock_changed']} normali + {ghosts_stocked} ghost)...")
            bulk_inventory_update(stock_updates)

        # 📊 RIEPILOGO FINALE
        elapsed_seconds = time.perf_counter() - START_TIME
        mins, secs = divmod(int(elapsed_seconds), 60)
        cleanup_old_logs(days=7)

        print("\n" + "=" * 60); console_log("RIEPILOGO FINALE"); print("=" * 60)
        print(f"  ⏳ Tempo di esecuzione:       {mins} min e {secs} sec")
        print(f"  📦 Nuovi prodotti creati:     {stats['new']}")
        print(f"  🔄 Variazioni Stock inviate:  {total_stock_sent}")
        print(f"  💶 Variazioni Prezzi:         {stats['price_changed']}")
        print(f"  👻 Varianti 'Ghost' azzerate: {ghost_count} (di cui {ghosts_stocked} con stock attivo)")
        print(f"  🛌 Prodotti messi in Bozza:   {stats['drafted']}")
        print(f"  ☀️ Prodotti Riattivati:       {stats['activated']}")
        print(f"\n  📄 Log Audit (TXT):         {LOG_FILENAME}")
        print(f"  📊 Log Modifiche (CSV):     {CSV_FILENAME}")
        print("=" * 60)

    finally: close_logs()

if __name__ == "__main__":
    main()
