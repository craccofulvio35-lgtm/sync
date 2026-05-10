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
RETRY_STRATEGY = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY, pool_connections=25, pool_maxsize=100)
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
    LOG_FILE_HANDLE = open(LOG_FILENAME, "w", encoding="utf-8")
    LOG_FILE_HANDLE.write("=== AUDIT COMPLETO SINCRONIZZAZIONE ===\n\n")
    LOG_FILE_HANDLE.flush()
    
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
    LOG_FILE_HANDLE.flush()

def log_csv(event, name, sku, t_stock, s_stock, t_price, s_price, f_price, note):
    if not CSV_WRITER: return
    CSV_WRITER.writerow([event, name[:50], sku, t_stock, s_stock, t_price, s_price, f_price, note])
    CSV_FILE_HANDLE.flush()

def cleanup_old_logs(days=7):
    now = time.time()
    deleted = 0
    for f in glob.glob("sync_*_*.txt") + glob.glob("sync_*_*.csv"):
        if os.stat(f).st_mtime < now - (days * 86400): 
            os.remove(f)
            deleted += 1
    if deleted > 0: console_log(f"🧹 Pulizia automatica: Eliminati {deleted} vecchi report.")

# =========================
# API SHOPIFY (ANTI-CRASH)
# =========================

def handle_rate_limit(attempt):
    base = 2 ** attempt
    jitter = random.uniform(0, math.sqrt(base))
    sleep_time = min(base + jitter, 8)
    console_log(f"⏸️ Rate limit: attesa {sleep_time:.1f}s")
    time.sleep(sleep_time)

def shopify_post(payload, retries=4):
    for attempt in range(retries):
        try:
            r = SHOP_SESSION.post(SHOPIFY_API_URL, headers=HEADERS, json=payload, timeout=25)
            if r.status_code == 429: 
                handle_rate_limit(attempt); continue
            data = r.json()
            if "errors" in data:
                if any(e.get("extensions", {}).get("code") == "THROTTLED" for e in data["errors"]):
                    handle_rate_limit(attempt); continue
            available = data.get("extensions", {}).get("cost", {}).get("throttleStatus", {}).get("currentlyAvailable", 1000)
            if available < 100: time.sleep(1.0)
            return data
        except Exception:
            if attempt == retries - 1: return {}
            time.sleep(2)
    return {}

def shopify_rest(method, endpoint, payload=None, retries=3):
    url = f"{SHOPIFY_REST_URL}/{endpoint}"
    for attempt in range(retries):
        try:
            if method == "POST": r = SHOP_SESSION.post(url, headers=HEADERS, json=payload, timeout=25)
            elif method == "GET": r = SHOP_SESSION.get(url, headers=HEADERS, timeout=25)
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
        products = r_data.json().get("data",[])
        if not products: console_log("ERRORE CRITICO: 0 prodotti da Turum."); sys.exit(1)
        return products
    except Exception as e: console_log(f"ERRORE DI RETE TURUM: {e}"); sys.exit(1)

def get_shopify_location():
    data = shopify_post({'query': '{ locations(first: 5, query: "active:true") { edges { node { id } } } }'})
    edges = data.get("data", {}).get("locations", {}).get("edges", [])
    return edges[0]["node"]["id"] if edges else None

def get_online_store_publication_id():
    edges = shopify_post({'query': '{ publications(first: 20) { edges { node { id name } } } }'}).get("data", {}).get("publications", {}).get("edges",[])
    for e in edges:
        if any(w in e["node"]["name"].lower() for w in["online", "negozio", "web"]): return e["node"]["id"]
    return edges[0]["node"]["id"] if edges else None

def publish_to_online_store(product_id):
    if PUBLICATION_ID: 
        shopify_post({
            "query": "mutation publishablePublish($id: ID!, $input:[PublicationInput!]!) { publishablePublish(id: $id, input: $input) { userErrors { message } } }", 
            "variables": {
                "id": product_id, 
                "input":[{"publicationId": PUBLICATION_ID}]
            }
        })

def update_product_status(product_id, status):
    shopify_post({
        "query": "mutation productUpdate($input: ProductInput!) { productUpdate(input: $input) { userErrors { message } } }", 
        "variables": {"input": {"id": product_id, "status": status}}
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
        shopify_post({
            "query": "mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) { inventorySetOnHandQuantities(input: $input) { userErrors { message } } }", 
            "variables": {"input": {"reason": "correction", "setQuantities": chunk}}
        })

def bulk_price_update(product_id, variants_prices):
    shopify_post({
        "query": "mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) { productVariantsBulkUpdate(productId: $productId, variants: $variants) { userErrors { message } } }", 
        "variables": {"productId": product_id, "variants": variants_prices}
    })

def add_variant_to_product(product_id, sku, size, price, stock, o_name):
    q = """
    mutation vBulk($productId: ID!, $variants:[ProductVariantsBulkInput!]!) {
      productVariantsBulkCreate(productId: $productId, variants: $variants) {
        productVariants { id sku }
        userErrors { field message }
      }
    }
    """
    v = {"productId": product_id, "variants":[{
        "price": str(price), "sku": sku,
        "optionValues":[{"optionName": o_name, "name": size}],
        "inventoryItem": {"tracked": True},
        "inventoryQuantities":[{"locationId": LOCATION_ID, "name": "available", "quantity": int(stock)}]
    }]}
    return shopify_post({"query": q, "variables": v})

def get_shopify_inventory():
    console_log("Download inventario Shopify globale in corso...")
    inv, status_map, cursor, has_next = {}, {}, None, True
    
    # FIX: Query formattata in modo pulito e con le parentesi controllate!
    while has_next:
        q = f"""
        query($cursor: String) {{
          productVariants(first: 250, after: $cursor) {{
            pageInfo {{ hasNextPage endCursor }}
            edges {{
              node {{
                id sku price
                product {{ id status tags }}
                inventoryItem {{
                  id
                  inventoryLevel(locationId: "{LOCATION_ID}") {{
                    quantities(names: ["available"]) {{ quantity }}
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        res = shopify_post({"query": q, "variables": {"cursor": cursor}})
        if "errors" in res and not res.get("data"):
            console_log(f"Errore GraphQL fatale nel download inventario: {res['errors']}")
            sys.exit(1)
            
        vdata = res.get("data", {}).get("productVariants", {})
        for e in vdata.get("edges",[]):
            n = e["node"]; sku = n.get("sku")
            if not sku: continue
            p_id = n["product"]["id"]; status_map[p_id] = n["product"]["status"]
            qty = 0
            if n.get("inventoryItem", {}).get("inventoryLevel"): 
                qty = n["inventoryItem"]["inventoryLevel"]["quantities"][0]["quantity"]
            inv[sku] = {"variant_id": n["id"], "product_id": p_id, "inv_id": n["inventoryItem"]["id"], "qty": qty, "price": float(n["price"]), "is_turum": "Turum" in n["product"]["tags"]}
        
        has_next = vdata.get("pageInfo", {}).get("hasNextPage", False)
        cursor = vdata.get("pageInfo", {}).get("endCursor")
        
    return inv, status_map

def create_product(name, item, variants):
    p_type, o_name = ("Scarpe", "Taglia EU") if any(c.isdigit() for c in str(variants[0].get("eu_size", "") or variants[0].get("size", ""))) else ("Abbigliamento", "Taglia")
    bsku, brand = item.get("sku", "NOSKU"), item.get("brand", "Custom")
    handle_slug = HANDLE_RE.sub('-', name.lower()).strip('-'); sku_slug = HANDLE_RE.sub('-', bsku.lower()).strip('-')
    
    vars_shopify =[]
    for v in variants:
        size_val = str(v.get('eu_size', '') or v.get('size', '')).strip()
        vars_shopify.append({
            "price": str(round(float(v.get("price", 0)) * 1.22 * 1.10, 2)), 
            "sku": f"{bsku}-{size_val}" if size_val else bsku, 
            "options": [size_val or "N/A"], 
            "inventoryItem": {"tracked": True},
            "inventoryQuantities":[{"locationId": LOCATION_ID, "availableQuantity": int(v.get("stock", 0))}]
        })

    product_input = {
        "title": name, "handle": f"{handle_slug}-{sku_slug}", "vendor": brand, "productType": p_type, "status": "ACTIVE", 
        "tags":["Turum", "turum-sync", p_type, brand], "options": [o_name], "variants": vars_shopify
    }
    
    variables = {"input": product_input, "media":[]} # Inizializza array vuoto
    if item.get("image") and "not_found" not in item.get("image", ""):
        variables["media"] = [{
            "alt": name,
            "mediaContentType": "IMAGE",
            "originalSource": item["image"]
        }]

    q = """
    mutation pCreate($input: ProductInput!, $media: [CreateMediaInput!]) { 
      productCreate(input: $input, media: $media) { 
        product { id } 
        userErrors { field message } 
      } 
    }
    """
    return shopify_post({"query": q, "variables": variables})

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

        stock_updates, prices_updates =[], {}
        turum_skus_seen = set()
        stats = {"new": 0, "existing": 0, "stock_changed": 0, "price_changed": 0, "drafted": 0, "activated": 0, "fixed": 0}
        
        pending_publish_ids, pending_collection_assigns = [],[]

        for idx, item in enumerate(products, 1):
            name, base_sku, variants = item.get("name", "").strip(), item.get("sku", "NOSKU"), item.get("variants",[])
            sys.stdout.write(f"\r\033[K[{datetime.now().strftime('%H:%M:%S')}][{idx}/{len(products)}] Elaborazione: {name[:40]}..."); sys.stdout.flush()

            if not item.get("image") or "not_found" in item.get("image"):
                log_txt("SKIP", name, base_sku, note="Nessuna immagine fornita da Turum"); continue
            if not variants: continue

            # Controllo se il prodotto padre esiste
            existing_p_id = None
            for v in variants:
                sku = f"{base_sku}-{str(v.get('eu_size','') or v.get('size','')).strip()}"
                turum_skus_seen.add(sku)
                if sku in shopify_db: existing_p_id = shopify_db[sku]["product_id"]

            if existing_p_id:
                stats["existing"] += 1; p_total_stock = 0

                # Calcolo per l'opzione da abbinare nel caso serva creare una variante mancante
                _, o_name = ("Scarpe", "Taglia EU") if any(c.isdigit() for c in str(variants[0].get("eu_size", "") or variants[0].get("size", ""))) else ("Abbigliamento", "Taglia")

                for v in variants:
                    size = str(v.get('eu_size', '') or v.get('size', '')).strip()
                    sku = f"{base_sku}-{size}" if size else base_sku
                    t_stock = int(v.get("stock", 0))
                    t_price_raw = float(v.get("price", 0))
                    f_price = round(t_price_raw * 1.22 * 1.10, 2)
                    p_total_stock += t_stock

                    if sku not in shopify_db: 
                        # FIX[MISSING] -> Tenta di creare la variante che manca
                        res_v = add_variant_to_product(existing_p_id, sku, size, f_price, t_stock, o_name)
                        v_err = res_v.get("data", {}).get("productVariantsBulkCreate", {}).get("userErrors",[])
                        
                        if not v_err and res_v.get("data", {}).get("productVariantsBulkCreate", {}).get("productVariants"):
                            log_txt("FIXED", name, sku, note="Variante mancante creata con successo")
                            stats["fixed"] += 1
                        else:
                            err_msg = v_err[0].get("message") if v_err else res_v.get("errors", [{"message": "Unknown"}])[0].get("message")
                            log_txt("MISSING", name, sku, note=f"Errore creazione variante: {err_msg}")
                        continue

                    shop_d = shopify_db[sku]
                    s_changed, p_changed = False, False

                    s_qty_now = int(shop_d["qty"] or 0)
                    if s_qty_now != t_stock:
                        stock_updates.append({"inventoryItemId": shop_d["inv_id"], "locationId": LOCATION_ID, "quantity": t_stock})
                        stats["stock_changed"] += 1; s_changed = True
                        
                    if round(shop_d["price"], 2) != round(f_price, 2):
                        if existing_p_id not in prices_updates: prices_updates[existing_p_id] =[]
                        prices_updates[existing_p_id].append({"id": shop_d["variant_id"], "price": str(f_price)})
                        stats["price_changed"] += 1; p_changed = True

                    log_txt("UPDATE" if (s_changed or p_changed) else "OK", name, sku, 
                            t_stock, s_qty_now, "SI" if s_changed else "NO", 
                            t_price_raw, shop_d["price"], f_price, "SI" if p_changed else "NO")
                    
                    if s_changed or p_changed: 
                        log_csv("UPDATE", name, sku, t_stock, s_qty_now, t_price_raw, shop_d["price"], f_price, 
                                f"Stock {'SI' if s_changed else 'NO'} | Price {'SI' if p_changed else 'NO'}")

                if existing_p_id and existing_p_id in product_status_map:
                    if p_total_stock == 0 and product_status_map[existing_p_id] == "ACTIVE":
                        update_product_status(existing_p_id, "DRAFT"); stats["drafted"] += 1
                        log_txt("STATUS", name, "ALL", note="Prodotto esaurito -> Messo in BOZZA")
                    elif p_total_stock > 0 and product_status_map[existing_p_id] == "DRAFT":
                        update_product_status(existing_p_id, "ACTIVE"); stats["activated"] += 1
                        log_txt("STATUS", name, "ALL", note="Prodotto tornato in stock -> Messo ATTIVO")

            else:
                # Creazione Prodotto Nuovo
                if stats["new"] >= MAX_NEW_PRODUCTS_PER_RUN: continue
                res = create_product(name, item, variants)
                pid = res.get("data", {}).get("productCreate", {}).get("product", {}).get("id") if res.get("data") else None
                
                if pid:
                    pending_publish_ids.append(pid)
                    p_type = "Scarpe" if any(c.isdigit() for c in str(variants[0].get("size", ""))) else "Abbigliamento"
                    coll_p = get_or_create_collection(p_type); coll_b = get_or_create_collection(item.get("brand", "Custom"))
                    numeric_pid = int(pid.split("/")[-1])
                    pending_collection_assigns.extend([{"p_id": numeric_pid, "c_id": coll_p}, {"p_id": numeric_pid, "c_id": coll_b}])
                    
                    stats["new"] += 1
                    for v in variants: 
                        sku_new = f"{base_sku}-{v.get('size','') or v.get('eu_size','')}".strip('-')
                        log_txt("NEW", name, sku_new, note="Creato ex-novo in Shopify (Batch)")
                        log_csv("NEW", name, sku_new, v.get("stock",0), 0, v.get("price",0), 0, round(float(v.get("price",0))*1.22*1.10,2), "Prodotto nuovo")
                else: 
                    # Cattura l'errore vero da GraphQL
                    graphql_errors = res.get("errors",[])
                    user_errors = res.get("data", {}).get("productCreate", {}).get("userErrors", []) if res.get("data") else[]
                    err_msg = user_errors[0].get("message") if user_errors else (graphql_errors[0].get("message") if graphql_errors else "Errore Sconosciuto")
                    log_txt("ERROR", name, base_sku, note=f"Errore API Shopify: {err_msg}")

        print("\n"); console_log("Fase 1 completata. Invio task multi-thread e analisi Ghost..."); print("=" * 60)

        if prices_updates:
            console_log(f"Aggiornamento prezzi simultaneo per {len(prices_updates)} prodotti...")
            for p_id, v_list in prices_updates.items(): bulk_price_update(p_id, v_list)

        if pending_publish_ids:
            console_log(f"Pubblicazione in blocco di {len(pending_publish_ids)} prodotti...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as exec:
                futures =[exec.submit(publish_to_online_store, pid) for pid in pending_publish_ids]; concurrent.futures.wait(futures)

        if pending_collection_assigns:
            console_log(f"Assegnazione in blocco a {len(pending_collection_assigns)} collezioni...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exec:
                futures =[exec.submit(add_product_to_collection, a["p_id"], a["c_id"]) for a in pending_collection_assigns]; concurrent.futures.wait(futures)

        # 👻 ANALISI GHOST
        ghost_count, ghosts_stocked = 0, 0
        products_to_draft = set()
        
        for sku, data in shopify_db.items():
            if data["is_turum"] and sku not in turum_skus_seen:
                ghost_count += 1
                s_qty = int(data.get("qty", 0) or 0)
                
                if s_qty > 0:
                    stock_updates.append({"inventoryItemId": data["inv_id"], "locationId": LOCATION_ID, "quantity": 0})
                    ghosts_stocked += 1
                
                products_to_draft.add(data["product_id"])
                log_txt("GHOST", "PRODOTTO RIMOSSO", sku, note=f"Esaurito -> BOZZA (Prima: {s_qty})")

        # Sposta i ghost in bozza per nasconderli dal sito
        if products_to_draft:
            console_log(f"Spostamento di {len(products_to_draft)} prodotti 'Ghost' in bozza...")
            for pid in products_to_draft: update_product_status(pid, "DRAFT")

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
        print(f"  🔧 Varianti mancanti create:  {stats['fixed']}")
        print(f"  🔄 Variazioni Stock inviate:  {total_stock_sent}")
        print(f"  💶 Variazioni Prezzi:         {stats['price_changed']}")
        print(f"  👻 Varianti 'Ghost' azzerate: {ghost_count} (di cui {ghosts_stocked} attive, ora Bozza)")
        print(f"  🛌 Prodotti messi in Bozza:   {stats['drafted'] + len(products_to_draft)}")
        print(f"  ☀️ Prodotti Riattivati:       {stats['activated']}")
        print(f"\n  📄 Log Audit (TXT):         {LOG_FILENAME}")
        print(f"  📊 Log Modifiche (CSV):     {CSV_FILENAME}")
        print("=" * 60)

    finally: close_logs()

if __name__ == "__main__":
    main()
