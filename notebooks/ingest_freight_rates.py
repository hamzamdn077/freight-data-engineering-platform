# Databricks notebook source
# MAGIC %pip install azure-keyvault-secrets azure-identity

# COMMAND ----------

dbutils.secrets.get(scope="kv", key="freight-api-key")

# COMMAND ----------

import requests, json
from datetime import datetime

# COMMAND ----------

API_KEY  = dbutils.secrets.get(scope="kv", key="freight-api-key")
BASE_URL = "https://freightpulsehq.com/api/v1"
HEADERS  = {"X-API-Key": API_KEY, "Accept": "application/json"}
output = {}

# COMMAND ----------

# ── 1. Fuel prices (EIA via FreightPulse) ──────────────
r=requests.get(f"{BASE_URL}/fuel-prices",headers=HEADERS,timeout=15)
r.raise_for_status()
output["fuel_prices"]=r.json()
print(f"✅ Fuel prices fetched")


# COMMAND ----------

import time
# ── 2. Port congestion (IMF PortWatch via FreightPulse) ─
PORTS = [
    "Shanghai", "Rotterdam", "Hamburg", "Felixstowe",
    "Los Angeles", "New York", "Singapore", "Busan",
    "Casablanca", "Agadir", "Tanger Med"
]

port_data = []
for i, port in enumerate(PORTS):
    resp = requests.get(
        f"{BASE_URL}/port-congestion",
        headers=HEADERS,
        params={"port": port},
        timeout=15
    )
    if resp.ok:
        port_data.append({"port": port, "data": resp.json()})
        print(f"  ✅ {port}")
    elif resp.status_code == 429:
        print(f"  ⏳ Rate limit hit at {port}, waiting 15s...")
        time.sleep(15)
        retry = requests.get(
            f"{BASE_URL}/port-congestion",
            headers=HEADERS,
            params={"port": port},
            timeout=15
        )
        if retry.ok:
            port_data.append({"port": port, "data": retry.json()})
            print(f"  ✅ {port} (retry OK)")
        else:
            print(f"  ❌ {port} failed after retry: {retry.status_code}")
    else:
        print(f"  ⚠️ {port}: {resp.status_code}")

    # 13s between calls = ~4.6 req/min, safely under free tier limit
    if i < len(PORTS) - 1:
        time.sleep(13)

output["port_congestion"] = port_data


# COMMAND ----------

# 3. FBX route rates
output["fbx_route_rates"] = [
    {"origin": "Shanghai",   "dest": "Rotterdam",   "rate_usd_per_teu": 2779, "index": "FBX11", "container": "40HC"},
    {"origin": "Shenzhen",   "dest": "Hamburg",     "rate_usd_per_teu": 2779, "index": "FBX11", "container": "40HC"},
    {"origin": "Singapore",  "dest": "Felixstowe",  "rate_usd_per_teu": 2779, "index": "FBX11", "container": "40HC"},
    {"origin": "Busan",      "dest": "Los Angeles", "rate_usd_per_teu": 2418, "index": "FBX01", "container": "40HC"},
    {"origin": "Ningbo",     "dest": "New York",    "rate_usd_per_teu": 3859, "index": "FBX03", "container": "40HC"},
    {"origin": "Tanger Med", "dest": "Rotterdam",   "rate_usd_per_teu": 446,  "index": "FBX21", "container": "40HC"},
    {"origin": "Tanger Med", "dest": "Barcelona",   "rate_usd_per_teu": 446,  "index": "FBX21", "container": "40HC"},
    {"origin": "Casablanca", "dest": "Marseille",   "rate_usd_per_teu": 446,  "index": "FBX21", "container": "40HC"},
    {"origin": "Agadir",     "dest": "Hamburg",     "rate_usd_per_teu": 1416, "index": "FBX22", "container": "40HC"},
]
print(f"✅ FBX route rates: {len(output['fbx_route_rates'])} lanes")

# COMMAND ----------

date_str=datetime.now().strftime("%Y-%m-%d")
output_path=f"abfss://bronze@freightstorageacc.dfs.core.windows.net/freight_rates/{date_str}.json"
dbutils.fs.put(output_path,json.dumps(output, indent=2),overwrite=True)

# COMMAND ----------

