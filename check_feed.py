import os
import csv
import io
import json
import base64
import asyncio
import datetime as dt

import aiohttp
import gspread
from google.oauth2.service_account import Credentials

# ---------------- Config (vía variables de entorno / secrets) ----------------
FEED_URL    = os.environ["FEED_URL"]
SHEET_ID    = os.environ["SHEET_ID"]
WORKSHEET   = os.environ.get("WORKSHEET", "Errores")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "30"))   # peticiones en paralelo
TIMEOUT     = int(os.environ.get("TIMEOUT", "20"))       # segundos por link
READ_BYTES  = int(os.environ.get("READ_BYTES", "200000"))  # cuánto HTML leer para buscar el marcador

# Textos que indican "producto no encontrado" (separa varios con | )
ERROR_MARKERS = [
    m.strip().lower()
    for m in os.environ.get("ERROR_MARKERS", "producto no encontrado").split("|")
    if m.strip()
]

# ---------------- Auth Google Sheets ----------------
def get_client():
    raw = base64.b64decode(os.environ["GCP_SA_BASE64"]).decode("utf-8")
    info = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

# ---------------- Descargar y parsear el feed ----------------
async def download_feed(session):
    # timeout largo y propio: el feed pesa 70MB+
    async with session.get(FEED_URL, timeout=aiohttp.ClientTimeout(total=300)) as r:
        r.raise_for_status()
        data = await r.text()

    reader = csv.reader(io.StringIO(data), delimiter="\t")
    header = [h.strip().lower() for h in next(reader)]
    idx = {name: i for i, name in enumerate(header)}
    i_id, i_title, i_link = idx.get("id"), idx.get("title"), idx.get("link")
    if i_link is None:
        raise RuntimeError(f"No encontré la columna 'link'. Cabeceras: {header}")

    def safe(row, i):
        return row[i].strip() if (i is not None and i < len(row)) else ""

    rows = []
    for row in reader:
        if not row:
            continue
        rows.append((safe(row, i_id), safe(row, i_title), safe(row, i_link)))
    return rows

# ---------------- Chequear un link ----------------
async def check_one(session, sem, item):
    pid, title, url = item
    if not url:
        return None
    async with sem:
        try:
            async with session.get(
                url,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            ) as r:
                status = r.status
                chunk = await r.content.read(READ_BYTES)
                body = chunk.decode("utf-8", errors="ignore").lower()
                soft404 = any(m in body for m in ERROR_MARKERS)
                if status >= 400 or soft404:
                    motivo = f"HTTP {status}" if status >= 400 else "Producto no encontrado"
                    return [pid, title, url, str(status), motivo]
        except asyncio.TimeoutError:
            return [pid, title, url, "TIMEOUT", "No respondió a tiempo"]
        except Exception as e:
            return [pid, title, url, "ERROR", str(e)[:120]]
    return None

# ---------------- Escribir resultados al Sheet ----------------
def write_sheet(results):
    gc = get_client()
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(WORKSHEET)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET, rows=max(len(results) + 10, 100), cols=8)

    lima = dt.timezone(dt.timedelta(hours=-5))
    ts = dt.datetime.now(dt.timezone.utc).astimezone(lima)

    header = ["id", "title", "link", "status", "motivo"]
    ws.update(values=[header] + results, range_name="A1", value_input_option="RAW")
    ws.update(values=[[f"Última ejecución: {ts:%Y-%m-%d %H:%M} (Lima) — {len(results)} links con problema"]],
              range_name="G1", value_input_option="RAW")

# ---------------- Main ----------------
async def main():
    headers = {"User-Agent": "JuntozFeedChecker/1.0 (revision de links del feed)"}
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        rows = await download_feed(session)
        print(f"Filas en el feed: {len(rows)}")

        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [asyncio.create_task(check_one(session, sem, item)) for item in rows]

        results, done = [], 0
        for coro in asyncio.as_completed(tasks):
            res = await coro
            done += 1
            if done % 5000 == 0:
                print(f"Procesados {done}/{len(rows)}")
            if res:
                results.append(res)

    print(f"Links con problema: {len(results)}")
    write_sheet(results)
    print("Listo, escrito en el Sheet.")

if __name__ == "__main__":
    asyncio.run(main())