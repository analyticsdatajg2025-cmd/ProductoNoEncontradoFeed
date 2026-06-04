import os
import csv
import io
import json
import base64
import random
import asyncio
import datetime as dt

import aiohttp
import gspread
from google.oauth2.service_account import Credentials

# ---------------- Config (vía variables de entorno / secrets) ----------------
FEED_URL      = os.environ["FEED_URL"]
SHEET_ID      = os.environ["SHEET_ID"]
WS_ERRORES    = os.environ.get("WORKSHEET", "Errores")        # productos rotos confirmados
WS_SIN_IMAGEN = os.environ.get("WORKSHEET_IMG", "SinImagen")  # image_link vacío (solo lee el feed)
WS_NO_VERIF   = os.environ.get("WORKSHEET_NV", "NoVerificado") # 429/timeout tras reintentos

CONCURRENCY = int(os.environ.get("CONCURRENCY", "10"))    # peticiones en paralelo (OJO: 30 gatilla rate-limit)
TIMEOUT     = int(os.environ.get("TIMEOUT", "25"))        # segundos por link
READ_BYTES  = int(os.environ.get("READ_BYTES", "60000"))  # cuánto HTML leer para buscar el marcador
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))     # reintentos ante 429 / timeout
LIMIT       = int(os.environ.get("LIMIT", "0"))           # 0 = todos; >0 = solo los primeros N (modo prueba)
DEBUG       = os.environ.get("DEBUG", "0") == "1"         # 1 = imprime cada link y NO escribe al Sheet

# Textos que indican "producto no encontrado" (separa varios con | )
ERROR_MARKERS = [
    m.strip().lower()
    for m in os.environ.get("ERROR_MARKERS", "producto no encontrado").split("|")
    if m.strip()
]

# ---------------- Auth Google Sheets ----------------
def get_client():
    raw = (os.environ.get("GCP_SA_JSON") or os.environ.get("GCP_SA_BASE64") or "").strip()
    if raw.startswith("{"):
        info = json.loads(raw)                                    # JSON directo
    else:
        info = json.loads(base64.b64decode(raw).decode("utf-8"))  # base64 (respaldo)
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
    i_id, i_title, i_link, i_img = idx.get("id"), idx.get("title"), idx.get("link"), idx.get("image_link")
    if i_link is None:
        raise RuntimeError(f"No encontré la columna 'link'. Cabeceras: {header}")
    if i_img is None:
        print("AVISO: no encontré la columna 'image_link'; el chequeo de imágenes se omite.")

    def safe(row, i):
        return row[i].strip() if (i is not None and i < len(row)) else ""

    rows, sin_imagen = [], []
    for row in reader:
        if not row:
            continue
        pid   = safe(row, i_id)
        title = safe(row, i_title)
        link  = safe(row, i_link)
        img   = safe(row, i_img)
        # Chequeo de imagen: directo del feed, sin HTTP. Es confiable al 100%.
        if i_img is not None and not img:
            sin_imagen.append([pid, title, link])
        rows.append((pid, title, link))
    return rows, sin_imagen

# ---------------- Helper: cuánto esperar ante un 429 ----------------
def calc_espera(retry_after, intento):
    # Respeta el header Retry-After si viene; si no, backoff exponencial con jitter.
    if retry_after:
        try:
            return min(float(retry_after), 30)
        except ValueError:
            pass
    return min((2 ** intento) + random.random(), 30)

# ---------------- Chequear un link (con reintentos) ----------------
async def check_one(session, sem, item):
    pid, title, url = item
    if not url:
        return None
    async with sem:
        for intento in range(MAX_RETRIES + 1):
            try:
                async with session.get(
                    url,
                    allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                ) as r:
                    status = r.status

                    # --- 429: NO es un error de producto. Reintentar con pausa. ---
                    if status == 429:
                        if intento < MAX_RETRIES:
                            await asyncio.sleep(calc_espera(r.headers.get("Retry-After"), intento))
                            continue
                        # Tras agotar reintentos seguimos bloqueados -> no verificado, NO error.
                        if DEBUG:
                            print(f"[429] no verificado tras {MAX_RETRIES} reintentos {url[:90]}")
                        return ("nv", [pid, title, url, "429", "No verificado (rate limit)"])

                    # --- Error duro de HTTP (404, 410, 500...): producto roto ---
                    if status >= 400:
                        if DEBUG:
                            print(f"[{status}] ERROR {url[:90]}")
                        return ("err", [pid, title, url, str(status), f"HTTP {status}"])

                    # --- 200/3xx: revisar soft-404 leyendo el cuerpo ---
                    chunk = await r.content.read(READ_BYTES)
                    body = chunk.decode("utf-8", errors="ignore").lower()
                    soft404 = any(m in body for m in ERROR_MARKERS)
                    if DEBUG:
                        print(f"[{status}] marcador={soft404} bytes={len(chunk)} {url[:90]}")
                    if soft404:
                        return ("err", [pid, title, url, str(status), "Producto no encontrado"])
                    return None  # OK, todo bien

            except asyncio.TimeoutError:
                if intento < MAX_RETRIES:
                    await asyncio.sleep(calc_espera(None, intento))
                    continue
                return ("nv", [pid, title, url, "TIMEOUT", "No verificado (no respondió)"])
            except Exception as e:
                return ("nv", [pid, title, url, "ERROR", str(e)[:120]])
    return None

# ---------------- Escribir una hoja ----------------
def escribir_hoja(sh, nombre, header, filas, nota=""):
    try:
        ws = sh.worksheet(nombre)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=nombre, rows=max(len(filas) + 10, 100), cols=8)
    ws.update(values=[header] + filas, range_name="A1", value_input_option="RAW")
    if nota:
        ws.update(values=[[nota]], range_name="G1", value_input_option="RAW")

# ---------------- Main ----------------
async def main():
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    }
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        rows, sin_imagen = await download_feed(session)
        print(f"Filas en el feed: {len(rows)} | Sin image_link: {len(sin_imagen)}")
        if LIMIT > 0:
            rows = rows[:LIMIT]
            print(f"MODO PRUEBA: revisando solo los primeros {len(rows)} links")

        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [asyncio.create_task(check_one(session, sem, item)) for item in rows]

        errores, no_verif, done = [], [], 0
        for coro in asyncio.as_completed(tasks):
            res = await coro
            done += 1
            if done % 5000 == 0:
                print(f"Procesados {done}/{len(rows)}")
            if res:
                tipo, fila = res
                (errores if tipo == "err" else no_verif).append(fila)

    print(f"Errores reales: {len(errores)} | No verificados: {len(no_verif)} | Sin imagen: {len(sin_imagen)}")

    if DEBUG:
        print("MODO DEBUG: no se escribe al Sheet.")
        return

    gc = get_client()
    sh = gc.open_by_key(SHEET_ID)
    lima = dt.timezone(dt.timedelta(hours=-5))
    ts = dt.datetime.now(dt.timezone.utc).astimezone(lima)

    escribir_hoja(sh, WS_ERRORES, ["id", "title", "link", "status", "motivo"], errores,
                  nota=f"Última ejecución: {ts:%Y-%m-%d %H:%M} (Lima) — {len(errores)} productos rotos")
    escribir_hoja(sh, WS_SIN_IMAGEN, ["id", "title", "link"], sin_imagen,
                  nota=f"Última ejecución: {ts:%Y-%m-%d %H:%M} (Lima) — {len(sin_imagen)} sin image_link")
    escribir_hoja(sh, WS_NO_VERIF, ["id", "title", "link", "status", "motivo"], no_verif,
                  nota=f"Última ejecución: {ts:%Y-%m-%d %H:%M} (Lima) — {len(no_verif)} no verificados (revisar si suben mucho)")
    print("Listo, escrito en el Sheet (3 hojas).")

if __name__ == "__main__":
    asyncio.run(main())