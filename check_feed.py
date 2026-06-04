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
WS_ERRORES    = os.environ.get("WORKSHEET", "Errores")         # productos rotos confirmados
WS_SIN_IMAGEN = os.environ.get("WORKSHEET_IMG", "SinImagen")   # image_link vacío (solo lee el feed)
WS_NO_VERIF   = os.environ.get("WORKSHEET_NV", "NoVerificado") # 429/timeout tras 2 pasadas

CONCURRENCY    = int(os.environ.get("CONCURRENCY", "10"))     # 1ra pasada (rápida)
CONCURRENCY_2  = int(os.environ.get("CONCURRENCY_2", "3"))    # 2da pasada (gentil, limpia 429)
COOLDOWN       = int(os.environ.get("COOLDOWN", "45"))        # seg de pausa antes de la 2da pasada
TIMEOUT        = int(os.environ.get("TIMEOUT", "25"))         # seg por link
READ_BYTES     = int(os.environ.get("READ_BYTES", "60000"))  # cuánto HTML leer para buscar el marcador
MAX_RETRIES    = int(os.environ.get("MAX_RETRIES", "4"))      # reintentos por link en la 1ra pasada
MAX_RETRIES_2  = int(os.environ.get("MAX_RETRIES_2", "6"))    # reintentos por link en la 2da pasada
LIMIT          = int(os.environ.get("LIMIT", "0"))           # 0 = todos; >0 = solo los primeros N (prueba)
DEBUG          = os.environ.get("DEBUG", "0") == "1"         # 1 = imprime cada link y NO escribe al Sheet

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
        pid, title, link, img = safe(row, i_id), safe(row, i_title), safe(row, i_link), safe(row, i_img)
        if i_img is not None and not img:
            sin_imagen.append([pid, title, link])
        rows.append((pid, title, link))
    return rows, sin_imagen

# ---------------- Helper: cuánto esperar ante un 429 ----------------
def calc_espera(retry_after, intento):
    if retry_after:
        try:
            return min(float(retry_after), 30)
        except ValueError:
            pass
    return min((2 ** intento) + random.random(), 30)

# ---------------- Chequear un link (con reintentos) ----------------
async def check_one(session, sem, item, max_retries):
    pid, title, url = item
    if not url:
        return None
    async with sem:
        for intento in range(max_retries + 1):
            try:
                async with session.get(
                    url, allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                ) as r:
                    status = r.status

                    if status == 429:  # rate-limit: NO es error de producto, reintentar
                        if intento < max_retries:
                            await asyncio.sleep(calc_espera(r.headers.get("Retry-After"), intento))
                            continue
                        return ("nv", [pid, title, url, "429", "No verificado (rate limit)"])

                    if status >= 400:  # 404/410/500: producto roto
                        return ("err", [pid, title, url, str(status), f"HTTP {status}"])

                    chunk = await r.content.read(READ_BYTES)
                    body = chunk.decode("utf-8", errors="ignore").lower()
                    if any(m in body for m in ERROR_MARKERS):
                        return ("err", [pid, title, url, str(status), "Producto no encontrado"])
                    return None  # OK

            except asyncio.TimeoutError:
                if intento < max_retries:
                    await asyncio.sleep(calc_espera(None, intento))
                    continue
                return ("nv", [pid, title, url, "TIMEOUT", "No verificado (no respondió)"])
            except Exception as e:
                return ("nv", [pid, title, url, "ERROR", str(e)[:120]])
    return None

# ---------------- Correr una tanda de links ----------------
async def correr_tanda(session, items, concurrencia, max_retries, etiqueta):
    sem = asyncio.Semaphore(concurrencia)
    tasks = [asyncio.create_task(check_one(session, sem, it, max_retries)) for it in items]
    errores, no_verif, done = [], [], 0
    for coro in asyncio.as_completed(tasks):
        res = await coro
        done += 1
        if done % 5000 == 0:
            print(f"  [{etiqueta}] {done}/{len(items)}")
        if res:
            tipo, fila = res
            (errores if tipo == "err" else no_verif).append(fila)
    return errores, no_verif

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

        # 1ra pasada: rápida
        print(f"Pasada 1 (concurrencia {CONCURRENCY})...")
        errores, no_verif = await correr_tanda(session, rows, CONCURRENCY, MAX_RETRIES, "p1")
        print(f"  Pasada 1 -> errores: {len(errores)} | no verificados: {len(no_verif)}")

        # 2da pasada: re-chequea SOLO los no verificados, gentil para no gatillar 429
        if no_verif:
            print(f"Enfriando {COOLDOWN}s antes de la pasada 2...")
            await asyncio.sleep(COOLDOWN)
            reintentar = [(r[0], r[1], r[2]) for r in no_verif]
            print(f"Pasada 2 (concurrencia {CONCURRENCY_2}) sobre {len(reintentar)} links...")
            err2, no_verif = await correr_tanda(session, reintentar, CONCURRENCY_2, MAX_RETRIES_2, "p2")
            errores.extend(err2)
            print(f"  Pasada 2 -> nuevos errores: {len(err2)} | siguen sin verificar: {len(no_verif)}")

    print(f"FINAL -> Errores: {len(errores)} | No verificados: {len(no_verif)} | Sin imagen: {len(sin_imagen)}")

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
                  nota=f"Última ejecución: {ts:%Y-%m-%d %H:%M} (Lima) — {len(no_verif)} no verificados (si suben mucho, baja CONCURRENCY)")
    print("Listo, escrito en el Sheet (3 hojas).")

if __name__ == "__main__":
    asyncio.run(main())
