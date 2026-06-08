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
WS_REPORTE    = os.environ.get("WORKSHEET", "Reporte")         # hoja única consolidada
WS_RESUMEN    = os.environ.get("WORKSHEET_RESUMEN", "Resumen") # conteos + última ejecución

CONCURRENCY    = int(os.environ.get("CONCURRENCY", "10"))
CONCURRENCY_2  = int(os.environ.get("CONCURRENCY_2", "3"))
COOLDOWN       = int(os.environ.get("COOLDOWN", "45"))
TIMEOUT        = int(os.environ.get("TIMEOUT", "25"))
READ_BYTES     = int(os.environ.get("READ_BYTES", "60000"))
MAX_RETRIES    = int(os.environ.get("MAX_RETRIES", "4"))
MAX_RETRIES_2  = int(os.environ.get("MAX_RETRIES_2", "6"))
LIMIT          = int(os.environ.get("LIMIT", "0"))             # 0 = TODOS (producción)
DEBUG          = os.environ.get("DEBUG", "0") == "1"

ERROR_MARKERS = [
    m.strip().lower()
    for m in os.environ.get("ERROR_MARKERS", "producto no encontrado").split("|")
    if m.strip()
]

# Prioridad visual en el reporte (para ordenar filas)
TIPO_ORDEN = {
    "LINK_ROTO":      1,
    "SOFT_404":       2,
    "SIN_IMAGEN":     3,
    "NO_VERIFICADO":  4,
}

# ---------------- Auth Google Sheets ----------------
def get_client():
    raw = (os.environ.get("GCP_SA_JSON") or os.environ.get("GCP_SA_BASE64") or "").strip()
    if raw.startswith("{"):
        info = json.loads(raw)
    else:
        info = json.loads(base64.b64decode(raw).decode("utf-8"))
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
    i_id    = idx.get("id")
    i_title = idx.get("title")
    i_link  = idx.get("link")
    i_img   = idx.get("image_link")

    if i_link is None:
        raise RuntimeError(f"No encontré la columna 'link'. Cabeceras: {header}")
    if i_img is None:
        print("AVISO: no encontré la columna 'image_link'; los productos sin imagen no se detectarán.")

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

        # Productos sin image_link → van directo al reporte (no requieren HTTP check)
        if i_img is not None and not img:
            sin_imagen.append({
                "tipo": "SIN_IMAGEN",
                "id": pid, "title": title, "link": link,
                "status": "", "motivo": "image_link vacío en el feed",
            })
        rows.append((pid, title, link))

    return rows, sin_imagen

# ---------------- Helper: backoff ante 429 ----------------
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

                    if status == 429:
                        if intento < max_retries:
                            await asyncio.sleep(calc_espera(r.headers.get("Retry-After"), intento))
                            continue
                        return {
                            "tipo": "NO_VERIFICADO",
                            "id": pid, "title": title, "link": url,
                            "status": "429", "motivo": "Rate limit — no se pudo verificar",
                        }

                    if status >= 400:
                        return {
                            "tipo": "LINK_ROTO",
                            "id": pid, "title": title, "link": url,
                            "status": str(status), "motivo": f"HTTP {status}",
                        }

                    chunk = await r.content.read(READ_BYTES)
                    body = chunk.decode("utf-8", errors="ignore").lower()
                    if any(m in body for m in ERROR_MARKERS):
                        return {
                            "tipo": "SOFT_404",
                            "id": pid, "title": title, "link": url,
                            "status": str(status), "motivo": "Soft-404: producto no encontrado en página",
                        }
                    return None  # OK

            except asyncio.TimeoutError:
                if intento < max_retries:
                    await asyncio.sleep(calc_espera(None, intento))
                    continue
                return {
                    "tipo": "NO_VERIFICADO",
                    "id": pid, "title": title, "link": url,
                    "status": "TIMEOUT", "motivo": "No respondió (timeout)",
                }
            except Exception as e:
                return {
                    "tipo": "NO_VERIFICADO",
                    "id": pid, "title": title, "link": url,
                    "status": "ERROR", "motivo": str(e)[:120],
                }
    return None

# ---------------- Correr una tanda de links ----------------
async def correr_tanda(session, items, concurrencia, max_retries, etiqueta):
    sem = asyncio.Semaphore(concurrencia)
    tasks = [asyncio.create_task(check_one(session, sem, it, max_retries)) for it in items]
    problemas, no_verif, done = [], [], 0
    for coro in asyncio.as_completed(tasks):
        res = await coro
        done += 1
        if done % 5000 == 0:
            print(f"  [{etiqueta}] {done}/{len(items)}")
        if res:
            if res["tipo"] == "NO_VERIFICADO":
                no_verif.append(res)
            else:
                problemas.append(res)
    return problemas, no_verif

# ---------------- Formato para la hoja ----------------
HEADER = ["tipo_error", "id", "title", "link", "status_http", "motivo", "acción_sugerida"]

ACCIONES = {
    "LINK_ROTO":     "Revisar URL o dar de baja el producto del feed",
    "SOFT_404":      "Verificar que el producto esté publicado y activo",
    "SIN_IMAGEN":    "Subir imagen al producto o completar el campo image_link",
    "NO_VERIFICADO": "Revisar manualmente — el servidor no respondió en tiempo",
}

def fila(d):
    return [
        d["tipo"],
        d["id"],
        d["title"],
        d["link"],
        d["status"],
        d["motivo"],
        ACCIONES.get(d["tipo"], ""),
    ]

# ---------------- Escribir hojas ----------------
def escribir_reporte(sh, nombre, filas_dict, ts):
    """Escribe la hoja principal con todos los problemas, ordenados por tipo."""
    filas_dict.sort(key=lambda d: (TIPO_ORDEN.get(d["tipo"], 9), d["id"]))
    filas = [fila(d) for d in filas_dict]

    try:
        ws = sh.worksheet(nombre)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=nombre, rows=max(len(filas) + 20, 200), cols=8)

    ws.update(values=[HEADER] + filas, range_name="A1", value_input_option="RAW")

    # Formato: congelar fila 1, nota de ejecución en H1
    ws.update(values=[[f"Última ejecución: {ts:%Y-%m-%d %H:%M} (Lima) — {len(filas)} problemas totales"]],
              range_name="H1", value_input_option="RAW")

    try:
        ws.freeze(rows=1)
    except Exception:
        pass  # freeze puede fallar en algunas versiones; no es crítico

    print(f"  → Hoja '{nombre}': {len(filas)} filas escritas.")


def escribir_resumen(sh, nombre, conteos, total_feed, ts):
    """Escribe una hoja de resumen ejecutivo con conteos por tipo."""
    try:
        ws = sh.worksheet(nombre)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=nombre, rows=20, cols=4)

    filas = [
        ["Resumen de la última ejecución", "", "", ""],
        ["", "", "", ""],
        ["Fecha / hora (Lima)",  f"{ts:%Y-%m-%d %H:%M}", "", ""],
        ["Productos en el feed", str(total_feed), "", ""],
        ["", "", "", ""],
        ["tipo_error", "cantidad", "% del feed", "qué significa"],
        ["LINK_ROTO",     str(conteos.get("LINK_ROTO", 0)),
         _pct(conteos.get("LINK_ROTO", 0), total_feed),
         "HTTP 4xx/5xx — URL no existe o servidor rechaza"],
        ["SOFT_404",      str(conteos.get("SOFT_404", 0)),
         _pct(conteos.get("SOFT_404", 0), total_feed),
         "Página carga pero muestra 'producto no encontrado'"],
        ["SIN_IMAGEN",    str(conteos.get("SIN_IMAGEN", 0)),
         _pct(conteos.get("SIN_IMAGEN", 0), total_feed),
         "image_link vacío en el feed — producto sin foto"],
        ["NO_VERIFICADO", str(conteos.get("NO_VERIFICADO", 0)),
         _pct(conteos.get("NO_VERIFICADO", 0), total_feed),
         "Timeout o rate limit — revisar manualmente"],
        ["", "", "", ""],
        ["TOTAL PROBLEMAS", str(sum(conteos.values())),
         _pct(sum(conteos.values()), total_feed), ""],
    ]

    ws.update(values=filas, range_name="A1", value_input_option="RAW")
    print(f"  → Hoja '{nombre}': resumen escrito.")

def _pct(n, total):
    if not total:
        return "—"
    return f"{n / total * 100:.2f}%"

# ---------------- Main ----------------
async def main():
    headers_http = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    }
    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY, limit_per_host=CONCURRENCY, ttl_dns_cache=300
    )
    async with aiohttp.ClientSession(headers=headers_http, connector=connector) as session:

        # 1. Descargar y parsear feed
        rows, sin_imagen = await download_feed(session)
        total_feed = len(rows)
        print(f"Filas en el feed: {total_feed} | Sin image_link: {len(sin_imagen)}")

        if LIMIT > 0:
            rows = rows[:LIMIT]
            print(f"MODO PRUEBA: revisando solo los primeros {len(rows)} links")

        # 2. Pasada 1: rápida
        print(f"Pasada 1 (concurrencia {CONCURRENCY})...")
        problemas, no_verif = await correr_tanda(session, rows, CONCURRENCY, MAX_RETRIES, "p1")
        print(f"  Pasada 1 → problemas: {len(problemas)} | no verificados: {len(no_verif)}")

        # 3. Pasada 2: reintenta solo los no verificados, gentil
        if no_verif:
            print(f"Enfriando {COOLDOWN}s antes de la pasada 2...")
            await asyncio.sleep(COOLDOWN)
            reintentar = [(d["id"], d["title"], d["link"]) for d in no_verif]
            print(f"Pasada 2 (concurrencia {CONCURRENCY_2}) sobre {len(reintentar)} links...")
            prob2, no_verif = await correr_tanda(
                session, reintentar, CONCURRENCY_2, MAX_RETRIES_2, "p2"
            )
            problemas.extend(prob2)
            print(f"  Pasada 2 → nuevos problemas: {len(prob2)} | siguen sin verificar: {len(no_verif)}")

    # 4. Consolidar todo
    todos = problemas + sin_imagen + no_verif   # sin_imagen no necesitó HTTP check
    conteos = {}
    for d in todos:
        conteos[d["tipo"]] = conteos.get(d["tipo"], 0) + 1

    print(f"\nRESUMEN FINAL:")
    for tipo, n in sorted(conteos.items(), key=lambda x: TIPO_ORDEN.get(x[0], 9)):
        print(f"  {tipo}: {n}")
    print(f"  TOTAL: {len(todos)} problemas sobre {total_feed} productos")

    if DEBUG:
        print("\nMODO DEBUG: no se escribe al Sheet.")
        return

    # 5. Escribir en Sheets
    gc = get_client()
    sh = gc.open_by_key(SHEET_ID)
    lima = dt.timezone(dt.timedelta(hours=-5))
    ts = dt.datetime.now(dt.timezone.utc).astimezone(lima)

    escribir_reporte(sh, WS_REPORTE, todos, ts)
    escribir_resumen(sh, WS_RESUMEN, conteos, total_feed, ts)

    print("\nListo — 2 hojas actualizadas en el Sheet.")

if __name__ == "__main__":
    asyncio.run(main())
