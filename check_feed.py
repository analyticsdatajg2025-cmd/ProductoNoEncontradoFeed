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

# ---------------- Config ----------------
FEED_URL      = os.environ["FEED_URL"]
SHEET_ID      = os.environ["SHEET_ID"]
WS_REPORTE    = os.environ.get("WORKSHEET", "Reporte")
WS_RESUMEN    = os.environ.get("WORKSHEET_RESUMEN", "Resumen")

# Sharding: SHARD_INDEX = qué trozo procesa este job (0-based)
#           SHARD_TOTAL = cuántos jobs en paralelo hay en total
SHARD_INDEX   = int(os.environ.get("SHARD_INDEX", "0"))
SHARD_TOTAL   = int(os.environ.get("SHARD_TOTAL", "1"))
IS_MERGE      = os.environ.get("MERGE_JOB", "0") == "1"

CONCURRENCY   = int(os.environ.get("CONCURRENCY", "50"))
CONCURRENCY_2 = int(os.environ.get("CONCURRENCY_2", "10"))
COOLDOWN      = int(os.environ.get("COOLDOWN", "30"))
TIMEOUT       = int(os.environ.get("TIMEOUT", "20"))
READ_BYTES    = int(os.environ.get("READ_BYTES", "60000"))
MAX_RETRIES   = int(os.environ.get("MAX_RETRIES", "3"))
MAX_RETRIES_2 = int(os.environ.get("MAX_RETRIES_2", "5"))
LIMIT         = int(os.environ.get("LIMIT", "0"))
DEBUG         = os.environ.get("DEBUG", "0") == "1"

ERROR_MARKERS = [
    m.strip().lower()
    for m in os.environ.get("ERROR_MARKERS", "producto no encontrado").split("|")
    if m.strip()
]

TIPO_ORDEN = {"LINK_ROTO": 1, "SOFT_404": 2, "SIN_IMAGEN": 3, "NO_VERIFICADO": 4}

HEADER = ["tipo_error", "id", "title", "link", "status_http", "motivo", "acción_sugerida"]

ACCIONES = {
    "LINK_ROTO":     "Revisar URL o dar de baja el producto del feed",
    "SOFT_404":      "Verificar que el producto esté publicado y activo",
    "SIN_IMAGEN":    "Subir imagen al producto o completar el campo image_link",
    "NO_VERIFICADO": "Revisar manualmente — el servidor no respondió en tiempo",
}

# ---------------- Auth ----------------
def get_client():
    raw = (os.environ.get("GCP_SA_JSON") or os.environ.get("GCP_SA_BASE64") or "").strip()
    if raw.startswith("{"):
        info = json.loads(raw)
    else:
        info = json.loads(base64.b64decode(raw).decode("utf-8"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

# ---------------- Feed ----------------
async def download_feed(session):
    async with session.get(FEED_URL, timeout=aiohttp.ClientTimeout(total=300)) as r:
        r.raise_for_status()
        data = await r.text()

    reader = csv.reader(io.StringIO(data), delimiter="\t")
    header = [h.strip().lower() for h in next(reader)]
    idx = {name: i for i, name in enumerate(header)}
    i_id = idx.get("id"); i_title = idx.get("title")
    i_link = idx.get("link"); i_img = idx.get("image_link")

    if i_link is None:
        raise RuntimeError(f"No encontré columna 'link'. Cabeceras: {header}")

    def safe(row, i):
        return row[i].strip() if (i is not None and i < len(row)) else ""

    rows, sin_imagen = [], []
    for row in reader:
        if not row:
            continue
        pid = safe(row, i_id); title = safe(row, i_title)
        link = safe(row, i_link); img = safe(row, i_img)
        if i_img is not None and not img:
            sin_imagen.append({"tipo": "SIN_IMAGEN", "id": pid, "title": title,
                                "link": link, "status": "", "motivo": "image_link vacío en el feed"})
        rows.append((pid, title, link))
    return rows, sin_imagen

# ---------------- HTTP check ----------------
def calc_espera(retry_after, intento):
    if retry_after:
        try:
            return min(float(retry_after), 30)
        except ValueError:
            pass
    return min((2 ** intento) + random.random(), 30)

async def check_one(session, sem, item, max_retries):
    pid, title, url = item
    if not url:
        return None
    async with sem:
        for intento in range(max_retries + 1):
            try:
                async with session.get(url, allow_redirects=True,
                                       timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as r:
                    status = r.status
                    if status == 429:
                        if intento < max_retries:
                            await asyncio.sleep(calc_espera(r.headers.get("Retry-After"), intento))
                            continue
                        return {"tipo": "NO_VERIFICADO", "id": pid, "title": title, "link": url,
                                "status": "429", "motivo": "Rate limit — no se pudo verificar"}
                    if status >= 400:
                        return {"tipo": "LINK_ROTO", "id": pid, "title": title, "link": url,
                                "status": str(status), "motivo": f"HTTP {status}"}
                    chunk = await r.content.read(READ_BYTES)
                    body = chunk.decode("utf-8", errors="ignore").lower()
                    if any(m in body for m in ERROR_MARKERS):
                        return {"tipo": "SOFT_404", "id": pid, "title": title, "link": url,
                                "status": str(status), "motivo": "Soft-404: producto no encontrado en página"}
                    return None
            except asyncio.TimeoutError:
                if intento < max_retries:
                    await asyncio.sleep(calc_espera(None, intento))
                    continue
                return {"tipo": "NO_VERIFICADO", "id": pid, "title": title, "link": url,
                        "status": "TIMEOUT", "motivo": "No respondió (timeout)"}
            except Exception as e:
                return {"tipo": "NO_VERIFICADO", "id": pid, "title": title, "link": url,
                        "status": "ERROR", "motivo": str(e)[:120]}
    return None

async def correr_tanda(session, items, concurrencia, max_retries, etiqueta):
    sem = asyncio.Semaphore(concurrencia)
    tasks = [asyncio.create_task(check_one(session, sem, it, max_retries)) for it in items]
    problemas, no_verif, done = [], [], 0
    for coro in asyncio.as_completed(tasks):
        res = await coro
        done += 1
        if done % 2000 == 0:
            print(f"  [{etiqueta}] {done}/{len(items)}")
        if res:
            (no_verif if res["tipo"] == "NO_VERIFICADO" else problemas).append(res)
    return problemas, no_verif

# ---------------- Sheets helpers ----------------
def _pct(n, total):
    return f"{n / total * 100:.2f}%" if total else "—"

def fila(d):
    return [d["tipo"], d["id"], d["title"], d["link"],
            d["status"], d["motivo"], ACCIONES.get(d["tipo"], "")]

def get_or_create_ws(sh, nombre, rows=500, cols=8):
    try:
        ws = sh.worksheet(nombre)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=nombre, rows=rows, cols=cols)
    return ws

def escribir_shard(sh, shard_idx, filas_dict):
    """Escribe resultados parciales de un shard en hoja temporal _shard_N."""
    nombre = f"_shard_{shard_idx}"
    ws = get_or_create_ws(sh, nombre, rows=max(len(filas_dict) + 5, 50))
    if filas_dict:
        ws.update(values=[HEADER] + [fila(d) for d in filas_dict],
                  range_name="A1", value_input_option="RAW")
    else:
        ws.update(values=[HEADER], range_name="A1", value_input_option="RAW")
    print(f"  → Shard {shard_idx}: {len(filas_dict)} problemas → hoja '{nombre}'")

def merge_shards(sh, shard_total, total_feed, sin_imagen, ts):
    """Lee todas las hojas _shard_N, consolida en Reporte + Resumen, borra temporales."""
    todos = list(sin_imagen)  # sin_imagen solo se reporta en el merge

    for idx in range(shard_total):
        nombre = f"_shard_{idx}"
        try:
            ws = sh.worksheet(nombre)
            vals = ws.get_all_values()
            if len(vals) > 1:  # tiene datos además del header
                for row in vals[1:]:
                    if len(row) >= 6:
                        todos.append({
                            "tipo": row[0], "id": row[1], "title": row[2],
                            "link": row[3], "status": row[4], "motivo": row[5],
                        })
            sh.del_worksheet(ws)
            print(f"  Shard {idx} leído y eliminado ({len(vals)-1} filas)")
        except gspread.WorksheetNotFound:
            print(f"  AVISO: hoja _shard_{idx} no encontrada — shard puede haber fallado")

    # Ordenar por severidad
    todos.sort(key=lambda d: (TIPO_ORDEN.get(d["tipo"], 9), d["id"]))

    # Conteos
    conteos = {}
    for d in todos:
        conteos[d["tipo"]] = conteos.get(d["tipo"], 0) + 1

    # Escribir Reporte
    ws_rep = get_or_create_ws(sh, WS_REPORTE, rows=max(len(todos) + 20, 200))
    ws_rep.update(values=[HEADER] + [fila(d) for d in todos],
                  range_name="A1", value_input_option="RAW")
    ws_rep.update(values=[[f"Última ejecución: {ts:%Y-%m-%d %H:%M} (Lima) — {len(todos)} problemas"]],
                  range_name="H1", value_input_option="RAW")
    try:
        ws_rep.freeze(rows=1)
    except Exception:
        pass
    print(f"  → Reporte: {len(todos)} filas totales")

    # Escribir Resumen
    ws_res = get_or_create_ws(sh, WS_RESUMEN, rows=20, cols=4)
    filas_res = [
        ["Resumen de la última ejecución", "", "", ""],
        ["", "", "", ""],
        ["Fecha / hora (Lima)", f"{ts:%Y-%m-%d %H:%M}", "", ""],
        ["Productos en el feed", str(total_feed), "", ""],
        ["Shards utilizados", str(shard_total), "", ""],
        ["", "", "", ""],
        ["tipo_error", "cantidad", "% del feed", "qué significa"],
        ["LINK_ROTO",     str(conteos.get("LINK_ROTO", 0)),
         _pct(conteos.get("LINK_ROTO", 0), total_feed),
         "HTTP 4xx/5xx — URL no existe o el servidor rechaza"],
        ["SOFT_404",      str(conteos.get("SOFT_404", 0)),
         _pct(conteos.get("SOFT_404", 0), total_feed),
         "Página carga pero dice 'producto no encontrado'"],
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
    ws_res.update(values=filas_res, range_name="A1", value_input_option="RAW")
    print(f"  → Resumen escrito")

    return conteos

# ---------------- MERGE JOB ----------------
async def run_merge():
    """Solo descarga el feed para contar filas + sin_imagen, luego consolida shards."""
    http_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"
    }
    async with aiohttp.ClientSession(headers=http_headers) as session:
        rows, sin_imagen = await download_feed(session)

    total_feed = len(rows)
    print(f"Feed: {total_feed} productos | Sin imagen: {len(sin_imagen)}")

    gc = get_client()
    sh = gc.open_by_key(SHEET_ID)
    lima = dt.timezone(dt.timedelta(hours=-5))
    ts = dt.datetime.now(dt.timezone.utc).astimezone(lima)

    conteos = merge_shards(sh, SHARD_TOTAL, total_feed, sin_imagen, ts)

    print(f"\nMERGE COMPLETO:")
    for tipo, n in sorted(conteos.items(), key=lambda x: TIPO_ORDEN.get(x[0], 9)):
        print(f"  {tipo}: {n}")
    print(f"  TOTAL: {sum(conteos.values())} problemas")

# ---------------- SHARD JOB ----------------
async def run_shard():
    """Descarga el feed, procesa su trozo, escribe en hoja temporal _shard_N."""
    http_headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    }
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY,
                                     ttl_dns_cache=300)
    async with aiohttp.ClientSession(headers=http_headers, connector=connector) as session:
        rows, _ = await download_feed(session)  # sin_imagen se maneja solo en merge
        total = len(rows)

        # Aplicar LIMIT global si está en modo prueba
        if LIMIT > 0:
            rows = rows[:LIMIT]

        # Dividir el feed en SHARD_TOTAL trozos; este job toma el trozo SHARD_INDEX
        chunk = rows[SHARD_INDEX::SHARD_TOTAL]  # interleaved: 0,10,20... / 1,11,21...
        print(f"Shard {SHARD_INDEX}/{SHARD_TOTAL} | feed total: {total} | "
              f"este chunk: {len(chunk)} links")

        # Pasada 1
        print(f"Pasada 1 (concurrencia {CONCURRENCY})...")
        problemas, no_verif = await correr_tanda(session, chunk, CONCURRENCY, MAX_RETRIES, "p1")
        print(f"  → problemas: {len(problemas)} | no verificados: {len(no_verif)}")

        # Pasada 2
        if no_verif:
            print(f"Enfriando {COOLDOWN}s antes de pasada 2...")
            await asyncio.sleep(COOLDOWN)
            reintentar = [(d["id"], d["title"], d["link"]) for d in no_verif]
            print(f"Pasada 2 (concurrencia {CONCURRENCY_2}) sobre {len(reintentar)} links...")
            prob2, no_verif = await correr_tanda(session, reintentar, CONCURRENCY_2,
                                                  MAX_RETRIES_2, "p2")
            problemas.extend(prob2)
            print(f"  → nuevos problemas: {len(prob2)} | siguen sin verificar: {len(no_verif)}")

    todos = problemas + no_verif
    print(f"Shard {SHARD_INDEX} terminado: {len(todos)} problemas encontrados")

    if DEBUG:
        print("DEBUG: no se escribe al Sheet.")
        return

    gc = get_client()
    sh = gc.open_by_key(SHEET_ID)
    escribir_shard(sh, SHARD_INDEX, todos)

# ---------------- Entry point ----------------
if __name__ == "__main__":
    if IS_MERGE:
        asyncio.run(run_merge())
    else:
        asyncio.run(run_shard())
