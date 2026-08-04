import os
import re
import csv
import io
import json
import base64
import random
import asyncio
import unicodedata
import datetime as dt
from urllib.parse import urlsplit

import aiohttp
import gspread
from google.oauth2.service_account import Credentials

# ---------------- Config ----------------
FEED_URL      = os.environ["FEED_URL"]
SHEET_ID      = os.environ["SHEET_ID"]
WS_REPORTE    = os.environ.get("WORKSHEET", "Reporte")
WS_RESUMEN    = os.environ.get("WORKSHEET_RESUMEN", "Resumen")

SHARD_INDEX   = int(os.environ.get("SHARD_INDEX", "0"))
SHARD_TOTAL   = int(os.environ.get("SHARD_TOTAL", "1"))
IS_MERGE      = os.environ.get("MERGE_JOB", "0") == "1"

CONCURRENCY   = int(os.environ.get("CONCURRENCY", "25"))
CONCURRENCY_2 = int(os.environ.get("CONCURRENCY_2", "8"))
COOLDOWN      = int(os.environ.get("COOLDOWN", "30"))
TIMEOUT       = int(os.environ.get("TIMEOUT", "25"))
MAX_RETRIES   = int(os.environ.get("MAX_RETRIES", "3"))
MAX_RETRIES_2 = int(os.environ.get("MAX_RETRIES_2", "5"))
LIMIT         = int(os.environ.get("LIMIT", "0"))
DEBUG         = os.environ.get("DEBUG", "0") == "1"

# Antes: READ_BYTES=60000. Insuficiente — el marcador vive en el <body>.
MAX_BYTES     = int(os.environ.get("MAX_BYTES", "1500000"))

# ---------------- Marcadores (calibrados para juntoz.com) ----------------
# Sobre HTML CRUDO: assets exclusivos de la página de error.
ERROR_MARKERS_RAW = [
    "not-found-product.png",        # imagen exclusiva del 404 de producto
    "orig_juntoz-new-logo.png",     # og:image fallback = no hay producto real
]

# Sobre TEXTO VISIBLE + <title> (minúsculas, sin tildes).
ERROR_MARKERS_TXT = [
    "producto no encontrado",
    "este producto no esta disponible por ahora",
    "tenemos buenas y malas noticias",
]

# Prueba POSITIVA de PDP vivo.
# NO usar precios ni "agregar a carrito": el carrusel de relacionados los
# renderiza también en la página caída.
POSITIVE_MARKERS_TXT = [
    "vendido y enviado por",
]

SIN_STOCK_MARKERS = ["producto fuera de stock", "agotado"]

# NUNCA agregar "no se encontraron resultados": es el empty state del buscador
# del header y aparece en el DOM de TODAS las páginas de Juntoz.
RE_CONTEO = re.compile(r"se encontraron\s*(\d+)\s*productos")

TIPO_ORDEN = {
    "LINK_ROTO": 1, "SOFT_404": 2, "REDIRECT": 3, "STOCK_DESALINEADO": 4,
    "SIN_STOCK": 5, "SIN_IMAGEN": 6, "SIN_PRODUCTOS": 7, "REVISAR": 8,
    "NO_VERIFICADO": 9,
}

ACCIONES = {
    "LINK_ROTO":         "Revisar URL o dar de baja el producto del feed",
    "SOFT_404":          "Producto inexistente — quitar del feed",
    "REDIRECT":          "La URL redirige a otra página — corregir link en el feed",
    "STOCK_DESALINEADO": "El feed dice disponible pero la página no — revisar sincronización",
    "SIN_STOCK":         "Producto sin stock — pausar en campañas hasta reposición",
    "SIN_IMAGEN":        "Subir imagen o completar el campo image_link",
    "SIN_PRODUCTOS":     "Listado sin resultados — revisar la categoría",
    "REVISAR":           "Cargó 200 OK sin bloque de compra — revisar manualmente",
    "NO_VERIFICADO":     "El servidor no respondió en tiempo — reintentar",
}

QUE_SIGNIFICA = {
    "LINK_ROTO":         "HTTP 4xx/5xx — la URL no existe o el servidor rechaza",
    "SOFT_404":          "Página 'Producto no encontrado' devuelta con HTTP 200",
    "REDIRECT":          "La URL final es distinta a la del feed",
    "STOCK_DESALINEADO": "Feed dice in_stock, la página dice lo contrario",
    "SIN_STOCK":         "PDP vivo pero sin stock disponible",
    "SIN_IMAGEN":        "image_link vacío en el feed",
    "SIN_PRODUCTOS":     "Página de listado que carga con 0 productos",
    "REVISAR":           "200 OK sin señal de ficha de producto — ambiguo",
    "NO_VERIFICADO":     "Timeout o rate limit — revisar manualmente",
}

HEADER = ["tipo_error", "id", "title", "link", "url_final",
          "status_http", "motivo", "acción_sugerida"]

HTTP_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    # Sin "br": si el server responde brotli y no está la lib, aiohttp revienta
    # y todo cae en NO_VERIFICADO.
    "Accept-Encoding": "gzip, deflate",
}


# ---------------- Auth ----------------
def get_client():
    raw = (os.environ.get("GCP_SA_JSON") or os.environ.get("GCP_SA_BASE64") or "").strip()
    info = json.loads(raw) if raw.startswith("{") else json.loads(
        base64.b64decode(raw).decode("utf-8"))
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds)


# ---------------- Feed ----------------
async def download_feed(session):
    async with session.get(FEED_URL, timeout=aiohttp.ClientTimeout(total=600)) as r:
        r.raise_for_status()
        data = await r.text()

    reader = csv.reader(io.StringIO(data), delimiter="\t")
    header = [h.strip().lower() for h in next(reader)]
    idx = {name: i for i, name in enumerate(header)}
    i_id, i_title = idx.get("id"), idx.get("title")
    i_link, i_img = idx.get("link"), idx.get("image_link")
    i_avail = idx.get("availability")

    if i_link is None:
        raise RuntimeError(f"No encontré columna 'link'. Cabeceras: {header}")

    def safe(row, i):
        return row[i].strip() if (i is not None and i < len(row)) else ""

    rows, sin_imagen = [], []
    for row in reader:
        if not row:
            continue
        pid, title = safe(row, i_id), safe(row, i_title)
        link, img = safe(row, i_link), safe(row, i_img)
        avail = safe(row, i_avail).lower()
        if i_img is not None and not img:
            sin_imagen.append({"tipo": "SIN_IMAGEN", "id": pid, "title": title,
                               "link": link, "url_final": "", "status": "",
                               "motivo": "image_link vacío en el feed"})
        rows.append((pid, title, link, avail))
    return rows, sin_imagen


# ---------------- Análisis de HTML ----------------
def _sin_tildes(s):
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


def texto_visible(html):
    t = html.lower()
    t = re.sub(r"<script\b[^>]*>.*?</script>", " ", t, flags=re.S)
    t = re.sub(r"<style\b[^>]*>.*?</style>", " ", t, flags=re.S)
    t = re.sub(r"<!--.*?-->", " ", t, flags=re.S)
    t = re.sub(r"<[^>]+>", " ", t)
    t = (t.replace("&nbsp;", " ").replace("&#160;", " ")
          .replace("&amp;", "&").replace("&quot;", '"'))
    return re.sub(r"\s+", " ", _sin_tildes(t)).strip()


def get_title(html):
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.S | re.I)
    return texto_visible(m.group(1)) if m else ""


def hubo_redirect(url_orig, url_final):
    a, b = urlsplit(url_orig), urlsplit(str(url_final))
    return a.path.rstrip("/").lower() != b.path.rstrip("/").lower()


def analizar_html(html, url_orig, url_final, avail_feed):
    crudo = html.lower()
    visible = texto_visible(html)
    titulo = get_title(html)
    ctx = titulo + " " + visible

    hit_raw = next((m for m in ERROR_MARKERS_RAW if m in crudo), None)
    hit_txt = next((m for m in ERROR_MARKERS_TXT if m in ctx), None)
    hit_pos = next((m for m in POSITIVE_MARKERS_TXT if m in visible), None)

    # 1. Página "Producto no encontrado" (llega con HTTP 200).
    if hit_raw or hit_txt:
        return {"tipo": "SOFT_404",
                "motivo": f"Página de producto no encontrado ({hit_raw or hit_txt})"}

    # 2. Redirect fuera del PDP.
    if hubo_redirect(url_orig, url_final) and not hit_pos:
        return {"tipo": "REDIRECT", "motivo": f"Redirige a {url_final}"}

    # 3. Listado con 0 productos.
    m = RE_CONTEO.search(visible)
    if m and int(m.group(1)) == 0:
        return {"tipo": "SIN_PRODUCTOS", "motivo": "Listado con 0 productos"}

    # 4. PDP vivo: contrastar stock contra lo que declara el feed.
    if hit_pos:
        hit_stock = next((s for s in SIN_STOCK_MARKERS if s in visible), None)
        if hit_stock:
            if avail_feed and "in" in avail_feed and "stock" in avail_feed:
                return {"tipo": "STOCK_DESALINEADO",
                        "motivo": f"Feed dice '{avail_feed}' pero la página dice '{hit_stock}'"}
            return {"tipo": "SIN_STOCK", "motivo": f"Página indica '{hit_stock}'"}
        return None

    # 5. Ni error ni prueba de PDP.
    return {"tipo": "REVISAR",
            "motivo": f"200 OK sin bloque de compra (title: {titulo[:60]})"}


# ---------------- HTTP check ----------------
def calc_espera(retry_after, intento):
    if retry_after:
        try:
            return min(float(retry_after), 30)
        except ValueError:
            pass
    return min((2 ** intento) + random.random(), 30)


async def check_one(session, sem, item, max_retries):
    pid, title, url, avail = item
    if not url:
        return None
    base = {"id": pid, "title": title, "link": url, "avail": avail, "url_final": ""}
    async with sem:
        for intento in range(max_retries + 1):
            try:
                async with session.get(url, allow_redirects=True,
                                       timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as r:
                    status = r.status
                    final = str(r.url)
                    base["url_final"] = final if final != url else ""

                    if status == 429:
                        if intento < max_retries:
                            await asyncio.sleep(
                                calc_espera(r.headers.get("Retry-After"), intento))
                            continue
                        return {**base, "tipo": "NO_VERIFICADO", "status": "429",
                                "motivo": "Rate limit — no se pudo verificar"}
                    if status >= 400:
                        return {**base, "tipo": "LINK_ROTO", "status": str(status),
                                "motivo": f"HTTP {status}"}

                    if "html" not in (r.headers.get("Content-Type") or "").lower():
                        return None

                    raw = await r.content.read(MAX_BYTES)
                    try:
                        html = raw.decode(r.charset or "utf-8", errors="ignore")
                    except LookupError:
                        html = raw.decode("utf-8", errors="ignore")

                    res = analizar_html(html, url, final, avail)
                    return {**base, "status": str(status), **res} if res else None

            except asyncio.TimeoutError:
                if intento < max_retries:
                    await asyncio.sleep(calc_espera(None, intento))
                    continue
                return {**base, "tipo": "NO_VERIFICADO", "status": "TIMEOUT",
                        "motivo": "No respondió (timeout)"}
            except Exception as e:
                return {**base, "tipo": "NO_VERIFICADO", "status": "ERROR",
                        "motivo": str(e)[:120]}
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


# ---------------- Sheets ----------------
def _pct(n, total):
    return f"{n / total * 100:.2f}%" if total else "—"


def fila(d):
    return [d["tipo"], d.get("id", ""), d.get("title", ""), d.get("link", ""),
            d.get("url_final", ""), d.get("status", ""), d.get("motivo", ""),
            ACCIONES.get(d["tipo"], "")]


def get_or_create_ws(sh, nombre, rows=500, cols=10):
    try:
        ws = sh.worksheet(nombre)
        ws.clear()
        # clear() NO redimensiona: si la hoja quedó chica de una corrida
        # anterior, update() falla con "exceeds grid limits".
        if ws.row_count < rows or ws.col_count < cols:
            ws.resize(rows=max(rows, ws.row_count), cols=max(cols, ws.col_count))
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=nombre, rows=rows, cols=cols)
    return ws


def escribir_shard(sh, shard_idx, filas_dict):
    nombre = f"_shard_{shard_idx}"
    ws = get_or_create_ws(sh, nombre, rows=max(len(filas_dict) + 5, 50))
    valores = [HEADER] + [fila(d) for d in filas_dict] if filas_dict else [HEADER]
    ws.update(values=valores, range_name="A1", value_input_option="RAW")
    print(f"  → Shard {shard_idx}: {len(filas_dict)} problemas → '{nombre}'")


def merge_shards(sh, shard_total, total_feed, sin_imagen, ts):
    todos = list(sin_imagen)
    encontrados, faltantes = 0, []

    for idx in range(shard_total):
        nombre = f"_shard_{idx}"
        try:
            ws = sh.worksheet(nombre)
            vals = ws.get_all_values()
            for row in vals[1:]:
                if len(row) >= 7:
                    todos.append({"tipo": row[0], "id": row[1], "title": row[2],
                                  "link": row[3], "url_final": row[4],
                                  "status": row[5], "motivo": row[6]})
            sh.del_worksheet(ws)
            encontrados += 1
            print(f"  Shard {idx} leído y eliminado ({len(vals)-1} filas)")
        except gspread.WorksheetNotFound:
            faltantes.append(idx)
            print(f"  AVISO: _shard_{idx} no encontrada — el shard pudo fallar")

    # Si no llegó NINGÚN shard, no tocar el Reporte: un reporte vacío se lee
    # como "todo sano" y borra el resultado bueno de la corrida anterior.
    if encontrados == 0:
        raise RuntimeError(
            f"Ningún shard produjo resultados ({shard_total} esperados). "
            "Se conserva el Reporte anterior. Revisar los logs de los jobs shard.")

    # Cobertura parcial: se escribe, pero queda marcado en el Sheet.
    cobertura = encontrados / shard_total
    aviso = ""
    if faltantes:
        aviso = (f"PARCIAL: faltaron los shards {faltantes} — "
                 f"solo se revisó ~{cobertura*100:.0f}% del feed")
        print(f"  {aviso}")

    todos.sort(key=lambda d: (TIPO_ORDEN.get(d["tipo"], 9), d.get("id", "")))

    conteos = {}
    for d in todos:
        conteos[d["tipo"]] = conteos.get(d["tipo"], 0) + 1

    ws_rep = get_or_create_ws(sh, WS_REPORTE, rows=max(len(todos) + 20, 200))
    ws_rep.update(values=[HEADER] + [fila(d) for d in todos],
                  range_name="A1", value_input_option="RAW")
    encabezado = f"Última ejecución: {ts:%Y-%m-%d %H:%M} (Lima) — {len(todos)} problemas"
    if aviso:
        encabezado = "⚠ " + aviso + " | " + encabezado
    ws_rep.update(values=[[encabezado]], range_name="J1", value_input_option="RAW")
    try:
        ws_rep.freeze(rows=1)
    except Exception:
        pass
    print(f"  → Reporte: {len(todos)} filas")

    filas_res = [
        ["Resumen de la última ejecución", "", "", ""],
        ["", "", "", ""],
        ["Fecha / hora (Lima)", f"{ts:%Y-%m-%d %H:%M}", "", ""],
        ["Productos en el feed", str(total_feed), "", ""],
        ["Shards utilizados", f"{encontrados} de {shard_total}", "", ""],
        ["Cobertura", f"{cobertura*100:.0f}%", aviso, ""],
        ["", "", "", ""],
        ["tipo_error", "cantidad", "% del feed", "qué significa"],
    ]
    for tipo in sorted(TIPO_ORDEN, key=lambda t: TIPO_ORDEN[t]):
        n = conteos.get(tipo, 0)
        filas_res.append([tipo, str(n), _pct(n, total_feed), QUE_SIGNIFICA.get(tipo, "")])
    filas_res += [["", "", "", ""],
                  ["TOTAL PROBLEMAS", str(sum(conteos.values())),
                   _pct(sum(conteos.values()), total_feed), ""]]

    ws_res = get_or_create_ws(sh, WS_RESUMEN, rows=30, cols=4)
    ws_res.update(values=filas_res, range_name="A1", value_input_option="RAW")
    print("  → Resumen escrito")
    return conteos


# ---------------- Jobs ----------------
async def run_merge():
    async with aiohttp.ClientSession(headers=HTTP_HEADERS) as session:
        rows, sin_imagen = await download_feed(session)

    total_feed = len(rows)
    print(f"Feed: {total_feed} productos | Sin imagen: {len(sin_imagen)}")

    sh = get_client().open_by_key(SHEET_ID)
    lima = dt.timezone(dt.timedelta(hours=-5))
    ts = dt.datetime.now(dt.timezone.utc).astimezone(lima)
    conteos = merge_shards(sh, SHARD_TOTAL, total_feed, sin_imagen, ts)

    print("\nMERGE COMPLETO:")
    for tipo, n in sorted(conteos.items(), key=lambda x: TIPO_ORDEN.get(x[0], 9)):
        print(f"  {tipo}: {n}")
    print(f"  TOTAL: {sum(conteos.values())} problemas")


async def run_shard():
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=CONCURRENCY,
                                     ttl_dns_cache=300)
    async with aiohttp.ClientSession(headers=HTTP_HEADERS, connector=connector) as session:
        rows, _ = await download_feed(session)
        total = len(rows)
        if LIMIT > 0:
            rows = rows[:LIMIT]

        chunk = rows[SHARD_INDEX::SHARD_TOTAL]
        print(f"Shard {SHARD_INDEX}/{SHARD_TOTAL} | feed: {total} | chunk: {len(chunk)}")

        print(f"Pasada 1 (concurrencia {CONCURRENCY})...")
        problemas, no_verif = await correr_tanda(session, chunk, CONCURRENCY,
                                                 MAX_RETRIES, "p1")
        print(f"  → problemas: {len(problemas)} | no verificados: {len(no_verif)}")

        if no_verif:
            print(f"Enfriando {COOLDOWN}s...")
            await asyncio.sleep(COOLDOWN)
            reintentar = [(d["id"], d["title"], d["link"], d.get("avail", ""))
                          for d in no_verif]
            print(f"Pasada 2 (concurrencia {CONCURRENCY_2}) sobre {len(reintentar)}...")
            prob2, no_verif = await correr_tanda(session, reintentar, CONCURRENCY_2,
                                                 MAX_RETRIES_2, "p2")
            problemas.extend(prob2)
            print(f"  → nuevos: {len(prob2)} | siguen sin verificar: {len(no_verif)}")

    todos = problemas + no_verif
    print(f"Shard {SHARD_INDEX} terminado: {len(todos)} problemas")

    if DEBUG:
        print("DEBUG: no se escribe al Sheet.")
        return

    escribir_shard(get_client().open_by_key(SHEET_ID), SHARD_INDEX, todos)


if __name__ == "__main__":
    asyncio.run(run_merge() if IS_MERGE else run_shard())
