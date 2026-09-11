import asyncio
import csv
import io
import ipaddress
import json
import os
import platform
import socket
import time
import warnings
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse, StreamingResponse

import httpx
import psutil
import psycopg
from psycopg.rows import dict_row

import fleet

warnings.filterwarnings("ignore", category=Warning, module="httpx")

try:
    from kubernetes import client as k8s, config as k8s_cfg

    k8s_cfg.load_incluster_config()
    _v1 = k8s.CoreV1Api()
    K8S = True
except Exception:
    K8S = False

BASE_DIR = Path(__file__).resolve().parent
DATABASE_URL = os.environ["DATABASE_URL"]
PHOTOS_DIR = Path(os.getenv("PHOTO_DIR", str(BASE_DIR / "photos")))

DEFAULT_PVE_IPS: list[str] = []

HISTORY_WINDOWS = {
    "day": {"seconds": 24 * 3600, "bucket": 5 * 60},
    "week": {"seconds": 7 * 24 * 3600, "bucket": 3600},
    "month": {"seconds": 30 * 24 * 3600, "bucket": 6 * 3600},
}

WEATHER_CODES = {
    0: "Clear",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Rime fog",
    51: "Light drizzle",
    53: "Drizzle",
    55: "Dense drizzle",
    56: "Freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Light rain",
    63: "Rain",
    65: "Heavy rain",
    66: "Freezing rain",
    67: "Heavy freezing rain",
    71: "Light snow",
    73: "Snow",
    75: "Heavy snow",
    77: "Snow grains",
    80: "Rain showers",
    81: "Heavy showers",
    82: "Violent showers",
    85: "Snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm and hail",
    99: "Severe thunderstorm and hail",
}

DEFAULT_CHANGELOG = [
    {
        "date": "2026-04-22",
        "title": "History and console rollout",
        "body": "Added SQLite-backed history, hall-of-fame summaries, CSV export, and a live request console with SSE.",
    },
    {
        "date": "2026-04-22",
        "title": "Weather and guestbook support",
        "body": "Added cached outdoor weather fetches plus guestbook submission, moderation, and rate limiting.",
    },
    {
        "date": "2026-04-22",
        "title": "Cluster-focused cleanup",
        "body": "Moved Proxmox access into environment config and cleaned up the live dashboard links and empty states.",
    },
]


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, "")
    if not raw.strip():
        return default
    values = [value.strip() for value in raw.split(",")]
    return [value for value in values if value] or default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except ValueError:
        return default


def _env_json(name: str, default):
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


PVE_TOKEN = os.getenv("PVE_TOKEN", "").strip()
PVE_IPS = _env_list("PVE_IPS", DEFAULT_PVE_IPS)
PVE_TIMEOUT = _env_float("PVE_TIMEOUT", 4.0)
PVE_HDR = {"Authorization": f"PVEAPIToken={PVE_TOKEN}"} if PVE_TOKEN else {}
PVE_SSH_USER = os.getenv("PVE_SSH_USER", "root").strip() or "root"
PVE_SSH_PASS = os.getenv("PVE_SSH_PASS", "").strip()
K8S_NODE_NAME = os.getenv("K8S_NODE_NAME", "").strip()
LIVE_INTERVAL_SECONDS = max(_env_int("LIVE_INTERVAL_SECONDS", 5), 1)
SNAPSHOT_INTERVAL_SECONDS = max(_env_int("SNAPSHOT_INTERVAL_SECONDS", 60), LIVE_INTERVAL_SECONDS)
CONSOLE_LIMIT = max(_env_int("CONSOLE_LIMIT", 50), 10)
WEATHER_PROXY_URL = os.getenv("WEATHER_PROXY_URL", "").strip()
WEATHER_LAT = os.getenv("WEATHER_LAT", "").strip()
WEATHER_LON = os.getenv("WEATHER_LON", "").strip()
WEATHER_LABEL = os.getenv("WEATHER_LABEL", "Outdoor weather").strip() or "Outdoor weather"
WEATHER_CACHE_SECONDS = max(_env_int("WEATHER_CACHE_SECONDS", 600), 60)
GUESTBOOK_RATE_LIMIT_SECONDS = max(_env_int("GUESTBOOK_RATE_LIMIT_SECONDS", 300), 10)
GUESTBOOK_MAX_NAME = max(_env_int("GUESTBOOK_MAX_NAME", 40), 8)
GUESTBOOK_MAX_MESSAGE = max(_env_int("GUESTBOOK_MAX_MESSAGE", 280), 80)
GUESTBOOK_PUBLIC_LIMIT = max(_env_int("GUESTBOOK_PUBLIC_LIMIT", 12), 4)
MOD_TOKEN = os.getenv("GUESTBOOK_MOD_TOKEN", "").strip()
SITE_CHANGELOG = _env_json("SITE_CHANGELOG_JSON", DEFAULT_CHANGELOG)

_req_count = 0
_cache: dict = {}
_metrics_clients: set = set()
_console_clients: set = set()
_loop_task: asyncio.Task | None = None
_flush_task: asyncio.Task | None = None
_last_snapshot_write = 0
_weather_cache: dict = {"configured": False}
_weather_cache_ts = 0
_weather_lock = asyncio.Lock()
_req_event_buffer: list = []
HISTORY_SUMMARY_CACHE_SECONDS = 60
HISTORY_SERIES_CACHE_SECONDS = 30
_history_summary_cache: dict | None = None
_history_summary_cache_ts: float = 0.0
_history_series_cache: dict = {}
_history_series_cache_ts: dict = {}
_pve_vm_ip_map: dict = {}
_pve_vm_ip_map_ts: float = 0.0
PVE_VM_IP_CACHE_SECONDS = 300
_pve_node_ip_map: dict = {}
_pve_node_ip_map_ts: float = 0.0
PVE_NODE_IP_CACHE_SECONDS = 300
_pve_specs_map: dict = {}
_pve_specs_map_ts: float = 0.0
PVE_SPECS_CACHE_SECONDS = 900
_pve_ssh_temp_cache: dict = {}
PVE_SSH_TEMP_CACHE_SECONDS = 30

# ── public incident tracking ──
INCIDENT_TEMP_C = _env_float("INCIDENT_TEMP_C", 85.0)
INCIDENT_CONFIRM_TICKS = max(_env_int("INCIDENT_CONFIRM_TICKS", 2), 1)
_open_incidents: dict[tuple, dict] = {}
_incident_flap: dict[tuple, int] = {}
_incident_backlog: list[dict] = []

# ── per-IP API rate limiting (fixed 60s window, per pod) ──
RATE_LIMIT_PER_MIN = max(_env_int("RATE_LIMIT_PER_MIN", 120), 10)
_rate_buckets: dict[str, list] = {}
_RATE_EXEMPT_PREFIXES = ("/api/fleet/v1/ingest", "/api/fleet/v1/register", "/api/host")


def _db_connect():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True, connect_timeout=5)


def _ddl(cur, sql):
    try:
        cur.execute(sql)
    except psycopg.errors.UniqueViolation:
        pass


def _setup_db_schema():
    with _db_connect() as conn, conn.cursor() as cur:
        _ddl(cur,
            """CREATE TABLE IF NOT EXISTS metric_snapshots (
                id BIGSERIAL PRIMARY KEY,
                ts BIGINT NOT NULL, host TEXT NOT NULL, uptime BIGINT NOT NULL,
                cpu_pct DOUBLE PRECISION NOT NULL, mem_pct DOUBLE PRECISION NOT NULL, mem_used BIGINT NOT NULL,
                mem_total BIGINT NOT NULL, disk_pct DOUBLE PRECISION NOT NULL, disk_used BIGINT NOT NULL,
                disk_total BIGINT NOT NULL, net_in BIGINT NOT NULL, net_out BIGINT NOT NULL,
                temp DOUBLE PRECISION, requests BIGINT NOT NULL, cluster_nodes_ready INTEGER NOT NULL,
                cluster_nodes_total INTEGER NOT NULL, cluster_pods INTEGER NOT NULL,
                pve_online INTEGER, pve_total INTEGER, pve_cpu DOUBLE PRECISION, pve_mem_pct DOUBLE PRECISION, pve_avg_temp DOUBLE PRECISION
            )"""
        )
        _ddl(cur,
            """CREATE TABLE IF NOT EXISTS request_events (
                id BIGSERIAL PRIMARY KEY,
                ts BIGINT NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL,
                client_ip TEXT NOT NULL, country_code TEXT, user_agent TEXT
            )"""
        )
        _ddl(cur,
            """CREATE TABLE IF NOT EXISTS guestbook_entries (
                id BIGSERIAL PRIMARY KEY,
                ts BIGINT NOT NULL, name TEXT NOT NULL, message TEXT NOT NULL,
                client_ip TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                moderated_ts BIGINT, moderated_by TEXT
            )"""
        )
        _ddl(cur,
            """CREATE TABLE IF NOT EXISTS public_incidents (
                id BIGSERIAL PRIMARY KEY,
                type TEXT NOT NULL, target TEXT NOT NULL, target_real TEXT,
                detail TEXT, started_ts BIGINT NOT NULL, ended_ts BIGINT
            )"""
        )
        _ddl(cur, "CREATE INDEX IF NOT EXISTS idx_public_incidents_started ON public_incidents(started_ts)")
        _ddl(cur, "CREATE INDEX IF NOT EXISTS idx_metric_snapshots_ts ON metric_snapshots(ts)")
        _ddl(cur, "CREATE INDEX IF NOT EXISTS idx_request_events_ts ON request_events(ts)")
        _ddl(cur, "CREATE INDEX IF NOT EXISTS idx_guestbook_entries_ts ON guestbook_entries(ts)")
        _ddl(cur, "CREATE INDEX IF NOT EXISTS idx_guestbook_entries_status ON guestbook_entries(status, ts)")


def _init_db(deadline_seconds: float = 60) -> bool:
    """Set up the schema. Returns True on success, False if the DB is not
    reachable in time.

    Deliberately does NOT raise. Raising here failed FastAPI's lifespan and
    exited the container, so a brief DNS or Postgres hiccup turned into a
    restart loop -- the monitoring service became the first casualty of the
    outage it exists to report. Startup now continues and _db_retry_loop keeps
    trying in the background; DB-backed endpoints degrade until it connects.
    """
    deadline = time.time() + deadline_seconds
    last_exc = None
    while time.time() < deadline:
        try:
            _setup_db_schema()
            return True
        except Exception as exc:
            last_exc = exc
            print(f"DB init waiting ({exc})", flush=True)
            time.sleep(2)
    print(f"DB unreachable, starting without it: {last_exc}", flush=True)
    return False


async def _db_retry_loop():
    """Keep trying to reach the database, forever, without blocking serving."""
    global _req_count
    while True:
        await asyncio.sleep(15)
        if await asyncio.to_thread(_init_db, 5):
            print("DB reachable; schema ready", flush=True)
            with suppress(Exception):
                fleet.setup_schema()
            with suppress(Exception):
                _req_count = await asyncio.to_thread(_db_total_requests)
            return




def _db_total_requests() -> int:
    with _db_connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM request_events").fetchone()
        return int(row["total"] if row else 0)


def _db_insert_snapshot(snapshot: dict):
    pve_total = snapshot.get("pve", {}).get("total", {})
    cluster = snapshot.get("cluster", {})
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO metric_snapshots (
                ts, host, uptime, cpu_pct, mem_pct, mem_used, mem_total,
                disk_pct, disk_used, disk_total, net_in, net_out, temp,
                requests, cluster_nodes_ready, cluster_nodes_total, cluster_pods,
                pve_online, pve_total, pve_cpu, pve_mem_pct, pve_avg_temp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                snapshot["ts"],
                snapshot["host"],
                snapshot["uptime"],
                snapshot["cpu"]["pct"],
                snapshot["mem"]["pct"],
                snapshot["mem"]["used"],
                snapshot["mem"]["total"],
                snapshot["disk"]["pct"],
                snapshot["disk"]["used"],
                snapshot["disk"]["total"],
                snapshot["net"]["in"],
                snapshot["net"]["out"],
                snapshot["temp"],
                snapshot["requests"],
                cluster.get("nodes_ready", 0),
                cluster.get("nodes_total", 0),
                cluster.get("pods", 0),
                pve_total.get("online"),
                pve_total.get("total"),
                pve_total.get("cpu"),
                pve_total.get("mem_pct"),
                pve_total.get("avg_temp"),
            ),
        )


def _db_insert_request(event: dict):
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO request_events (ts, method, path, client_ip, country_code, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                event["ts"],
                event["method"],
                event["path"],
                event["client_ip"],
                event.get("country_code"),
                event.get("user_agent"),
            ),
        )


def _db_insert_requests_batch(events: list[dict]):
    with _db_connect() as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO request_events (ts, method, path, client_ip, country_code, user_agent) VALUES (%s, %s, %s, %s, %s, %s)",
            [
                (e["ts"], e["method"], e["path"], e["client_ip"], e.get("country_code"), e.get("user_agent"))
                for e in events
            ],
        )


def _serialize_request_row(row: dict) -> dict:
    return {
        "id": row["id"],
        "ts": row["ts"],
        "method": row["method"],
        "path": row["path"],
        "client_ip": _mask_ip(row["client_ip"]),
        "country_code": row["country_code"],
        "user_agent": row["user_agent"],
    }


def _db_recent_requests(limit: int) -> list[dict]:
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT id, ts, method, path, client_ip, country_code, user_agent
            FROM request_events
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [_serialize_request_row(row) for row in rows]


def _snapshot_point_from_live(snapshot: dict) -> dict:
    pve_total = snapshot.get("pve", {}).get("total", {})
    return {
        "ts": snapshot["ts"],
        "cpu_pct": snapshot["cpu"]["pct"],
        "mem_pct": snapshot["mem"]["pct"],
        "disk_pct": snapshot["disk"]["pct"],
        "temp": snapshot["temp"],
        "net_in": snapshot["net"]["in"],
        "net_out": snapshot["net"]["out"],
        "cluster_pods": snapshot["cluster"]["pods"],
        "requests": snapshot["requests"],
        "pve_cpu": pve_total.get("cpu"),
        "pve_mem_pct": pve_total.get("mem_pct"),
    }


def _db_history_points(window: str) -> dict:
    cfg = HISTORY_WINDOWS.get(window, HISTORY_WINDOWS["day"])
    cutoff = int(time.time()) - cfg["seconds"]
    bucket = cfg["bucket"]
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT
                ((ts / %s)::bigint) * %s AS bucket_ts,
                ROUND(AVG(cpu_pct)::numeric, 1)::float AS cpu_pct,
                ROUND(AVG(mem_pct)::numeric, 1)::float AS mem_pct,
                ROUND(AVG(disk_pct)::numeric, 1)::float AS disk_pct,
                ROUND(AVG(COALESCE(temp, pve_avg_temp))::numeric, 1)::float AS temp,
                ROUND(AVG(net_in)::numeric, 0)::bigint AS net_in,
                ROUND(AVG(net_out)::numeric, 0)::bigint AS net_out,
                ROUND(AVG(cluster_pods)::numeric, 1)::float AS cluster_pods,
                MAX(requests) AS requests,
                ROUND(AVG(pve_cpu)::numeric, 1)::float AS pve_cpu,
                ROUND(AVG(pve_mem_pct)::numeric, 1)::float AS pve_mem_pct
            FROM metric_snapshots
            WHERE ts >= %s
            GROUP BY bucket_ts
            ORDER BY bucket_ts
            """,
            (bucket, bucket, cutoff),
        ).fetchall()

    points = [
        {
            "ts": row["bucket_ts"],
            "cpu_pct": row["cpu_pct"],
            "mem_pct": row["mem_pct"],
            "disk_pct": row["disk_pct"],
            "temp": row["temp"],
            "net_in": row["net_in"],
            "net_out": row["net_out"],
            "cluster_pods": row["cluster_pods"],
            "requests": row["requests"],
            "pve_cpu": row["pve_cpu"],
            "pve_mem_pct": row["pve_mem_pct"],
        }
        for row in rows
    ]

    if not points and _cache:
        points = [_snapshot_point_from_live(_cache)]

    return {"window": window, "bucket_seconds": bucket, "points": points}


def _hall_of_fame_entry(label: str, value, unit: str, stamp: int | None, detail: str | None = None) -> dict:
    return {"label": label, "value": value, "unit": unit, "ts": stamp, "detail": detail}


def _shift_months(dt: datetime, months: int) -> datetime:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    return dt.replace(year=year, month=month, day=1)


def _db_request_rate() -> list[dict]:
    now = int(time.time())
    bucket = 3600
    cutoff = now - 24 * 3600
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT ((ts / %s)::bigint) * %s AS bucket_ts, COUNT(*) AS hits
            FROM request_events
            WHERE ts >= %s
            GROUP BY bucket_ts
            ORDER BY bucket_ts
            """,
            (bucket, bucket, cutoff),
        ).fetchall()
    hits_by_bucket = {row["bucket_ts"]: row["hits"] for row in rows}
    start = cutoff - (cutoff % bucket)
    return [{"ts": ts, "hits": int(hits_by_bucket.get(ts, 0))} for ts in range(start, now + 1, bucket)]


def _db_visitor_countries(limit: int = 12) -> dict:
    with _db_connect() as conn:
        total_row = conn.execute(
            "SELECT COUNT(*) AS total FROM request_events WHERE country_code IS NOT NULL AND country_code != ''"
        ).fetchone()
        rows = conn.execute(
            """
            SELECT country_code, COUNT(*) AS hits
            FROM request_events
            WHERE country_code IS NOT NULL AND country_code != ''
            GROUP BY country_code
            ORDER BY hits DESC, country_code ASC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
        distinct_row = conn.execute(
            "SELECT COUNT(DISTINCT country_code) AS total FROM request_events WHERE country_code IS NOT NULL AND country_code != ''"
        ).fetchone()
    total = int(total_row["total"] if total_row else 0)
    return {
        "total_hits": total,
        "distinct_countries": int(distinct_row["total"] if distinct_row else 0),
        "countries": [
            {
                "country_code": row["country_code"],
                "hits": int(row["hits"]),
                "share": round(int(row["hits"]) / max(total, 1) * 100, 1),
            }
            for row in rows
        ],
    }


def _photo_placeholder(title: str, subtitle: str, accent: str) -> str:
    svg = f"""
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 700">
      <defs>
        <linearGradient id="g" x1="0" x2="1" y1="0" y2="1">
          <stop offset="0%" stop-color="#08111d"/>
          <stop offset="100%" stop-color="{accent}"/>
        </linearGradient>
      </defs>
      <rect width="1200" height="700" fill="url(#g)"/>
      <circle cx="1020" cy="120" r="140" fill="rgba(255,255,255,0.08)"/>
      <circle cx="240" cy="610" r="180" fill="rgba(255,255,255,0.06)"/>
      <g stroke="rgba(255,255,255,0.14)" stroke-width="2" fill="none">
        <path d="M100 520h1000"/>
        <path d="M100 420h1000"/>
        <path d="M100 320h1000"/>
      </g>
      <text x="90" y="180" fill="white" font-family="system-ui, sans-serif" font-size="78" font-weight="700">{title}</text>
      <text x="94" y="246" fill="rgba(255,255,255,0.8)" font-family="system-ui, sans-serif" font-size="28">{subtitle}</text>
      <rect x="92" y="412" width="240" height="150" rx="26" fill="rgba(255,255,255,0.08)" stroke="rgba(255,255,255,0.12)"/>
      <rect x="372" y="360" width="280" height="202" rx="26" fill="rgba(255,255,255,0.1)" stroke="rgba(255,255,255,0.14)"/>
      <rect x="692" y="280" width="416" height="282" rx="30" fill="rgba(255,255,255,0.12)" stroke="rgba(255,255,255,0.18)"/>
    </svg>
    """
    return f"data:image/svg+xml,{quote(svg.strip())}"


def _site_photos() -> list[dict]:
    photos = []
    if PHOTOS_DIR.exists():
        for path in sorted(PHOTOS_DIR.iterdir()):
            if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                photos.append(
                    {
                        "title": path.stem.replace("-", " ").replace("_", " ").title(),
                        "caption": "Local photo asset",
                        "src": f"/media/{path.name}",
                    }
                )
    if photos:
        return photos[:12]
    return [
        {
            "title": "Cluster racks",
            "caption": "Drop real rack photos into /home/miles/Stats/photos to replace the placeholders.",
            "src": _photo_placeholder("Cluster racks", "Default placeholder until local photos are added.", "#4f8ef7"),
        },
        {
            "title": "Node line-up",
            "caption": "Designed to keep the carousel working before image assets exist.",
            "src": _photo_placeholder("Node line-up", "Five Proxmox nodes and the K3s control plane.", "#2dd4bf"),
        },
        {
            "title": "Live console",
            "caption": "SSE-fed request history inspired by HelloESP's public feed.",
            "src": _photo_placeholder("Live console", "Traffic, history, and guestbook all in one surface.", "#f59e0b"),
        },
    ]


def _site_changelog() -> list[dict]:
    if isinstance(SITE_CHANGELOG, list) and SITE_CHANGELOG:
        cleaned = []
        for item in SITE_CHANGELOG:
            if not isinstance(item, dict):
                continue
            cleaned.append(
                {
                    "date": str(item.get("date", "")).strip() or "unknown",
                    "title": str(item.get("title", "")).strip() or "Update",
                    "body": str(item.get("body", "")).strip() or "",
                }
            )
        if cleaned:
            return cleaned[:10]
    return DEFAULT_CHANGELOG


def _serialize_guestbook_row(row: dict) -> dict:
    return {
        "id": row["id"],
        "ts": row["ts"],
        "name": row["name"],
        "message": row["message"],
        "status": row["status"],
    }


def _serialize_guestbook_mod_row(row: dict) -> dict:
    entry = _serialize_guestbook_row(row)
    entry["client_ip"] = row["client_ip"]
    return entry


def _db_guestbook_public(limit: int) -> list[dict]:
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT id, ts, name, message, status, client_ip
            FROM guestbook_entries
            WHERE status = 'approved'
            ORDER BY ts DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [_serialize_guestbook_row(row) for row in rows]


def _db_guestbook_pending(limit: int) -> list[dict]:
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT id, ts, name, message, status, client_ip
            FROM guestbook_entries
            WHERE status = 'pending'
            ORDER BY ts ASC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [_serialize_guestbook_mod_row(row) for row in rows]


def _db_guestbook_last_submit_ts(client_ip: str) -> int | None:
    with _db_connect() as conn:
        row = conn.execute(
            "SELECT ts FROM guestbook_entries WHERE client_ip = %s ORDER BY ts DESC LIMIT 1",
            (client_ip,),
        ).fetchone()
    return int(row["ts"]) if row else None


def _db_guestbook_submit(entry: dict) -> int:
    with _db_connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO guestbook_entries (ts, name, message, client_ip, status)
            VALUES (%s, %s, %s, %s, 'pending')
            RETURNING id
            """,
            (entry["ts"], entry["name"], entry["message"], entry["client_ip"]),
        )
        return int(cur.fetchone()["id"])


def _db_guestbook_moderate(entry_id: int, action: str, moderator: str) -> bool:
    status = "approved" if action == "approve" else "rejected"
    with _db_connect() as conn:
        cur = conn.execute(
            """
            UPDATE guestbook_entries
            SET status = %s, moderated_ts = %s, moderated_by = %s
            WHERE id = %s AND status = 'pending'
            """,
            (status, int(time.time()), moderator, entry_id),
        )
        return cur.rowcount > 0


def _db_guestbook_counts() -> dict:
    with _db_connect() as conn:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending
            FROM guestbook_entries
            """
        ).fetchone()
    return {
        "approved": int(row["approved"] or 0) if row else 0,
        "pending": int(row["pending"] or 0) if row else 0,
    }


def _db_history_summary() -> dict:
    with _db_connect() as conn:
        peak_temp = conn.execute(
            "SELECT ts, temp FROM metric_snapshots WHERE temp IS NOT NULL ORDER BY temp DESC, ts DESC LIMIT 1"
        ).fetchone()
        temp_bounds = conn.execute(
            "SELECT MIN(temp) AS low, MAX(temp) AS high FROM metric_snapshots WHERE temp IS NOT NULL"
        ).fetchone()
        peak_cpu = conn.execute(
            "SELECT ts, cpu_pct FROM metric_snapshots ORDER BY cpu_pct DESC, ts DESC LIMIT 1"
        ).fetchone()
        peak_pods = conn.execute(
            "SELECT ts, cluster_pods FROM metric_snapshots ORDER BY cluster_pods DESC, ts DESC LIMIT 1"
        ).fetchone()
        longest_uptime = conn.execute(
            "SELECT ts, uptime FROM metric_snapshots ORDER BY uptime DESC, ts DESC LIMIT 1"
        ).fetchone()
        busiest_day = conn.execute(
            """
            SELECT
                to_char(to_timestamp(ts), 'YYYY-MM-DD') AS day,
                COUNT(*) AS hits
            FROM request_events
            GROUP BY day
            ORDER BY hits DESC, day DESC
            LIMIT 1
            """
        ).fetchone()
        monthly_counts = conn.execute(
            """
            SELECT
                to_char(to_timestamp(ts), 'YYYY-MM') AS month,
                COUNT(*) AS hits
            FROM request_events
            GROUP BY month
            ORDER BY month
            """
        ).fetchall()
        stats = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM metric_snapshots) AS snapshots,
                (SELECT COUNT(*) FROM request_events) AS requests
            """
        ).fetchone()

    month_map = {row["month"]: row["hits"] for row in monthly_counts}
    now = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    monthly_deltas = []
    for offset in range(11, -1, -1):
        current = _shift_months(now, -offset)
        previous = current.replace(year=current.year - 1)
        current_key = current.strftime("%Y-%m")
        previous_key = previous.strftime("%Y-%m")
        current_hits = int(month_map.get(current_key, 0))
        previous_hits = int(month_map.get(previous_key, 0))
        monthly_deltas.append(
            {
                "month": current_key,
                "label": current.strftime("%b %Y"),
                "hits": current_hits,
                "previous_hits": previous_hits,
                "delta": current_hits - previous_hits,
            }
        )

    hall = []
    if peak_temp:
        hall.append(_hall_of_fame_entry("Peak host temp", peak_temp["temp"], "C", peak_temp["ts"]))
    if temp_bounds and temp_bounds["low"] is not None and temp_bounds["high"] is not None:
        hall.append(
            _hall_of_fame_entry(
                "Temp range",
                round(temp_bounds["high"] - temp_bounds["low"], 1),
                "C",
                None,
                f"{temp_bounds['low']:.1f} C to {temp_bounds['high']:.1f} C",
            )
        )
    if peak_cpu:
        hall.append(_hall_of_fame_entry("CPU spike", peak_cpu["cpu_pct"], "%", peak_cpu["ts"]))
    if peak_pods:
        hall.append(_hall_of_fame_entry("Most running pods", peak_pods["cluster_pods"], "pods", peak_pods["ts"]))
    if longest_uptime:
        hall.append(_hall_of_fame_entry("Longest uptime", longest_uptime["uptime"], "seconds", longest_uptime["ts"]))
    if busiest_day:
        hall.append(_hall_of_fame_entry("Busiest day", busiest_day["hits"], "hits", None, busiest_day["day"]))

    return {
        "hall_of_fame": hall,
        "monthly_deltas": monthly_deltas,
        "totals": {
            "snapshots": int(stats["snapshots"] if stats else 0),
            "requests": int(stats["requests"] if stats else 0),
        },
        "visitor_geography": _db_visitor_countries(),
        "request_rate": _db_request_rate(),
        "guestbook": _db_guestbook_counts(),
        "changelog": _site_changelog(),
        "photos": _site_photos(),
    }


def _sse_message(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


async def _broadcast(clients: set, message: str):
    dead = set()
    for queue in list(clients):
        try:
            queue.put_nowait(message)
        except Exception:
            dead.add(queue)
    clients.difference_update(dead)


def _should_track_request(request: Request, status_code: int) -> bool:
    path = request.url.path
    if request.method != "GET" or status_code >= 500:
        return False
    if path.startswith("/api/") or path.startswith("/media/"):
        return False
    ua = request.headers.get("user-agent", "")
    if ua.startswith("kube-probe/"):
        return False
    return path in {"/", "/history", "/console"}


def _request_country_code(request: Request) -> str | None:
    for header in ("cf-ipcountry", "x-vercel-ip-country", "x-country-code"):
        code = request.headers.get(header, "").strip().upper()
        if code and code not in {"XX", "T1"}:
            return code
    return None


_TRUSTED_PROXY_NETS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
]


def _peer_is_trusted(host: str | None) -> bool:
    if not host:
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return any(ip in net for net in _TRUSTED_PROXY_NETS)


def _request_client_ip(request: Request) -> str:
    peer = request.client.host if request.client else None
    if _peer_is_trusted(peer):
        for header in ("cf-connecting-ip", "x-real-ip", "x-forwarded-for"):
            value = request.headers.get(header, "").strip()
            if value:
                return value.split(",")[0].strip()
    if peer:
        return peer
    return "unknown"


def _mask_ip(ip: str) -> str:
    if not ip or ip == "unknown":
        return "unknown"
    if ":" in ip:
        parts = [part for part in ip.split(":") if part]
        return f"{':'.join(parts[:2])}::" if parts else "::"
    parts = ip.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.{parts[2]}.x"
    return ip


def _request_event(request: Request) -> dict:
    return {
        "ts": int(time.time()),
        "method": request.method,
        "path": request.url.path,
        "client_ip": _request_client_ip(request),
        "country_code": _request_country_code(request),
        "user_agent": (request.headers.get("user-agent", "") or "")[:240],
    }


async def _flush_loop():
    global _req_event_buffer
    while True:
        await asyncio.sleep(0.5)
        if not _req_event_buffer:
            continue
        batch = _req_event_buffer[:]
        del _req_event_buffer[:]
        try:
            await asyncio.to_thread(_db_insert_requests_batch, batch)
        except Exception as exc:
            print(f"flush error: {exc}", flush=True)


async def _record_request(event: dict):
    _req_event_buffer.append(event)
    public_event = dict(event)
    public_event["client_ip"] = _mask_ip(event["client_ip"])
    await _broadcast(_console_clients, _sse_message(public_event))


def _sanitize_guestbook_text(value: str, max_len: int) -> str:
    cleaned = " ".join((value or "").strip().split())
    return cleaned[:max_len]


def _moderation_token(request: Request) -> str:
    return request.headers.get("x-mod-token", "").strip()


def _require_mod_token(request: Request) -> str:
    if not MOD_TOKEN:
        raise HTTPException(status_code=503, detail="Moderation is not configured.")
    provided = _moderation_token(request)
    if provided != MOD_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid moderation token.")
    return "moderator"


def _weather_params() -> dict:
    params = {
        "current": "temperature_2m,apparent_temperature,relative_humidity_2m,weather_code,is_day,wind_speed_10m",
        "timezone": "auto",
    }
    if WEATHER_LAT and WEATHER_LON:
        params["latitude"] = WEATHER_LAT
        params["longitude"] = WEATHER_LON
    return params


def _normalize_weather(payload: dict, source: str) -> dict:
    current = payload.get("current") or payload.get("current_weather") or payload
    if not isinstance(current, dict):
        return {"configured": False, "label": WEATHER_LABEL}

    code = current.get("weather_code")
    if code is None and "weathercode" in current:
        code = current.get("weathercode")
    temp = current.get("temperature_2m")
    if temp is None and "temperature" in current:
        temp = current.get("temperature")
    wind = current.get("wind_speed_10m")
    if wind is None and "windspeed" in current:
        wind = current.get("windspeed")
    humidity = current.get("relative_humidity_2m")
    apparent = current.get("apparent_temperature")
    is_day = current.get("is_day")

    return {
        "configured": True,
        "label": WEATHER_LABEL,
        "source": source,
        "temperature_c": temp,
        "apparent_c": apparent,
        "humidity_pct": humidity,
        "wind_kph": wind,
        "code": code,
        "summary": WEATHER_CODES.get(int(code), "Weather") if code is not None else "Weather",
        "is_day": is_day,
        "ts": int(time.time()),
    }


async def _fetch_weather() -> dict:
    global _weather_cache, _weather_cache_ts

    async with _weather_lock:
        now = time.time()
        if now - _weather_cache_ts < WEATHER_CACHE_SECONDS and _weather_cache:
            return _weather_cache

        try:
            params = _weather_params()
            if WEATHER_PROXY_URL:
                async with httpx.AsyncClient(timeout=8) as client:
                    response = await client.get(WEATHER_PROXY_URL, params=params)
            elif WEATHER_LAT and WEATHER_LON:
                async with httpx.AsyncClient(timeout=8) as client:
                    response = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
            else:
                _weather_cache = {"configured": False, "label": WEATHER_LABEL}
                _weather_cache_ts = now
                return _weather_cache

            response.raise_for_status()
            _weather_cache = _normalize_weather(
                response.json(),
                "proxy" if WEATHER_PROXY_URL else "open-meteo",
            )
        except Exception as exc:
            print(f"weather fetch error: {exc}", flush=True)
            _weather_cache = {
                "configured": bool(WEATHER_PROXY_URL or (WEATHER_LAT and WEATHER_LON)),
                "label": WEATHER_LABEL,
                "error": "Weather fetch failed.",
            }
        _weather_cache_ts = now
        return _weather_cache


def _safe_photo_path(filename: str) -> Path:
    candidate = (PHOTOS_DIR / filename).resolve()
    if not str(candidate).startswith(str(PHOTOS_DIR.resolve())):
        raise HTTPException(status_code=404, detail="Not found")
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return candidate


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _req_count, _loop_task, _flush_task

    # to_thread: _init_db sleeps between retries, and doing that on the event
    # loop stalled every request -- including the liveness probe on "/".
    db_ready = await asyncio.to_thread(_init_db, 20)
    with suppress(Exception):
        fleet.setup_schema()
    _req_count = 0
    with suppress(Exception):
        _req_count = await asyncio.to_thread(_db_total_requests)
    if not db_ready:
        asyncio.create_task(_db_retry_loop())
    _loop_task = asyncio.create_task(_loop())
    _flush_task = asyncio.create_task(_flush_loop())
    fleet_alert_task = asyncio.create_task(fleet.alert_loop())
    fleet_retention_task = asyncio.create_task(fleet.retention_loop())
    try:
        yield
    finally:
        for task in (_loop_task, _flush_task, fleet_alert_task, fleet_retention_task):
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        if _req_event_buffer:
            with suppress(Exception):
                _db_insert_requests_batch(_req_event_buffer)


app = FastAPI(lifespan=lifespan)
app.include_router(fleet.router)
app.include_router(fleet.pages_router)


_CSP = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://unpkg.com; "
    "style-src 'self' 'unsafe-inline' https://unpkg.com; "
    "img-src 'self' data: blob: https://unpkg.com https://tile.openstreetmap.org https://*.tile.openstreetmap.org; "
    "connect-src 'self'; font-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'self'"
)


@app.middleware("http")
async def _security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("Strict-Transport-Security", "max-age=15552000; includeSubDomains")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    response.headers.setdefault("Content-Security-Policy", _CSP)
    return response


@app.middleware("http")
async def _rate_limit(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api/") and not any(path.startswith(p) for p in _RATE_EXEMPT_PREFIXES):
        ip = _request_client_ip(request)
        minute = int(time.time() // 60)
        bucket = _rate_buckets.get(ip)
        if bucket and bucket[0] == minute:
            bucket[1] += 1
            if bucket[1] > RATE_LIMIT_PER_MIN:
                return PlainTextResponse(
                    "rate limited",
                    status_code=429,
                    headers={"Retry-After": str(60 - int(time.time() % 60) or 1)},
                )
        else:
            _rate_buckets[ip] = [minute, 1]
            if len(_rate_buckets) > 4096:
                for stale_ip in [k for k, v in _rate_buckets.items() if v[0] != minute]:
                    _rate_buckets.pop(stale_ip, None)
    return await call_next(request)


@app.middleware("http")
async def _count(request: Request, call_next):
    global _req_count

    response = await call_next(request)
    if _should_track_request(request, response.status_code):
        _req_count += 1
        asyncio.create_task(_record_request(_request_event(request)))
    return response


def _cpu_temp():
    try:
        temps = psutil.sensors_temperatures()
        for key in ("coretemp", "k10temp", "cpu_thermal", "acpitz"):
            if key in temps and temps[key]:
                return round(max(item.current for item in temps[key]), 1)
    except Exception:
        pass
    for index in range(8):
        try:
            with open(f"/sys/class/thermal/thermal_zone{index}/temp") as handle:
                return round(int(handle.read().strip()) / 1000, 1)
        except Exception:
            continue
    return None


_host_specs_cache: dict | None = None


def _host_specs() -> dict:
    """Static hardware/OS specs for this host (cached; effectively immutable)."""
    global _host_specs_cache
    if _host_specs_cache is not None:
        return _host_specs_cache
    model = None
    try:
        with open("/proc/cpuinfo") as handle:
            for line in handle:
                if line.startswith("model name"):
                    model = line.split(":", 1)[1].strip()
                    break
    except Exception:
        pass
    freq = None
    try:
        info = psutil.cpu_freq()
        if info:
            freq = round(info.max or info.current) or None
    except Exception:
        pass
    _host_specs_cache = {
        "cpu_model": model,
        "cores_phys": psutil.cpu_count(logical=False),
        "cores_logical": psutil.cpu_count(),
        "cpu_mhz": freq,
        "kernel": platform.release(),
    }
    return _host_specs_cache


def _k8s_stats():
    if not K8S:
        return {"nodes_ready": 0, "nodes_total": 0, "pods": 0, "nodes": []}
    try:
        k8s_nodes = _v1.list_node()
        node_list = []
        for node in k8s_nodes.items:
            ready = any(
                c.type == "Ready" and c.status == "True"
                for c in (node.status.conditions or [])
            )
            internal_ip = next(
                (a.address for a in (node.status.addresses or []) if a.type == "InternalIP"),
                None,
            )
            node_list.append({"name": node.metadata.name, "ready": ready, "internal_ip": internal_ip})
        total = len(node_list)
        ready_count = sum(1 for n in node_list if n["ready"])
    except Exception:
        total = ready_count = 0
        node_list = []
    try:
        running = sum(1 for pod in _v1.list_pod_for_all_namespaces().items if pod.status.phase == "Running")
    except Exception:
        running = 0
    return {"nodes_ready": ready_count, "nodes_total": total, "pods": running, "nodes": node_list}


async def _pve_node_temp_ssh(host: str) -> float | None:
    global _pve_ssh_temp_cache
    now = time.time()
    cached = _pve_ssh_temp_cache.get(host)
    if cached is not None and now - cached[1] < PVE_SSH_TEMP_CACHE_SECONDS:
        return cached[0]
    if not PVE_SSH_PASS:
        return None
    result = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "sshpass", "-p", PVE_SSH_PASS,
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=4",
            "-o", "BatchMode=no",
            f"{PVE_SSH_USER}@{host}",
            "for d in /sys/class/hwmon/hwmon*/; do name=$(cat \"${d}name\" 2>/dev/null); if [ \"$name\" = \"coretemp\" ] || [ \"$name\" = \"k10temp\" ]; then cat \"${d}\"temp*_input 2>/dev/null; fi; done",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=6)
        values = []
        for line in stdout.decode().splitlines():
            line = line.strip()
            if line.isdigit():
                c = int(line) / 1000.0
                if 30.0 <= c <= 110.0:
                    values.append(c)
        result = round(max(values), 1) if values else None
    except Exception:
        pass
    _pve_ssh_temp_cache[host] = (result, now)
    return result


async def _pve_node_temp(client: httpx.AsyncClient, base: str, name: str, host_ip: str | None = None):
    try:
        response = await client.get(f"{base}/api2/json/nodes/{name}/status", headers=PVE_HDR)
        if response.status_code == 200:
            state = response.json().get("data", {}).get("thermalstate")
            if state:
                import re
                match = re.search(r"[\d.]+", str(state))
                if match:
                    return float(match.group())
    except Exception:
        pass
    if host_ip:
        return await _pve_node_temp_ssh(host_ip)
    return None


async def _pve_build_vm_name_map(client: httpx.AsyncClient, base: str, raw_nodes: list) -> dict:
    """Returns {vm_name_lower: pve_host_name} for all running VMs."""
    mapping: dict = {}
    for node in raw_nodes:
        node_name = node["node"]
        try:
            resp = await client.get(f"{base}/api2/json/nodes/{node_name}/qemu", headers=PVE_HDR)
            if resp.status_code != 200:
                continue
            for vm in resp.json().get("data", []):
                if vm.get("status") == "running" and vm.get("name"):
                    mapping[vm["name"].lower()] = node_name
        except Exception:
            continue
    return mapping


async def _pve_stats():
    global _pve_vm_ip_map, _pve_vm_ip_map_ts, _pve_node_ip_map, _pve_node_ip_map_ts
    global _pve_specs_map, _pve_specs_map_ts

    if not PVE_TOKEN or not PVE_IPS:
        return {"nodes": [], "total": {}, "configured": False, "vm_ip_map": {}}

    for ip in PVE_IPS:
        base = f"https://{ip}:8006"
        try:
            async with httpx.AsyncClient(verify=False, timeout=PVE_TIMEOUT) as client:
                response = await client.get(f"{base}/api2/json/nodes", headers=PVE_HDR)
                if response.status_code != 200:
                    continue
                raw_nodes = response.json().get("data", [])

                now = time.time()
                if not _pve_node_ip_map or now - _pve_node_ip_map_ts > PVE_NODE_IP_CACHE_SECONDS:
                    try:
                        cs = await client.get(f"{base}/api2/json/cluster/status", headers=PVE_HDR)
                        if cs.status_code == 200:
                            _pve_node_ip_map = {
                                e["name"]: e["ip"]
                                for e in cs.json().get("data", [])
                                if e.get("type") == "node" and e.get("ip")
                            }
                            _pve_node_ip_map_ts = now
                    except Exception:
                        pass

                if not _pve_specs_map or now - _pve_specs_map_ts > PVE_SPECS_CACHE_SECONDS:
                    specs: dict = {}
                    for node in raw_nodes:
                        try:
                            st = await client.get(
                                f"{base}/api2/json/nodes/{node['node']}/status", headers=PVE_HDR
                            )
                            if st.status_code == 200:
                                ci = st.json().get("data", {}).get("cpuinfo", {})
                                specs[node["node"]] = {
                                    "cpu_model": ci.get("model"),
                                    "cores_logical": ci.get("cpus"),
                                }
                        except Exception:
                            pass
                    if specs:
                        _pve_specs_map = specs
                        _pve_specs_map_ts = now

                temps = await asyncio.gather(*[
                    _pve_node_temp(client, base, node["node"], _pve_node_ip_map.get(node["node"]))
                    for node in raw_nodes
                ])

                now = time.time()
                if now - _pve_vm_ip_map_ts > PVE_VM_IP_CACHE_SECONDS:
                    _pve_vm_ip_map = await _pve_build_vm_name_map(client, base, raw_nodes)
                    _pve_vm_ip_map_ts = now

                nodes = []
                for node, temp in zip(raw_nodes, temps):
                    maxmem = max(node.get("maxmem", 1), 1)
                    maxdisk = max(node.get("maxdisk", 0), 0)
                    disk_used = node.get("disk", 0)
                    nodes.append(
                        {
                            "name": node["node"],
                            "status": node.get("status", "unknown"),
                            "cpu": round(node.get("cpu", 0) * 100, 1),
                            "cores": node.get("maxcpu"),
                            "mem_pct": round(node.get("mem", 0) / maxmem * 100, 1),
                            "mem_used": node.get("mem", 0),
                            "mem_total": maxmem,
                            "disk_used": disk_used,
                            "disk_total": maxdisk,
                            "disk_pct": round(disk_used / maxdisk * 100, 1) if maxdisk else None,
                            "uptime": node.get("uptime", 0),
                            "temp": temp,
                            "specs": _pve_specs_map.get(node["node"]),
                        }
                    )
                nodes.sort(key=lambda item: item["name"])

                online = [node for node in nodes if node["status"] == "online"]
                total_cpu = round(sum(node["cpu"] for node in online) / max(len(online), 1), 1) if online else 0
                total_mem_used = sum(node["mem_used"] for node in online)
                total_mem_total = sum(node["mem_total"] for node in online)
                total_mem_pct = round(total_mem_used / max(total_mem_total, 1) * 100, 1)
                total_disk_used = sum(node["disk_used"] for node in online)
                total_disk_total = sum(node["disk_total"] for node in online)
                known_temps = [node["temp"] for node in online if node["temp"] is not None]

                return {
                    "nodes": nodes,
                    "total": {
                        "online": len(online),
                        "total": len(nodes),
                        "cpu": total_cpu,
                        "mem_pct": total_mem_pct,
                        "mem_used": total_mem_used,
                        "mem_total": total_mem_total,
                        "disk_used": total_disk_used,
                        "disk_total": total_disk_total,
                        "disk_pct": round(total_disk_used / max(total_disk_total, 1) * 100, 1) if total_disk_total else None,
                        "avg_temp": round(sum(known_temps) / len(known_temps), 1) if known_temps else None,
                    },
                    "configured": True,
                    "vm_ip_map": _pve_vm_ip_map,
                }
        except Exception:
            continue
    return {
        "nodes": [],
        "total": {},
        "configured": bool(PVE_TOKEN and PVE_IPS),
        "vm_ip_map": _pve_vm_ip_map,
    }


_net_state: dict = {"loop": [None, 0.0], "api": [None, 0.0]}


def _local_host_metrics(state_key: str = "loop") -> dict:
    per_core = psutil.cpu_percent(interval=0.2, percpu=True)
    cpu = round(sum(per_core) / len(per_core), 1) if per_core else psutil.cpu_percent()
    try:
        load = [round(x, 2) for x in psutil.getloadavg()]
    except Exception:
        load = None
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()
    now = time.time()
    net_in = net_out = 0.0
    prev, prev_time = _net_state[state_key]
    if prev and prev_time:
        delta = now - prev_time
        if delta > 0:
            net_in = (net.bytes_recv - prev.bytes_recv) / delta
            net_out = (net.bytes_sent - prev.bytes_sent) / delta
    _net_state[state_key] = [net, now]
    return {
        "ts": int(now),
        "host": socket.gethostname(),
        "k8s_node": K8S_NODE_NAME,
        "uptime": int(now - psutil.boot_time()),
        "cpu_pct": round(cpu, 1),
        "cpu_per_core": [round(c, 1) for c in per_core],
        "load": load,
        "specs": _host_specs(),
        "mem_pct": round(mem.percent, 1),
        "mem_used": mem.used,
        "mem_total": mem.total,
        "disk_pct": round(disk.percent, 1),
        "disk_used": disk.used,
        "disk_total": disk.total,
        "net_in": int(net_in),
        "net_out": int(net_out),
        "temp": _cpu_temp(),
    }


def _list_peer_pods() -> list[dict]:
    if not K8S:
        return []
    try:
        pods = _v1.list_namespaced_pod(
            namespace=os.getenv("POD_NAMESPACE", "default"),
            label_selector="app=homelab-stats",
        )
    except Exception:
        return []
    out = []
    for p in pods.items:
        ip = getattr(p.status, "pod_ip", None)
        node = getattr(p.spec, "node_name", None)
        ready = any(
            (cs.ready and cs.name == "homelab-stats") for cs in (p.status.container_statuses or [])
        )
        if ip and node and ready and node != K8S_NODE_NAME:
            out.append({"node": node, "ip": ip})
    return out


_peer_metrics_cache: dict[str, tuple[float, dict]] = {}
PEER_CACHE_TTL = 20.0


def _pseudonym_maps(snapshot: dict) -> tuple[dict, dict]:
    """Stable name → pseudonym maps for k3s and PVE nodes (sorted by real name)."""
    cluster_nodes = (snapshot.get("cluster") or {}).get("nodes") or []
    pve_nodes = (snapshot.get("pve") or {}).get("nodes") or []
    k3s_map = {n.get("name"): f"node-{i+1}" for i, n in enumerate(sorted(cluster_nodes, key=lambda x: x.get("name") or ""))}
    pve_map = {n.get("name"): f"pve-{chr(ord('A') + i)}" for i, n in enumerate(sorted(pve_nodes, key=lambda x: x.get("name") or ""))}
    return k3s_map, pve_map


def _public_view(snapshot: dict) -> dict:
    """Return a public-safe copy of a metrics snapshot.

    Replaces internal hostnames/IPs with stable pseudonyms so the dashboard
    keeps its shape (node count, per-node metrics, mappings) without leaking
    actual k3s/Proxmox topology to the public site.
    """
    if not snapshot:
        return snapshot
    s = json.loads(json.dumps(snapshot))

    cluster_nodes = (s.get("cluster") or {}).get("nodes") or []
    pve_nodes = (s.get("pve") or {}).get("nodes") or []

    k3s_map, pve_map = _pseudonym_maps(s)

    if s.get("k8s_node") in k3s_map:
        s["k8s_node"] = k3s_map[s["k8s_node"]]
    s["host"] = s.get("k8s_node") or "node"

    for n in cluster_nodes:
        n["name"] = k3s_map.get(n.get("name"), n.get("name"))
        n["internal_ip"] = None
        if n.get("pve_host") in pve_map:
            n["pve_host"] = pve_map[n["pve_host"]]
        hm = n.get("host_metrics")
        if isinstance(hm, dict):
            if hm.get("k8s_node") in k3s_map:
                hm["k8s_node"] = k3s_map[hm["k8s_node"]]
            hm["host"] = n["name"]

    for n in pve_nodes:
        n["name"] = pve_map.get(n.get("name"), n.get("name"))

    if isinstance(s.get("pve"), dict):
        s["pve"].pop("vm_ip_map", None)

    return s


def _db_incident_insert(inc: dict) -> int:
    with _db_connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO public_incidents (type, target, target_real, detail, started_ts, ended_ts)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (inc["type"], inc["target"], inc.get("target_real"), inc.get("detail"), inc["started_ts"], inc.get("ended_ts")),
        )
        return int(cur.fetchone()["id"])


def _db_incident_close(db_id: int, ended_ts: int):
    with _db_connect() as conn:
        conn.execute("UPDATE public_incidents SET ended_ts = %s WHERE id = %s", (ended_ts, db_id))


def _db_recent_incidents(limit: int) -> list[dict]:
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT type, target, detail, started_ts, ended_ts
            FROM public_incidents
            WHERE ended_ts IS NOT NULL
            ORDER BY started_ts DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def _db_probe() -> bool:
    try:
        with _db_connect() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def _incident_conditions(snapshot: dict, db_ok: bool) -> dict:
    """Currently-failing conditions keyed by (type, real_target). Targets are pseudonymized."""
    k3s_map, pve_map = _pseudonym_maps(snapshot)
    conds: dict[tuple, dict] = {}

    for n in (snapshot.get("cluster") or {}).get("nodes") or []:
        name = n.get("name") or "?"
        if not n.get("ready"):
            conds[("node_down", name)] = {
                "type": "node_down",
                "target": k3s_map.get(name, "node-?"),
                "target_real": name,
                "detail": "k3s node reported not ready",
            }

    for p in (snapshot.get("pve") or {}).get("nodes") or []:
        name = p.get("name") or "?"
        pseudo = pve_map.get(name, "pve-?")
        if p.get("status") != "online":
            conds[("pve_down", name)] = {
                "type": "pve_down",
                "target": pseudo,
                "target_real": name,
                "detail": "Proxmox node offline",
            }
        elif p.get("temp") is not None and p["temp"] >= INCIDENT_TEMP_C:
            conds[("temp_high", name)] = {
                "type": "temp_high",
                "target": pseudo,
                "target_real": name,
                "detail": f"CPU at {p['temp']:.0f} C (threshold {INCIDENT_TEMP_C:.0f} C)",
            }

    if not db_ok:
        conds[("db_down", "postgres")] = {
            "type": "db_down",
            "target": "database",
            "target_real": "postgres",
            "detail": "metrics database unreachable, live view unaffected",
        }

    return conds


def _step_incidents(conds: dict):
    """Debounced open/close state machine. Persistence is best-effort and retried."""
    now = int(time.time())

    for key, info in conds.items():
        if key in _open_incidents:
            _open_incidents[key]["clear_count"] = 0
            _open_incidents[key]["detail"] = info["detail"]
            continue
        _incident_flap[key] = _incident_flap.get(key, 0) + 1
        if _incident_flap[key] >= INCIDENT_CONFIRM_TICKS:
            _incident_flap.pop(key, None)
            _open_incidents[key] = {**info, "started_ts": now, "db_id": None, "clear_count": 0}
            print(f"incident open: {info['type']} {info['target_real']}", flush=True)

    for key in list(_incident_flap):
        if key not in conds:
            _incident_flap.pop(key, None)

    for key, inc in list(_open_incidents.items()):
        if key in conds:
            continue
        inc["clear_count"] += 1
        if inc["clear_count"] >= INCIDENT_CONFIRM_TICKS:
            _open_incidents.pop(key, None)
            inc["ended_ts"] = now
            print(f"incident closed: {inc['type']} {inc['target_real']}", flush=True)
            _incident_backlog.append(inc)


def _persist_incidents():
    """Push open incidents and closed backlog to the DB; safe to fail (retried next tick)."""
    for inc in _open_incidents.values():
        if inc["db_id"] is None:
            try:
                inc["db_id"] = _db_incident_insert(inc)
            except Exception:
                return
    while _incident_backlog:
        inc = _incident_backlog[0]
        try:
            if inc.get("db_id") is not None:
                _db_incident_close(inc["db_id"], inc["ended_ts"])
            else:
                _db_incident_insert(inc)
            _incident_backlog.pop(0)
        except Exception:
            return


def _open_incidents_public() -> list[dict]:
    items = [
        {"type": inc["type"], "target": inc["target"], "detail": inc["detail"], "started_ts": inc["started_ts"]}
        for inc in _open_incidents.values()
    ]
    items.sort(key=lambda item: item["started_ts"])
    return items


_public_cache: dict = {}


async def _fetch_peer_metrics(peer: dict) -> tuple[str, dict | None]:
    url = f"http://{peer['ip']}:8080/api/host"
    for attempt in (1, 2):
        try:
            async with httpx.AsyncClient(timeout=4.0) as cx:
                r = await cx.get(url)
                if r.status_code == 200:
                    return peer["node"], r.json()
        except Exception:
            if attempt == 2:
                break
    return peer["node"], None


async def _collect_peer_metrics() -> dict:
    peers = await asyncio.get_running_loop().run_in_executor(None, _list_peer_pods)
    if not peers:
        return {}
    results = await asyncio.gather(*(_fetch_peer_metrics(p) for p in peers))
    out: dict = {}
    now = time.time()
    for node, data in results:
        if data:
            _peer_metrics_cache[node] = (now, data)
            out[node] = data
        else:
            cached = _peer_metrics_cache.get(node)
            if cached and now - cached[0] <= PEER_CACHE_TTL:
                out[node] = cached[1]
    return out


async def _collect():
    loop = asyncio.get_running_loop()
    self_metrics = await loop.run_in_executor(None, _local_host_metrics)
    now = self_metrics["ts"]

    k8s_data, pve, peer_metrics = await asyncio.gather(
        loop.run_in_executor(None, _k8s_stats),
        _pve_stats(),
        _collect_peer_metrics(),
    )

    vm_name_map = pve.get("vm_ip_map", {})
    pve_host_temps = {n["name"]: n["temp"] for n in pve.get("nodes", [])}
    for k3s_node in k8s_data.get("nodes", []):
        pve_host = vm_name_map.get(k3s_node.get("name", "").lower())
        k3s_node["pve_host"] = pve_host
        k3s_node["pve_host_temp"] = pve_host_temps.get(pve_host) if pve_host else None
        name = k3s_node.get("name")
        if name == K8S_NODE_NAME:
            k3s_node["host_metrics"] = self_metrics
        elif name in peer_metrics:
            k3s_node["host_metrics"] = peer_metrics[name]

    return {
        "ts": now,
        "host": self_metrics["host"],
        "k8s_node": K8S_NODE_NAME,
        "uptime": self_metrics["uptime"],
        "cpu": {"pct": self_metrics["cpu_pct"], "cores": psutil.cpu_count(), "per_core": self_metrics["cpu_per_core"]},
        "mem": {"pct": self_metrics["mem_pct"], "used": self_metrics["mem_used"], "total": self_metrics["mem_total"]},
        "disk": {"pct": self_metrics["disk_pct"], "used": self_metrics["disk_used"], "total": self_metrics["disk_total"]},
        "net": {"in": self_metrics["net_in"], "out": self_metrics["net_out"]},
        "load": self_metrics["load"],
        "specs": self_metrics["specs"],
        "temp": self_metrics["temp"],
        "cluster": k8s_data,
        "pve": pve,
        "requests": _req_count,
    }


async def _loop():
    global _cache, _public_cache, _last_snapshot_write

    while True:
        try:
            snapshot = await _collect()
            _cache = snapshot

            db_ok = await asyncio.to_thread(_db_probe)
            _step_incidents(_incident_conditions(snapshot, db_ok))
            if db_ok:
                await asyncio.to_thread(_persist_incidents)

            _public_cache = _public_view(snapshot)
            _public_cache["incidents_open"] = _open_incidents_public()
            await _broadcast(_metrics_clients, _sse_message(_public_cache))

            if snapshot["ts"] - _last_snapshot_write >= SNAPSHOT_INTERVAL_SECONDS:
                try:
                    await asyncio.to_thread(_db_insert_snapshot, snapshot)
                    _last_snapshot_write = snapshot["ts"]
                except Exception as exc:
                    print(f"snapshot write error: {exc}", flush=True)
        except Exception as exc:
            print(f"collect error: {exc}", flush=True)
        await asyncio.sleep(LIVE_INTERVAL_SECONDS)


def _stream_from_queue(queue: asyncio.Queue, clients: set):
    async def gen():
        try:
            while True:
                try:
                    yield await asyncio.wait_for(queue.get(), timeout=25)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            clients.discard(queue)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/stream")
async def stream():
    queue = asyncio.Queue(maxsize=10)
    _metrics_clients.add(queue)
    if _public_cache:
        queue.put_nowait(_sse_message(_public_cache))
    return _stream_from_queue(queue, _metrics_clients)


@app.get("/api/console/stream")
async def console_stream():
    queue = asyncio.Queue(maxsize=CONSOLE_LIMIT)
    _console_clients.add(queue)
    return _stream_from_queue(queue, _console_clients)


@app.get("/api/metrics")
async def metrics():
    return _public_cache


@app.get("/api/host")
async def host_metrics(request: Request):
    # Pod-to-pod peer exchange only: proxied (public) traffic always carries
    # forwarding headers, direct pod/LAN traffic never does.
    forwarded = request.headers.get("x-forwarded-for") or request.headers.get("x-real-ip")
    peer = request.client.host if request.client else None
    if forwarded or not _peer_is_trusted(peer):
        raise HTTPException(status_code=404, detail="Not found")
    return await asyncio.get_running_loop().run_in_executor(None, _local_host_metrics, "api")


@app.get("/api/weather")
async def weather():
    return await _fetch_weather()


@app.get("/api/console/recent")
async def console_recent(limit: int = CONSOLE_LIMIT):
    safe_limit = max(1, min(limit, 200))
    return {"events": await asyncio.to_thread(_db_recent_requests, safe_limit)}


@app.get("/api/history/series")
async def history_series(window: str = "day"):
    global _history_series_cache, _history_series_cache_ts
    safe_window = window if window in HISTORY_WINDOWS else "day"
    now = time.time()
    if safe_window in _history_series_cache and now - _history_series_cache_ts.get(safe_window, 0) < HISTORY_SERIES_CACHE_SECONDS:
        return _history_series_cache[safe_window]
    result = await asyncio.to_thread(_db_history_points, safe_window)
    _history_series_cache[safe_window] = result
    _history_series_cache_ts[safe_window] = now
    return result


@app.get("/api/history/summary")
async def history_summary():
    global _history_summary_cache, _history_summary_cache_ts
    now = time.time()
    if _history_summary_cache is not None and now - _history_summary_cache_ts < HISTORY_SUMMARY_CACHE_SECONDS:
        return _history_summary_cache
    result = await asyncio.to_thread(_db_history_summary)
    _history_summary_cache = result
    _history_summary_cache_ts = now
    return result


_incidents_cache: dict | None = None
_incidents_cache_ts: float = 0.0
INCIDENTS_CACHE_SECONDS = 15


@app.get("/api/incidents")
async def incidents():
    global _incidents_cache, _incidents_cache_ts
    now = time.time()
    if _incidents_cache is not None and now - _incidents_cache_ts < INCIDENTS_CACHE_SECONDS:
        recent = _incidents_cache
    else:
        try:
            recent = await asyncio.to_thread(_db_recent_incidents, 25)
            _incidents_cache = recent
            _incidents_cache_ts = now
        except Exception:
            recent = _incidents_cache or []
    return {
        "open": _open_incidents_public(),
        "recent": recent,
        "temp_threshold_c": INCIDENT_TEMP_C,
    }


@app.get("/robots.txt")
async def robots():
    return PlainTextResponse(
        "User-agent: *\nDisallow: /api/\nDisallow: /fleet/\nDisallow: /media/\nAllow: /\n"
    )


@app.get("/api/history/export.csv")
async def history_export(window: str = "day"):
    safe_window = window if window in HISTORY_WINDOWS else "day"
    payload = await history_series(safe_window)
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        ["ts", "cpu_pct", "mem_pct", "disk_pct", "temp", "net_in", "net_out", "cluster_pods", "requests"]
    )
    for point in payload["points"]:
        writer.writerow(
            [
                point["ts"],
                point["cpu_pct"],
                point["mem_pct"],
                point["disk_pct"],
                point["temp"],
                point["net_in"],
                point["net_out"],
                point["cluster_pods"],
                point["requests"],
            ]
        )
    return PlainTextResponse(
        buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="history-{safe_window}.csv"'},
    )


@app.get("/api/guestbook")
async def guestbook_public(limit: int = GUESTBOOK_PUBLIC_LIMIT):
    safe_limit = max(1, min(limit, 50))
    entries = await asyncio.to_thread(_db_guestbook_public, safe_limit)
    return {"entries": entries, "counts": await asyncio.to_thread(_db_guestbook_counts)}


@app.post("/api/guestbook", status_code=201)
async def guestbook_submit(payload: dict, request: Request):
    name = _sanitize_guestbook_text(str(payload.get("name", "")), GUESTBOOK_MAX_NAME)
    message = _sanitize_guestbook_text(str(payload.get("message", "")), GUESTBOOK_MAX_MESSAGE)
    if not name:
        raise HTTPException(status_code=400, detail="Name is required.")
    if not message:
        raise HTTPException(status_code=400, detail="Message is required.")

    client_ip = _request_client_ip(request)
    last_ts = await asyncio.to_thread(_db_guestbook_last_submit_ts, client_ip)
    now = int(time.time())
    if last_ts and now - last_ts < GUESTBOOK_RATE_LIMIT_SECONDS:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limited. Try again in {GUESTBOOK_RATE_LIMIT_SECONDS - (now - last_ts)} seconds.",
        )

    entry_id = await asyncio.to_thread(
        _db_guestbook_submit,
        {"ts": now, "name": name, "message": message, "client_ip": client_ip},
    )
    return {
        "ok": True,
        "id": entry_id,
        "status": "pending",
        "message": "Entry submitted for moderation.",
    }


@app.get("/api/guestbook/moderation")
async def guestbook_moderation(request: Request, limit: int = 50):
    _require_mod_token(request)
    safe_limit = max(1, min(limit, 200))
    return {"entries": await asyncio.to_thread(_db_guestbook_pending, safe_limit)}


@app.post("/api/guestbook/{entry_id}/moderate")
async def guestbook_moderate(entry_id: int, payload: dict, request: Request):
    moderator = _require_mod_token(request)
    action = str(payload.get("action", "")).strip().lower()
    if action not in {"approve", "reject"}:
        raise HTTPException(status_code=400, detail="Action must be approve or reject.")
    changed = await asyncio.to_thread(_db_guestbook_moderate, entry_id, action, moderator)
    if not changed:
        raise HTTPException(status_code=404, detail="Pending entry not found.")
    return {"ok": True, "id": entry_id, "status": "approved" if action == "approve" else "rejected"}


@app.get("/media/{filename:path}")
async def media(filename: str):
    return FileResponse(_safe_photo_path(filename))


@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "index.html")


@app.get("/history")
async def history_page():
    return FileResponse(BASE_DIR / "history.html")


@app.get("/console")
async def console_page():
    return FileResponse(BASE_DIR / "console.html")
