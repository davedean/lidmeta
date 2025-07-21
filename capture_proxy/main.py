import base64
import json
import os
import uuid
from datetime import datetime
from typing import List

import aiofiles
import brotli  # type: ignore  # dev dependency only used inside capture container
import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import Response as FastResponse

import logging
import sys
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

UPSTREAM_BASE = os.getenv("UPSTREAM_BASE", "https://api.musicinfo.pro")
LOCAL_BASE = os.getenv("LOCAL_BASE", "http://metadata-server:8080/api/v1")
LOG_DIR = os.getenv("LOG_DIR", "capture_logs")
ROUTE_LOCAL = [
    p.strip() for p in os.getenv("ROUTE_LOCAL_PREFIXES", "").split(",") if p.strip()
]


os.makedirs(LOG_DIR, exist_ok=True)

# Re-use a single client with configurable timeout
app = FastAPI(title="Skyhook Capture Proxy")
client = httpx.AsyncClient(
    follow_redirects=True,
    timeout=httpx.Timeout(float(os.getenv("PROXY_TIMEOUT", "60"))),
    trust_env=True,
    http2=False,
)
# Helpful startup banner
logger.info(f"Proxy timeout set to {float(os.getenv('PROXY_TIMEOUT', '30'))}s")


HOP_HEADERS: List[str] = [
    "host",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
]


def should_route_local(path: str) -> bool:
    return any(path.startswith(prefix.lstrip("/")) for prefix in ROUTE_LOCAL)


@app.api_route(
    "/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
)
async def proxy(full_path: str, request: Request):
    # Decide target base
    base = LOCAL_BASE if should_route_local(full_path) else UPSTREAM_BASE

    # Translate SkyHook v1 paths to v0.4 **only** when talking to upstream.
    if base == UPSTREAM_BASE and full_path.startswith("api/v1/"):
        translated_path = "api/v0.4/" + full_path[len("api/v1/") :]
    else:
        translated_path = full_path

    target_url = f"{base.rstrip('/')}/{translated_path}"

    # Prepare headers minus hop-by-hop headers and force plain responses
    headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_HEADERS}
    # Avoid compressed payloads so Lidarr never has to decode them again.
    headers["accept-encoding"] = "identity"

    body = await request.body()

    error_text: str | None = None
    try:
        resp = await client.request(
            request.method,
            target_url,
            params=request.query_params,
            content=body,
            headers=headers,
        )
        status = resp.status_code
        response_headers = {
            k: v for k, v in resp.headers.items() if k.lower() not in HOP_HEADERS
        }

        # Preserve raw bytes so downstream tooling can reconstruct exact payloads.
        raw_body: bytes = resp.content

        # If the payload is JSON we’d like to log it as pretty-printed text to make
        # diffs friendlier.  Some responses are Brotli/gzip compressed – we
        # transparently decode them here so the extractor doesn’t have to.
        content_type = response_headers.get("content-type", "").lower()
        encoding = response_headers.get("content-encoding", "").lower()

        if content_type.startswith("application/json"):
            try:
                if "br" in encoding:
                    raw_body = brotli.decompress(raw_body)
                    response_headers.pop("content-encoding", None)
                elif "gzip" in encoding:
                    import gzip

                    raw_body = gzip.decompress(raw_body)
                    response_headers.pop("content-encoding", None)
                response_body = raw_body.decode()
            except Exception:  # pragma: no cover – fallback to base64
                response_body = base64.b64encode(raw_body).decode()
                # Mark the encoding so extractor can reverse it.
                response_headers["x-body-encoding"] = "base64"
        else:
            # Binary/non-JSON responses: store base64 so we keep log JSON valid.
            response_body = base64.b64encode(raw_body).decode()
            response_headers["x-body-encoding"] = "base64"
        # Content-Length is now invalid – let FastAPI recalc.
        response_headers.pop("content-length", None)
    except httpx.HTTPError as exc:
        status = 502
        response_headers = {}
        response_body = ""
        error_text = str(exc)

    # Log interaction (incl. errors)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")
    filename = f"{timestamp}_{uuid.uuid4().hex}.json"
    log_path = os.path.join(LOG_DIR, filename)
    log_payload = {
        "request": {
            "method": request.method,
            "path": full_path,
            "query": str(request.url.query),
            "headers": dict(headers),
            "body": body.decode("utf-8", errors="ignore"),
        },
        "response": {
            "status": status,
            "headers": response_headers,
            "body": response_body,
            "error": error_text,
        },
    }
    async with aiofiles.open(log_path, "w") as fp:
        await fp.write(json.dumps(log_payload, indent=2))

    if error_text is not None:
        return FastResponse(
            content=f"Upstream proxy error: {error_text}", status_code=502
        )

    return FastResponse(
        content=resp.content, status_code=status, headers=response_headers
    )
