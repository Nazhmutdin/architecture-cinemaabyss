import os
import random
from fastapi import FastAPI, Request
from fastapi.responses import Response
import httpx

app = FastAPI()

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
GRADUAL = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))


@app.api_route("/api/proxy/health")
async def health():
    return {"status": "ok"}


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_fallback(request: Request, path: str):
    if "api/movies" in path:
        use_new = GRADUAL and random.randint(1, 100) <= PERCENT
        target = MOVIES_SERVICE_URL if use_new else MONOLITH_URL
        url = f"{target}/{path}"
    elif "api/events" in path:
        url = f"{EVENTS_SERVICE_URL}/{path}"
    else:
        url = f"{MONOLITH_URL}/{path}"

    async with httpx.AsyncClient() as client:
        req = client.build_request(
            method=request.method,
            url=url,
            headers=dict(request.headers),
            content=await request.body(),
        )
        resp = await client.send(req)

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers),
    )
