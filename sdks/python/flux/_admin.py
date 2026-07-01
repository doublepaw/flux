# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""HTTP admin client for topic and schema management."""

import asyncio
import json
import urllib.request
import urllib.error

from .exceptions import FluxException, SchemaException


class AdminClient:
    """HTTP wrapper for the Flux broker admin API."""

    def __init__(self, base_url: str, api_key: str | None = None):
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key

    async def list_topics(self) -> list[dict]:
        """Fetch all topics. Returns list of {topic_id, name, retention_hours, ...}."""
        return await self._get("/topics")

    async def register_schema(self, topic_id: int, schema: dict) -> int:
        """Register a schema for a topic. Returns schema_id (idempotent via hash dedup)."""
        body = {"topic_id": topic_id, "schema": schema}
        resp = await self._post("/schemas", body)
        return resp["schema_id"]

    async def _get(self, path: str) -> list | dict:
        url = self._base_url + path
        req = urllib.request.Request(url, method="GET")
        self._add_headers(req)
        return await self._do(req)

    async def _post(self, path: str, body: dict) -> dict:
        url = self._base_url + path
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        self._add_headers(req)
        return await self._do(req)

    def _add_headers(self, req: urllib.request.Request) -> None:
        if self._api_key:
            req.add_header("Authorization", f"Bearer {self._api_key}")

    async def _do(self, req: urllib.request.Request):
        try:
            resp = await asyncio.to_thread(urllib.request.urlopen, req)
            return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            if e.code == 409:
                raise SchemaException(f"Incompatible schema: {body}")
            raise FluxException(f"Admin API error {e.code}: {body}")
        except urllib.error.URLError as e:
            raise FluxException(f"Admin API unreachable: {e.reason}")