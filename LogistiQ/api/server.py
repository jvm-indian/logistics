import asyncio
import json
import os
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List

import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

try:
	from dotenv import load_dotenv
	load_dotenv()
except Exception:
	pass


APP_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_FILE = APP_ROOT / "dashboard" / "index.html"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CHANNELS = [
	"logistiq:raw_data",
	"logistiq:alerts",
	"logistiq:router_output",
	"logistiq:audit_output",
	"logistiq:sentinel_output",
	"logistiq:arbiter_output",
	"logistiq:commander_output",
]

app = FastAPI(title="LogistiQ API", version="1.0.0")

_latest_events: List[Dict[str, Any]] = []
_latest_lock = asyncio.Lock()


def _decode_message(payload: Any) -> Any:
	if isinstance(payload, (bytes, bytearray)):
		try:
			return payload.decode("utf-8")
		except Exception:
			return payload.decode(errors="replace")
	return payload


async def _store_event(channel: str, data: Any) -> None:
	message = {"channel": channel, "data": data}
	async with _latest_lock:
		_latest_events.append(message)
		if len(_latest_events) > 100:
			del _latest_events[:-100]


@app.on_event("startup")
async def startup_event() -> None:
	app.state.redis = redis.from_url(REDIS_URL, decode_responses=True)
	app.state.redis_task = asyncio.create_task(_redis_fanout_loop())


@app.on_event("shutdown")
async def shutdown_event() -> None:
	task = getattr(app.state, "redis_task", None)
	if task:
		task.cancel()
		try:
			await task
		except asyncio.CancelledError:
			pass
	client = getattr(app.state, "redis", None)
	if client:
		await client.aclose()


async def _redis_fanout_loop() -> None:
	client = app.state.redis
	pubsub = client.pubsub()
	await pubsub.subscribe(*CHANNELS)
	try:
		while True:
			message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
			if not message:
				await asyncio.sleep(0.05)
				continue
			channel = _decode_message(message.get("channel", "unknown"))
			data = _decode_message(message.get("data"))
			await _store_event(channel, data)
	finally:
		await pubsub.unsubscribe(*CHANNELS)
		await pubsub.close()


@app.get("/health")
async def health() -> JSONResponse:
	return JSONResponse({"status": "ok"})


@app.get("/snapshot")
async def snapshot() -> JSONResponse:
	async with _latest_lock:
		return JSONResponse({"events": list(_latest_events)})


@app.get("/events")
async def events() -> StreamingResponse:
	async def event_stream() -> AsyncIterator[str]:
		async with _latest_lock:
			last_seen = len(_latest_events)
		while True:
			async with _latest_lock:
				current_events = list(_latest_events)
				new_events = current_events[last_seen:]
				last_seen = len(current_events)
			for event in new_events:
				yield f"data: {json.dumps(event, ensure_ascii=True)}\n\n"
			await asyncio.sleep(0.5)

	return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/")
async def dashboard() -> FileResponse:
	return FileResponse(DASHBOARD_FILE)
