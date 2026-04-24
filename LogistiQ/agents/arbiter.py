import asyncio
import json
from typing import Any, Dict

try:
	from .base_agent import call_llm, get_redis_client, load_mock_response
except ImportError:
	from base_agent import call_llm, get_redis_client, load_mock_response

ROUTER_OUTPUT_CHANNEL = "logistiq:router_output"
AUDIT_OUTPUT_CHANNEL = "logistiq:audit_output"
SENTINEL_OUTPUT_CHANNEL = "logistiq:sentinel_output"
ARBITER_OUTPUT_CHANNEL = "logistiq:arbiter_output"

ARBITER_SYSTEM_PROMPT = """You are the Arbiter Agent for LogistiQ, an autonomous supply chain
intelligence system. You synthesise outputs from specialist agents and resolve conflicts.

Given the latest outputs from Router, Audit, and Sentinel agents, respond ONLY with a JSON
object containing:
- event_id: string (copy from whichever input has one)
- conflicts_detected: array of objects, each with:
  - conflict_type: string (e.g. ROUTE_VS_INVENTORY, DRIVER_HOURS_VIOLATION)
  - agents_involved: array of strings
  - description: string (plain English conflict description)
- final_actions: array of objects, each with:
  - action: string
  - asset: string
  - rationale: string
- blocked_actions: array of objects, each with:
  - action: string
  - reason: string
  - rule_violated: string
- escalate_to_human: boolean
- escalation_reason: string or null
- confidence_overall: float 0-1
- resolution_reasoning: string (concise explanation of the arbitration logic)

Return ONLY valid JSON. No explanation, no markdown."""


def _parse_payload(data: str) -> Dict[str, Any] | None:
	try:
		parsed = json.loads(data)
		if isinstance(parsed, dict):
			return parsed
	except json.JSONDecodeError:
		return None
	return None


def _extract_event_id(*payloads: Dict[str, Any] | None) -> str:
	for payload in payloads:
		if isinstance(payload, dict):
			event_id = payload.get("event_id")
			if event_id:
				return str(event_id)
	return "unknown_event"


async def run() -> None:
	redis_client = get_redis_client()
	pubsub = redis_client.pubsub()
	await pubsub.subscribe(ROUTER_OUTPUT_CHANNEL, AUDIT_OUTPUT_CHANNEL, SENTINEL_OUTPUT_CHANNEL)

	latest_router: Dict[str, Any] | None = None
	latest_audit: Dict[str, Any] | None = None
	latest_sentinel: Dict[str, Any] | None = None

	try:
		while True:
			message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
			if not message:
				await asyncio.sleep(0.05)
				continue

			channel = message.get("channel")
			raw_payload = message.get("data")
			if not isinstance(raw_payload, str):
				continue

			payload = _parse_payload(raw_payload)
			if payload is None:
				continue

			if channel == ROUTER_OUTPUT_CHANNEL:
				latest_router = payload
			elif channel == AUDIT_OUTPUT_CHANNEL:
				latest_audit = payload
			elif channel == SENTINEL_OUTPUT_CHANNEL:
				latest_sentinel = payload
			else:
				continue

			llm_input = {
				"event_id": _extract_event_id(latest_router, latest_audit, latest_sentinel),
				"router": latest_router,
				"audit": latest_audit,
				"sentinel": latest_sentinel,
			}
			try:
				arbiter_result = await call_llm(ARBITER_SYSTEM_PROMPT, llm_input)
			except Exception:
				arbiter_result = load_mock_response("arbiter")
			await redis_client.publish(
				ARBITER_OUTPUT_CHANNEL,
				json.dumps(arbiter_result, ensure_ascii=True),
			)
	finally:
		await pubsub.unsubscribe(ROUTER_OUTPUT_CHANNEL, AUDIT_OUTPUT_CHANNEL, SENTINEL_OUTPUT_CHANNEL)
		await pubsub.close()


if __name__ == "__main__":
	asyncio.run(run())
