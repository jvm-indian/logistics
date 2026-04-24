import asyncio
import json
from typing import Any, Dict

try:
	from .base_agent import call_llm, get_redis_client, load_mock_response
except ImportError:
	from base_agent import call_llm, get_redis_client, load_mock_response

ARBITER_OUTPUT_CHANNEL = "logistiq:arbiter_output"
COMMANDER_OUTPUT_CHANNEL = "logistiq:commander_output"

COMMANDER_SYSTEM_PROMPT = """You are the Commander Agent for LogistiQ, an autonomous supply chain
intelligence system. You translate the Arbiter's technical resolution into a concise executive
briefing for human decision-makers.

Given the Arbiter's output, respond ONLY with a JSON object containing:
- event_id: string (copy from input)
- executive_summary: string (2-3 plain English sentences suitable for a senior manager)
- incidents_resolved: integer (number of anomaly threads handled)
- actions_taken: array of strings (plain English list of autonomous actions executed)
- actions_blocked: array of strings (plain English list of actions that were blocked and why)
- requires_human_attention: boolean
- human_attention_reason: string or null (what the human must do and by when, if applicable)
- confidence_overall: float 0-1
- estimated_cost_impact: string (e.g. "Prevented estimated \u20b96,02,000 in combined losses")
- incident_report: object with:
  - title: string
  - timestamp: string (ISO 8601)
  - duration_seconds: integer (estimated end-to-end processing time)
  - root_causes: array of strings
  - resolution: string

Return ONLY valid JSON. No explanation, no markdown."""


def _parse_payload(data: str) -> Dict[str, Any] | None:
	try:
		parsed = json.loads(data)
		if isinstance(parsed, dict):
			return parsed
	except json.JSONDecodeError:
		return None
	return None


async def run() -> None:
	redis_client = get_redis_client()
	pubsub = redis_client.pubsub()
	await pubsub.subscribe(ARBITER_OUTPUT_CHANNEL)

	try:
		while True:
			message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
			if not message:
				await asyncio.sleep(0.05)
				continue

			raw_payload = message.get("data")
			if not isinstance(raw_payload, str):
				continue

			arbiter_output = _parse_payload(raw_payload)
			if arbiter_output is None:
				continue

			try:
				commander_result = await call_llm(COMMANDER_SYSTEM_PROMPT, arbiter_output)
			except Exception:
				commander_result = load_mock_response("commander")
			await redis_client.publish(
				COMMANDER_OUTPUT_CHANNEL,
				json.dumps(commander_result, ensure_ascii=True),
			)
	finally:
		await pubsub.unsubscribe(ARBITER_OUTPUT_CHANNEL)
		await pubsub.close()


if __name__ == "__main__":
	asyncio.run(run())
