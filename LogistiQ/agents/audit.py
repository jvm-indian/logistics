import asyncio
import json
from typing import Any, Dict, Iterable

try:
	from .base_agent import call_llm, get_redis_client, load_mock_response
except ImportError:
	from base_agent import call_llm, get_redis_client, load_mock_response

ALERTS_CHANNEL = "logistiq:alerts"
AUDIT_OUTPUT_CHANNEL = "logistiq:audit_output"

AUDIT_SYSTEM_PROMPT = """You are the Audit Agent for LogistiQ, an autonomous supply chain
intelligence system. You specialise in inventory reconciliation.

Given an RFID_MISMATCH alert, respond ONLY with a JSON object containing:
- event_id: string (copy from input)
- gap_quantity: integer (number of missing units)
- gap_value_estimate: string (estimated monetary value, e.g. "₹1,82,000")
- recount_priority: string (HIGH / MEDIUM / LOW)
- will_reroute_worsen_shortage: boolean
- shortage_context: string (plain English description of the shortage impact)
- recommended_action: string (concise action instruction)

Return ONLY valid JSON. No explanation, no markdown."""


def _parse_alert(data: str) -> Dict[str, Any] | None:
	try:
		parsed = json.loads(data)
		if isinstance(parsed, dict):
			return parsed
	except json.JSONDecodeError:
		return None
	return None


def _iter_qualifying_anomalies(alert: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
	anomalies = alert.get("anomalies", [])
	if not isinstance(anomalies, list):
		return []

	qualifying: list[Dict[str, Any]] = []
	for anomaly in anomalies:
		if not isinstance(anomaly, dict):
			continue
		if anomaly.get("anomaly_type") == "RFID_MISMATCH":
			qualifying.append(anomaly)
	return qualifying


async def run() -> None:
	redis_client = get_redis_client()
	pubsub = redis_client.pubsub()
	await pubsub.subscribe(ALERTS_CHANNEL)

	try:
		while True:
			message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
			if not message:
				await asyncio.sleep(0.05)
				continue

			raw_payload = message.get("data")
			if not isinstance(raw_payload, str):
				continue

			alert = _parse_alert(raw_payload)
			if alert is None:
				continue

			event_id = alert.get("event_id", "unknown_event")
			for anomaly in _iter_qualifying_anomalies(alert):
				llm_input = {
					"event_id": event_id,
					"anomaly": anomaly,
					"overall_assessment": alert.get("overall_assessment", ""),
				}
				try:
					audit_result = await call_llm(AUDIT_SYSTEM_PROMPT, llm_input)
				except Exception:
					audit_result = load_mock_response("audit")
				await redis_client.publish(
					AUDIT_OUTPUT_CHANNEL,
					json.dumps(audit_result, ensure_ascii=True),
				)
	finally:
		await pubsub.unsubscribe(ALERTS_CHANNEL)
		await pubsub.close()


if __name__ == "__main__":
	asyncio.run(run())
