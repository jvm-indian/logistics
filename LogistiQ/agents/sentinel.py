import asyncio
import json
from typing import Any, Dict, Iterable

try:
	from .base_agent import call_llm, get_redis_client, load_mock_response
except ImportError:
	from base_agent import call_llm, get_redis_client, load_mock_response

ALERTS_CHANNEL = "logistiq:alerts"
SENTINEL_OUTPUT_CHANNEL = "logistiq:sentinel_output"

SENTINEL_SYSTEM_PROMPT = """You are the Sentinel Agent for LogistiQ, an autonomous supply chain
intelligence system. You specialise in IoT causal-chain diagnosis.

Given an IOT_ANOMALY alert, respond ONLY with a JSON object containing:
- event_id: string (copy from input)
- symptom: string (plain English description of what was observed)
- probable_cause: string (DOOR_LEFT_OPEN / COMPRESSOR_FAULT / POWER_ISSUE / UNKNOWN)
- confidence: float 0-1
- causal_chain: array of strings (ordered reasoning steps leading to the conclusion)
- recommended_action: string (immediate corrective action)
- urgency: string (CRITICAL / HIGH / MEDIUM / LOW)
- estimated_goods_at_risk: string (monetary estimate, e.g. "₹4,20,000 in pharmaceutical inventory")

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
		if anomaly.get("anomaly_type") == "IOT_ANOMALY":
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
					sentinel_result = await call_llm(SENTINEL_SYSTEM_PROMPT, llm_input)
				except Exception:
					sentinel_result = load_mock_response("sentinel")
				await redis_client.publish(
					SENTINEL_OUTPUT_CHANNEL,
					json.dumps(sentinel_result, ensure_ascii=True),
				)
	finally:
		await pubsub.unsubscribe(ALERTS_CHANNEL)
		await pubsub.close()


if __name__ == "__main__":
	asyncio.run(run())
