import json
import os
from pathlib import Path
from typing import Any, Dict

import redis.asyncio as redis
from anthropic import AsyncAnthropic

try:
	from dotenv import load_dotenv
	load_dotenv()
except Exception:
	pass

MODEL_NAME = "claude-sonnet-4-20250514"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

_anthropic_client: AsyncAnthropic | None = None
_redis_client: redis.Redis | None = None


def get_redis_client() -> redis.Redis:
	global _redis_client
	if _redis_client is None:
		_redis_client = redis.from_url(REDIS_URL, decode_responses=True)
	return _redis_client


def get_anthropic_client() -> AsyncAnthropic:
	global _anthropic_client
	if _anthropic_client is None:
		_anthropic_client = AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
	return _anthropic_client


def _strip_markdown_fences(text: str) -> str:
	cleaned = text.strip()
	if cleaned.startswith("```"):
		parts = cleaned.splitlines()
		if parts:
			parts = parts[1:]
		if parts and parts[-1].strip().startswith("```"):
			parts = parts[:-1]
		cleaned = "\n".join(parts).strip()
	return cleaned


def _project_root() -> Path:
	return Path(__file__).resolve().parents[1]


def _default_mock_payloads() -> Dict[str, Dict[str, Any]]:
	return {
		"scout": {
			"event_id": "demo_event_001",
			"anomalies": [
				{
					"anomaly_type": "GPS_DEVIATION",
					"severity": 4,
					"confidence": 0.92,
					"affected_asset": "TRUCK-17",
					"key_facts": [
						"Vehicle drifted 3.4 km off planned corridor",
						"Deviation persisted for 14 minutes",
					],
					"recommended_agents": ["ROUTER", "SENTINEL"],
				}
			],
			"overall_assessment": "A sustained route deviation requires immediate reroute validation.",
		},
		"router": {
			"event_id": "demo_event_001",
			"proposed_action": "REROUTE_NORTH",
			"route_description": "Take Exit 22, merge onto North Bypass, and rejoin primary route at Junction D.",
			"eta_delta_minutes": 12,
			"driver_hours_remaining": 47,
			"driver_hours_after_reroute": 35,
			"driver_hours_violated": False,
			"priority_score": 8,
			"downstream_inventory_impact": "Minor delay for stop 3, no stockout risk for same-day orders.",
		},
		"audit": {
			"event_id": "demo_event_001",
			"gap_quantity": 65,
			"gap_value_estimate": "₹1,82,000",
			"recount_priority": "HIGH",
			"will_reroute_worsen_shortage": True,
			"shortage_context": "Destination warehouse is operating at 92.9% of expected stock.",
			"recommended_action": "Initiate manual recount and expedite delivery.",
		},
		"sentinel": {
			"event_id": "demo_event_001",
			"symptom": "Temperature spike from 6.2°C to 12.4°C over 8 minutes",
			"probable_cause": "DOOR_LEFT_OPEN",
			"confidence": 0.94,
			"causal_chain": [
				"RFID access event recorded at cold store entry.",
				"Temperature began rising 1 minute after access.",
				"Adjacent units show no temperature change — environmental cause ruled out.",
				"Vibration sensor within normal range — compressor fault ruled out.",
				"Conclusion: Door was left open after access event.",
			],
			"recommended_action": "Dispatch personnel to close door immediately and assess stock.",
			"urgency": "CRITICAL",
			"estimated_goods_at_risk": "₹4,20,000 in pharmaceutical inventory",
		},
		"arbiter": {
			"event_id": "demo_event_001",
			"conflicts_detected": [
				{
					"conflict_type": "ROUTE_VS_INVENTORY",
					"agents_involved": ["ROUTER", "AUDIT"],
					"description": "Proposed reroute adds delay that worsens inventory shortage at destination.",
				}
			],
			"final_actions": [
				{"action": "INITIATE_DRIVER_SWAP", "asset": "TRUCK-17", "rationale": "Enables reroute without hours violation."},
				{"action": "CLOSE_COLD_STORE_DOOR", "asset": "COLD-03", "rationale": "Sentinel confidence 94%."},
				{"action": "DISPATCH_RECOUNT_TEAM", "asset": "WH-B", "rationale": "65-unit gap requires physical verification."},
			],
			"blocked_actions": [
				{"action": "REROUTE_NORTH", "reason": "DRIVER_HOURS_VIOLATION", "rule_violated": "Max 9 hours per shift"},
			],
			"escalate_to_human": True,
			"escalation_reason": "Driver swap and pharmaceutical assessment require manager approval.",
			"confidence_overall": 0.87,
			"resolution_reasoning": "Reroute blocked due to legal hours constraint. Driver swap unblocks delivery. Cold store action proceeds independently.",
		},
		"commander": {
			"event_id": "demo_event_001",
			"executive_summary": "Three simultaneous supply chain incidents were detected and resolved autonomously. A driver swap was initiated to unblock a reroute, a cold storage door was flagged for immediate closure, and a warehouse recount was dispatched. Two actions require human sign-off within the next five minutes.",
			"incidents_resolved": 3,
			"actions_taken": [
				"Driver swap initiated for TRUCK-17.",
				"Cold store COLD-03 flagged for door closure.",
				"Recount team dispatched to WH-B.",
			],
			"actions_blocked": [
				"Northern reroute blocked — driver would exceed legal hours.",
			],
			"requires_human_attention": True,
			"human_attention_reason": "Driver swap coordination and pharmaceutical stock assessment require manager approval within 5 minutes.",
			"confidence_overall": 0.87,
			"estimated_cost_impact": "Prevented estimated ₹6,02,000 in combined losses.",
			"incident_report": {
				"title": "Triple Simultaneous Anomaly — Route / Inventory / Cold Chain",
				"timestamp": "2024-01-15T14:32:07Z",
				"duration_seconds": 8,
				"root_causes": [
					"Vehicle navigated around an unreported road closure.",
					"Warehouse RFID gap caused by loading error at origin.",
					"Cold store door left open after access event.",
				],
				"resolution": "Driver swap ordered. Cold store closure dispatched. Recount team en route. Human escalation raised.",
			},
		},
	}


def _infer_agent_key(system_prompt: str) -> str:
	prompt = system_prompt.lower()
	# Match the self-identification phrase unique to each agent's system prompt
	if "you are the commander agent" in prompt:
		return "commander"
	if "you are the arbiter agent" in prompt:
		return "arbiter"
	if "you are the sentinel agent" in prompt:
		return "sentinel"
	if "you are the audit agent" in prompt:
		return "audit"
	if "you are the router agent" in prompt:
		return "router"
	return "scout"


def load_mock_response(agent_key: str) -> Dict[str, Any]:
	mock_file = _project_root() / "mock_responses" / "crisis_scenario.json"
	fallback = _default_mock_payloads()
	if not mock_file.exists():
		return fallback.get(agent_key, {"event_id": "unknown", "error": "no_mock_available"})

	try:
		raw = mock_file.read_text(encoding="utf-8").strip()
		if not raw:
			return fallback.get(agent_key, {"event_id": "unknown", "error": "no_mock_available"})
		parsed = json.loads(raw)
		if isinstance(parsed, dict):
			if agent_key in parsed and isinstance(parsed[agent_key], dict):
				return parsed[agent_key]
			upper_key = agent_key.upper()
			if upper_key in parsed and isinstance(parsed[upper_key], dict):
				return parsed[upper_key]
			if "agents" in parsed and isinstance(parsed["agents"], dict):
				maybe = parsed["agents"].get(agent_key)
				if isinstance(maybe, dict):
					return maybe
			if agent_key == "scout" and "anomalies" in parsed:
				return parsed
			if agent_key == "router" and "proposed_action" in parsed:
				return parsed
	except Exception:
		pass
	return fallback.get(agent_key, {"event_id": "unknown", "error": "no_mock_available"})


async def call_llm(system_prompt: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
	agent_key = _infer_agent_key(system_prompt)

	if os.getenv("DEMO_MODE", "false").lower() == "true":
		return load_mock_response(agent_key)

	try:
		client = get_anthropic_client()
		response = await client.messages.create(
			model=MODEL_NAME,
			max_tokens=1200,
			system=system_prompt,
			messages=[
				{
					"role": "user",
					"content": json.dumps(user_data, ensure_ascii=True),
				}
			],
		)

		text_chunks: list[str] = []
		for content_block in response.content:
			text = getattr(content_block, "text", None)
			if text:
				text_chunks.append(text)

		raw_text = "\n".join(text_chunks)
		cleaned = _strip_markdown_fences(raw_text)
		return json.loads(cleaned)
	except Exception:
		return load_mock_response(agent_key)
