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
    """Remove surrounding ```json ... ``` or ``` ... ``` fences from LLM output."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        # Drop the opening fence line (e.g. ```json or ```)
        lines = lines[1:]
        # Drop the closing fence line if present
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
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
    }


def _infer_agent_key(system_prompt: str) -> str:
    prompt = system_prompt.lower()
    if "scout" in prompt:
        return "scout"
    if "router" in prompt:
        return "router"
    return "scout"


def load_mock_response(agent_key: str) -> Dict[str, Any]:
    mock_file = _project_root() / "LogistiQ" / "mock_responses" / "crisis_scenario.json"
    fallback = _default_mock_payloads()
    if not mock_file.exists():
        return fallback.get(agent_key, fallback["scout"])

    try:
        raw = mock_file.read_text(encoding="utf-8").strip()
        if not raw:
            return fallback.get(agent_key, fallback["scout"])
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
    return fallback.get(agent_key, fallback["scout"])


async def call_llm(system_prompt: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call the Anthropic LLM with the given system prompt and user data.

    In DEMO_MODE, returns a pre-canned mock response instead of a real API call.
    Markdown code fences (```json ... ```) are automatically stripped so the
    return value is always a valid JSON-deserialisable dict.
    """
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
