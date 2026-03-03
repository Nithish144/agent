"""
LLM Reasoner — Sends current state + gaps to OpenRouter (Trinity Large Preview)
and gets a tool decision back as JSON.

The LLM only REASONS. It never executes raw commands.
Uses OpenRouter's OpenAI-compatible API endpoint.
"""

import json
import logging
import requests
from config.settings import Settings
from config.prompts import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


class LLMReasoner:
    def __init__(self, settings: Settings):
        self.api_key = settings.openrouter_api_key
        self.model = settings.llm_model
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/hadoop-ai-agent",
            "X-Title": "Hadoop AI Agent",
        }

    def decide(
        self,
        current_state: dict,
        gaps: list[dict],
        override_instruction: str = None,
    ) -> dict | None:
        """
        Ask Trinity Large Preview to decide which tool to call next.
        Returns parsed JSON decision or None on failure.

        override_instruction: if provided, prepended to the user message as a
        hard directive — used to break infinite loops when daemons fail to start.
        """
        user_message = self._build_user_message(current_state, gaps, override_instruction)
        logger.debug(f"Sending to LLM:\n{user_message}")

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "max_tokens": 512,
            "temperature": 0.1,  # Low temp = deterministic tool selection
        }

        try:
            response = requests.post(
                OPENROUTER_URL,
                headers=self.headers,
                json=payload,
                timeout=30,
            )
            response.raise_for_status()

            data = response.json()
            raw = data["choices"][0]["message"]["content"].strip()
            logger.debug(f"LLM raw response: {raw}")

            # Strip markdown fences if model wraps output in ```json ... ```
            if raw.startswith("```"):
                parts = raw.split("```")
                raw = parts[1] if len(parts) > 1 else raw
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            decision = json.loads(raw)
            return decision

        except requests.exceptions.HTTPError as e:
            logger.error(f"OpenRouter HTTP error: {e.response.status_code} — {e.response.text}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"LLM returned invalid JSON: {e}\nRaw: {raw}")
            return None

    def _build_user_message(
        self,
        current_state: dict,
        gaps: list[dict],
        override_instruction: str = None,
    ) -> str:
        override_block = ""
        if override_instruction:
            override_block = f"""⚠️  AGENT OVERRIDE — FOLLOW THIS BEFORE ANYTHING ELSE:
{override_instruction}

"""
        return f"""{override_block}CURRENT CLUSTER STATE:
{json.dumps(current_state, indent=2)}

GAPS (not yet meeting goal):
{json.dumps(gaps, indent=2)}

Decide the single best tool call to make progress toward the goal state.
Respond ONLY with valid JSON in the required format."""
