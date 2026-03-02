"""
Settings — Environment-driven configuration for the Hadoop AI Agent.
Uses OpenRouter API with arcee-ai/trinity-large-preview:free model.
"""

import os


class Settings:
    def __init__(self, openrouter_api_key=None, llm_model=None,
                 max_iterations=None, loop_delay_seconds=None,
                 dry_run=None, api_host=None, api_port=None):

        self.openrouter_api_key = openrouter_api_key or os.environ.get("OPENROUTER_API_KEY", "")
        self.llm_model = llm_model or os.environ.get("LLM_MODEL", "arcee-ai/trinity-large-preview:free")
        self.max_iterations = max_iterations if max_iterations is not None else int(os.environ.get("MAX_ITERATIONS", "20"))
        self.loop_delay_seconds = loop_delay_seconds if loop_delay_seconds is not None else float(os.environ.get("LOOP_DELAY_SECONDS", "3"))
        self.dry_run = dry_run if dry_run is not None else (os.environ.get("DRY_RUN", "false").lower() == "true")
        self.api_host = api_host or os.environ.get("API_HOST", "0.0.0.0")
        self.api_port = api_port or int(os.environ.get("API_PORT", "8000"))

    def validate(self):
        if not self.openrouter_api_key:
            raise ValueError("OPENROUTER_API_KEY environment variable is required.")
        return self