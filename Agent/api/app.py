"""
FastAPI Backend — REST API for the Hadoop AI Agent.
Allows external systems to trigger the agent, check status, and view logs.
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import logging
import asyncio
import uuid
from datetime import datetime

from agent.agent import HadoopAgent
from agent.state_detector import StateDetector
from config.settings import Settings
from config.goal_state import GOAL_STATE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Hadoop AI Agent API",
    description="Goal-Based Infrastructure AI Agent for Hadoop HDFS",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

settings = Settings().validate()

# In-memory run store (use Redis/DB in production)
runs: dict = {}


class RunRequest(BaseModel):
    dry_run: bool = False
    max_iterations: Optional[int] = None


class RunResponse(BaseModel):
    run_id: str
    status: str
    message: str


@app.get("/")
def root():
    return {
        "agent": "Hadoop AI Agent",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/state")
def get_state():
    """Get current cluster state snapshot."""
    detector = StateDetector()
    state = detector.collect()
    return {"state": state, "goal": GOAL_STATE}


@app.get("/goal")
def get_goal():
    """Return the defined goal state."""
    return {"goal_state": GOAL_STATE}


@app.post("/run", response_model=RunResponse)
def start_run(request: RunRequest, background_tasks: BackgroundTasks):
    """Start the agent loop in the background."""
    run_id = str(uuid.uuid4())[:8]
    runs[run_id] = {"status": "running", "started_at": datetime.utcnow().isoformat(), "result": None}

    run_settings = Settings(
        anthropic_api_key=settings.anthropic_api_key,
        llm_model=settings.llm_model,
        dry_run=request.dry_run,
        max_iterations=request.max_iterations or settings.max_iterations,
    )

    def run_agent():
        try:
            agent = HadoopAgent(run_settings)
            result = agent.run()
            runs[run_id]["status"] = result.get("status", "done")
            runs[run_id]["result"] = result
            runs[run_id]["finished_at"] = datetime.utcnow().isoformat()
        except Exception as e:
            runs[run_id]["status"] = "error"
            runs[run_id]["error"] = str(e)

    background_tasks.add_task(run_agent)

    return RunResponse(
        run_id=run_id,
        status="started",
        message=f"Agent started. Poll /run/{run_id} for status.",
    )


@app.get("/run/{run_id}")
def get_run_status(run_id: str):
    """Get status and logs of a specific agent run."""
    if run_id not in runs:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found.")
    return runs[run_id]


@app.get("/runs")
def list_runs():
    """List all agent runs."""
    return {
        "count": len(runs),
        "runs": {rid: {"status": r["status"], "started_at": r["started_at"]} for rid, r in runs.items()},
    }


@app.delete("/runs")
def clear_runs():
    """Clear run history."""
    runs.clear()
    return {"cleared": True}