#!/usr/bin/env python3
"""
main.py — CLI entrypoint for the Hadoop AI Agent.

Usage:
  python main.py run              # Run agent loop
  python main.py run --dry-run    # Simulate without executing
  python main.py state            # Print current cluster state
  python main.py api              # Start FastAPI server
"""

import sys
import json
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def cmd_run(args):
    from agent.agent import HadoopAgent
    from config.settings import Settings

    settings = Settings()
    if args.dry_run:
        settings.dry_run = True
    if args.max_iterations:
        settings.max_iterations = args.max_iterations

    try:
        settings.validate()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    agent = HadoopAgent(settings)
    result = agent.run()

    print("\n" + "=" * 50)
    print("AGENT COMPLETED")
    print("=" * 50)
    print(json.dumps(result, indent=2))


def cmd_state(args):
    from agent.state_detector import StateDetector
    from config.goal_state import GOAL_STATE
    from agent.goal_comparator import GoalComparator

    detector = StateDetector()
    state = detector.collect()
    gaps = GoalComparator(GOAL_STATE).find_gaps(state)

    print("\n📊 CURRENT STATE:")
    print(json.dumps(state, indent=2))
    print("\n🎯 GAPS:")
    print(json.dumps(gaps, indent=2) if gaps else "  ✅ No gaps — goal state achieved!")


def cmd_api(args):
    import uvicorn
    from config.settings import Settings

    settings = Settings()
    uvicorn.run(
        "api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level="info",
    )


def main():
    parser = argparse.ArgumentParser(description="Hadoop AI Agent")
    sub = parser.add_subparsers(dest="command")

    # run
    run_parser = sub.add_parser("run", help="Start the agent loop")
    run_parser.add_argument("--dry-run", action="store_true", help="Simulate without executing tools")
    run_parser.add_argument("--max-iterations", type=int, help="Override max iterations")

    # state
    sub.add_parser("state", help="Print current cluster state and gaps")

    # api
    sub.add_parser("api", help="Start FastAPI server")

    args = parser.parse_args()

    if args.command == "run":
        cmd_run(args)
    elif args.command == "state":
        cmd_state(args)
    elif args.command == "api":
        cmd_api(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()