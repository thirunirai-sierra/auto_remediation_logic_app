"""Project entry point."""

from __future__ import annotations

import sys

from cli import main as remediation_main


if __name__ == "__main__":
    # Route to knowledge CLI if first arg is 'kb'
    if len(sys.argv) > 1 and sys.argv[1] == "kb":
        from agent.knowledge.cli import main as knowledge_main

        raise SystemExit(knowledge_main(sys.argv[2:]))
    else:
        raise SystemExit(remediation_main(sys.argv[1:]))