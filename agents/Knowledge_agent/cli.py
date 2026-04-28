"""Knowledge Agent CLI."""

import argparse
from agents.Knowledge_agent.knowledge_base import KnowledgeAgent


def main(args_list):
    """Main entry point for knowledge CLI."""
    parser = create_parser()
    args = parser.parse_args(args_list)
    
    if not args.command:
        parser.print_help()
        return 0
    
    return execute_command(args)


def create_parser():
    """Create argument parser."""
    parser = argparse.ArgumentParser(prog="python main.py kb")
    subparsers = parser.add_subparsers(dest="command")
    
    p = subparsers.add_parser("add-url", help="Add single URL (skips if exists)")
    p.add_argument("url", help="Microsoft Learn URL")
    p.add_argument("--category", help="Optional category")
    p.add_argument("--no-vectorize", action="store_true")
    
    subparsers.add_parser("stats", help="Show statistics")
    
    p = subparsers.add_parser("search", help="Search documentation")
    p.add_argument("query", nargs="+")
    p.add_argument("--top-k", type=int, default=5)
    
    return parser


def execute_command(args):
    """Execute the command."""
    kb = KnowledgeAgent()
    
    if args.command == "stats":
        stats = kb.get_stats()
        print(f"\n📊 Total: {stats['total']}, Vectorized: {stats['vectorized']}, Pending: {stats['pending']}")
    
    elif args.command == "search":
        query = " ".join(args.query)
        print(f"\n🔍 Searching: '{query}'")
        results = kb.search(query, top_k=args.top_k)
        for r in results:
            print(f"[{r['similarity']:.1f}%] {r['meta'].get('title', 'Unknown')[:60]}")
    
    elif args.command == "add-url":
        result = kb.add_url(args.url, args.category, args.no_vectorize)
        print(f"\n{result['message']}")
    
    return 0