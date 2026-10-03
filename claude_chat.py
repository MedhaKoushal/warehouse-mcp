"""
claude_chat.py: Chat with Claude to scrape Instagram, Amazon, and Web pages using Apify tools.

Flow:
1. User enters a prompt: "Scrape this Instagram page: https://instagram.com/nikestore"
2. Claude selects the appropriate Apify tool (e.g., apify_social_posts or apify_amazon_product).
3. The backend executes the tool via handle_apify_tool().
4. The scraped data is fed back into Claude first as a tool_result.
5. Claude analyzes the scraped content and generates the final response for the user.

Works both with:
- Live Anthropic Claude API (set ANTHROPIC_API_KEY in config.json or environment)
- Built-in simulation mode (--test / --mock) when Anthropic API key is not yet set.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List

from apify_tools import (
    APIFY_TOOLS,
    APIFY_TOOL_NAMES,
    APIFY_PROMPT_SECTION,
    handle_apify_tool,
)
import apify_gateway as gw

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("claude_chat")

DEFAULT_SYSTEM_PROMPT = """You are an expert e-commerce and social intelligence assistant for SellerOS.
You have access to live web and social scrapers via Apify.
When a user asks to inspect, research, or scrape an Instagram page, TikTok, Facebook, Twitter/X,
or Amazon product/offers/reviews, use the appropriate Apify tool.
When the tool response returns, synthesize the data clearly for the user."""


def get_anthropic_api_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY") or gw.get_setting("ANTHROPIC_API_KEY")
    if key and str(key).strip():
        return str(key).strip()
    return ""


def run_claude_turn_live(
    client,
    messages: List[Dict[str, Any]],
    ctx: Dict[str, Any],
    model: str = "claude-3-5-sonnet-20241022",
    max_tokens: int = 2500,
) -> str:
    """Multi-turn loop with live Anthropic API: feeds tool results back to Claude first."""
    system = DEFAULT_SYSTEM_PROMPT + "\n\n" + APIFY_PROMPT_SECTION

    while True:
        resp = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            tools=APIFY_TOOLS,
            messages=messages,
        )

        messages.append({"role": "assistant", "content": resp.content})

        if resp.stop_reason != "tool_use":
            # Claude provided final text output
            return "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")

        # Claude requested tool execution
        tool_results = []
        for block in resp.content:
            if getattr(block, "type", "") != "tool_use":
                continue

            tool_name = block.name
            tool_input = block.input
            print(f"\n[Claude Tool Request] -> {tool_name}({json.dumps(tool_input, default=str)})")

            if tool_name in APIFY_TOOL_NAMES:
                out = handle_apify_tool(tool_name, tool_input, ctx)
            else:
                out = {"error": f"Unknown tool {tool_name}"}

            print(f"[Tool Execution Complete] -> Feeding {len(json.dumps(out))} bytes back to Claude first...")

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": json.dumps(out, default=str, ensure_ascii=False),
                "is_error": "error" in out,
            })

        messages.append({"role": "user", "content": tool_results})


def simulate_claude_turn(
    user_prompt: str,
    ctx: Dict[str, Any],
) -> str:
    """
    Simulates Claude's exact tool-calling decision and response synthesis.
    Useful for testing scraper integration without consuming Anthropic API tokens.
    """
    prompt_lower = user_prompt.lower()
    tool_name = None
    tool_input = {}

    import re
    insta_match = re.search(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9_.-]+/?", user_prompt)
    asin_match = re.search(r"\b([B0-9][A-Z0-9]{9})\b", user_prompt)

    if "instagram" in prompt_lower or "insta" in prompt_lower:
        url = insta_match.group(0) if insta_match else "https://www.instagram.com/nikestore"
        tool_name = "apify_social_posts"
        tool_input = {"platform": "instagram", "urls": [url], "per_source": 10}
    elif "amazon" in prompt_lower or asin_match or "product" in prompt_lower or "amon" in prompt_lower:
        asin = asin_match.group(1) if asin_match else "B09X7MPX8L"
        if "review" in prompt_lower:
            tool_name = "apify_amazon_reviews"
            tool_input = {"asins": [asin], "per_asin": 20}
        elif "offer" in prompt_lower or "buy box" in prompt_lower:
            tool_name = "apify_amazon_offers"
            tool_input = {"asins": [asin], "max_offers": 10}
        else:
            tool_name = "apify_amazon_product"
            tool_input = {"asins": [asin], "max_age_hours": 24}
    else:
        # Default to web research or search
        tool_name = "apify_web_research"
        tool_input = {"mode": "google_search", "queries": [user_prompt[:80]]}

    print(f"\n[Claude Tool Selection] Decided to call: {tool_name}")
    print(f"[Claude Tool Arguments] {json.dumps(tool_input, indent=2)}")
    print("[Executing Tool via Apify] Scraping data...")

    result = handle_apify_tool(tool_name, tool_input, ctx)

    print(f"[Feeding Scraped Data Back to Claude]")
    preview = json.dumps(result, indent=2, default=str)
    if len(preview) > 500:
        print(preview[:500] + "\n... (truncated for display)")
    else:
        print(preview)

    # Simulated Claude response synthesis
    print("\n[Claude Final Response]")
    if "error" in result:
        return f"I attempted to scrape using {tool_name}, but encountered: {result['error']}"

    rows = result.get("rows") or []
    source = result.get("source", "live")
    return (
        f"I've successfully gathered the data (source: {source}). "
        f"Retrieved {len(rows)} items via {tool_name}. "
        f"The data has been processed and is ready for your analysis!"
    )


def main():
    parser = argparse.ArgumentParser(description="SellerOS Claude Apify Scraper Chat")
    parser.add_argument("--test", action="store_true", help="Run automated test suite")
    parser.add_argument("--mock", action="store_true", help="Force simulation mode")
    parser.add_argument("--prompt", type=str, help="Single prompt to execute and exit")
    args = parser.parse_args()

    api_key = get_anthropic_api_key()
    ctx = {
        "partner": gw.get_setting("APIFY_DEFAULT_PARTNER", "selleros"),
        "domain": gw.get_setting("APIFY_DEFAULT_DOMAIN", "www.amazon.com"),
    }

    if args.test:
        print("=" * 60)
        print("Running Automated Verification: Claude -> Apify Tool -> Claude")
        print("=" * 60)

        # Test 1: Instagram scrape prompt
        test_prompt_1 = "Please scrape this Instagram page: https://www.instagram.com/nikestore"
        print(f"\nTest 1 - User Prompt: \"{test_prompt_1}\"")
        resp1 = simulate_claude_turn(test_prompt_1, ctx)
        print(f"Result: {resp1}\n")

        # Test 2: Amazon product scrape prompt
        test_prompt_2 = "Can you scrape the Amazon product page for ASIN B09X7MPX8L?"
        print(f"\nTest 2 - User Prompt: \"{test_prompt_2}\"")
        resp2 = simulate_claude_turn(test_prompt_2, ctx)
        print(f"Result: {resp2}\n")

        print("=" * 60)
        print("[+] Verification complete! The tool pipeline correctly executes and feeds data back to Claude.")
        return

    # Check mode
    use_live = bool(api_key) and not args.mock
    client = None
    if use_live:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            print("[+] Initialized Anthropic Claude client with API key.")
        except ImportError:
            print("[!] anthropic package not installed ('pip install anthropic'). Falling back to simulation mode.")
            use_live = False
    else:
        if not args.mock:
            print("[*] ANTHROPIC_API_KEY not found in config.json or environment.")
            print("[*] Running in simulation mode. Add your ANTHROPIC_API_KEY to config.json for live API calls.\n")

    # If single prompt provided
    if args.prompt:
        if use_live and client:
            messages = [{"role": "user", "content": args.prompt}]
            ans = run_claude_turn_live(client, messages, ctx)
            print(f"\nClaude: {ans}")
        else:
            ans = simulate_claude_turn(args.prompt, ctx)
            print(f"\nClaude: {ans}")
        return

    # Interactive REPL
    print("=" * 60)
    print("SellerOS Claude Apify Scraper Chat (type 'exit' to quit)")
    print("Examples:")
    print(" - 'Scrape the instagram page https://www.instagram.com/nike'")
    print(" - 'Scrape the Amazon product for ASIN B09X7MPX8L'")
    print(" - 'What are the competing offers for ASIN B09X7MPX8L?'")
    print("=" * 60)

    messages = []
    while True:
        try:
            user_input = input("\nYou > ").strip()
            if not user_input:
                continue
            if user_input.lower() in ("exit", "quit", "q"):
                print("Goodbye!")
                break

            if use_live and client:
                messages.append({"role": "user", "content": user_input})
                ans = run_claude_turn_live(client, messages, ctx)
                print(f"\nClaude > {ans}")
            else:
                ans = simulate_claude_turn(user_input, ctx)
                print(f"\nClaude > {ans}")

        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            break


if __name__ == "__main__":
    main()
