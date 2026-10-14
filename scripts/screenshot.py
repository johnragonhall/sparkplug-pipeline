"""Capture docs/dashboard.png from the running stack with headless Chromium.

    uv run --no-project --with playwright==1.55.0 scripts/screenshot.py [--channel msedge]

Without --channel, run `uv run --no-project --with playwright==1.55.0 playwright install
chromium` once. With --channel msedge or chrome, Playwright drives an installed browser.
"""

import argparse
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parent.parent / "docs" / "dashboard.png"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:18080/")
    parser.add_argument(
        "--channel", default=None, help="msedge or chrome to use an installed browser"
    )
    args = parser.parse_args()
    with sync_playwright() as p:
        browser = p.chromium.launch(channel=args.channel, headless=True)
        page = browser.new_page(viewport={"width": 1240, "height": 900}, color_scheme="light")
        page.goto(args.url)
        page.wait_for_selector("body[data-ready='true']", timeout=30_000)
        page.screenshot(path=str(OUT), full_page=True)
        browser.close()
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
