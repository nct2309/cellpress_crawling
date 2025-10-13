"""Deprecated UI shim.

The original Tkinter-based desktop UI was removed to avoid requiring the OS
`python3-tk` package. Use the Streamlit web UI (`scripts/run_crawler_streamlit.py`)
or the CLI mode in `scripts/run_crawler.py` instead.

This module provides a small `main()` function that raises an informative
error if called directly.
"""

from __future__ import annotations

def main() -> None:
    raise RuntimeError(
        "The Tkinter desktop UI has been removed.\n"
        "Please use the Streamlit UI: `poetry run streamlit run scripts/run_crawler_streamlit.py`\n"
        "Or use the CLI mode: `poetry run python scripts/run_crawler.py --keywords 'kw1,kw2' --year-from 2018 --year-to 2021 --out ./pdfs`"
    )
