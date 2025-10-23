"""papers_crawler package"""

# Regular sync API (for scripts and Streamlit)
from .crawler import crawl, discover_journals

# Async API (for Colab/Jupyter notebooks)
from .crawler_async import crawl_async, discover_journals_async

from .crawl_text_async import crawl_text_async

__all__ = [
    "crawl",
    "discover_journals",
    "crawl_async",
    "discover_journals_async",
    "crawl_colab",
    "discover_journals_colab",
    "crawl_text_async",
]
