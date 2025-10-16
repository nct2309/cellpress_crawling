"""Helper functions for using the crawler in Google Colab or Jupyter notebooks.

Use these async versions when running in environments with asyncio event loops.
"""
import asyncio
from src.papers_crawler.crawler_async import crawl_async, discover_journals_async


def crawl_colab(
    keywords: str = "",
    year_from: int = 2020,
    year_to: int = 2024,
    out_folder: str = "papers",
    headless: bool = True,
    limit: int = 10,
    journal_slugs=None,
):
    """Wrapper for crawl_async that works in Colab/Jupyter.
    
    Example usage in Colab:
        from src.papers_crawler.colab_helper import crawl_colab, discover_journals_colab
        
        # Discover journals
        journals = discover_journals_colab()
        print(f"Found {len(journals)} journals")
        
        # Crawl specific journals
        crawl_colab(
            year_from=2020,
            year_to=2024,
            out_folder="./papers",
            headless=True,
            limit=10,
            journal_slugs=["cell", "immunity", "neuron"],
        )
    """
    return asyncio.get_event_loop().run_until_complete(
        crawl_async(
            keywords=keywords,
            year_from=year_from,
            year_to=year_to,
            out_folder=out_folder,
            headless=headless,
            limit=limit,
            journal_slugs=journal_slugs,
        )
    )


def discover_journals_colab(force_refresh: bool = False):
    """Wrapper for discover_journals_async that works in Colab/Jupyter.
    
    Example usage in Colab:
        from src.papers_crawler.colab_helper import discover_journals_colab
        
        journals = discover_journals_colab()
        print(f"Found {len(journals)} journals")
        for slug, name in journals[:5]:
            print(f"  {slug}: {name}")
    """
    return asyncio.get_event_loop().run_until_complete(
        discover_journals_async(force_refresh=force_refresh)
    )
