"""Async Playwright-based crawler for Cell.com PDFs.

This module provides async versions of the crawler functions for use in
environments with asyncio event loops (like Jupyter/Colab).
"""
from __future__ import annotations

import os
import time
import logging
from typing import List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def crawl_async(
    keywords: str = "",
    year_from: int = 2020,
    year_to: int = 2024,
    out_folder: str = "papers",
    headless: bool = True,
    limit: Optional[int] = None,
    journal_slugs: Optional[List[str]] = None,
    progress_callback=None,
    total_progress_callback=None,
) -> Tuple[List[str], List[str]]:
    """Async crawl Cell.com for articles matching keywords, year range, and optionally specific journals.
    
    If journal_slugs is provided, crawls from each journal's /newarticles page.
    Otherwise, uses keyword search across all journals.
    
    Args:
        progress_callback: Called with (filename, filepath) after each file is downloaded
        total_progress_callback: Called with (current, total, status_message, file_size, speed_kbps, stage) to update overall progress
    
    Returns:
        Tuple[List[str], List[str]]: (downloaded_file_paths, open_access_article_names)
    """
    import asyncio

    os.makedirs(out_folder, exist_ok=True)
    downloaded_files = []
    open_access_articles = []
    total_articles_found = 0

    async with async_playwright() as p:
        # Use Firefox with PDF download preferences
        logger.info("Launching Firefox browser...")
        browser = await p.firefox.launch(
            headless=headless,
            firefox_user_prefs={
                "pdfjs.disabled": True,
                "browser.helperApps.neverAsk.saveToDisk": "application/pdf",
                "browser.download.folderList": 2,
                "browser.download.manager.showWhenStarting": False,
                "browser.download.dir": os.path.abspath(out_folder),
                "plugin.disable_full_page_plugin_for_types": "application/pdf",
            }
        )
        
        context = await browser.new_context(
            accept_downloads=True,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0',
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York',
            permissions=['geolocation'],
            geolocation={'longitude': -74.0060, 'latitude': 40.7128},
            color_scheme='light',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        
        page = await context.new_page()
        
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        logger.info("Firefox browser ready")

        found_count = 0

        async def handle_cookie_consent(page):
            """Try to accept cookie consent if it appears."""
            try:
                cookie_selectors = [
                    'button:has-text("Accept")',
                    'button:has-text("Accept all")',
                    'button:has-text("Accept All")',
                    'button:has-text("I Accept")',
                    'button:has-text("I agree")',
                    'button:has-text("Agree")',
                    'button:has-text("OK")',
                    'button[id*="accept"]',
                    'button[class*="accept"]',
                    'a:has-text("Accept")',
                    '#onetrust-accept-btn-handler',
                    '.optanon-alert-box-button-middle',
                ]
                
                for selector in cookie_selectors:
                    try:
                        if await page.locator(selector).is_visible(timeout=2000):
                            logger.info(f"Found cookie consent button: {selector}")
                            await page.click(selector, timeout=3000)
                            logger.info("✓ Accepted cookie consent")
                            await page.wait_for_timeout(1000)
                            return True
                    except Exception:
                        continue
                        
            except Exception as e:
                logger.debug(f"No cookie consent found or already accepted: {e}")
            
            return False

        if journal_slugs:
            if total_progress_callback:
                total_progress_callback(0, 0, "Scanning journals for open access articles...", 0, 0, "scanning")
            
            for slug in journal_slugs:
                if limit and found_count >= limit:
                    break
                
                journal_folder = os.path.join(out_folder, slug.replace('/', '_'))
                os.makedirs(journal_folder, exist_ok=True)
                logger.info(f"Journal folder: {journal_folder}")
                
                url = f"https://www.cell.com/{slug}/newarticles"
                logger.info(f"Crawling journal: {slug} at {url}")
                
                if total_progress_callback:
                    total_progress_callback(found_count, total_articles_found, f"Loading journal: {slug}", 0, 0, "loading")
                
                await page.goto(url, timeout=30000)
                await page.wait_for_timeout(3000)
                
                await handle_cookie_consent(page)
                
                page_title = await page.title()
                if "Just a moment" in page_title or "Cloudflare" in page_title:
                    raise Exception(f"Cloudflare challenge detected on {url}. The website is blocking automated requests. Please try again later or use a VPN.")
                
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                articles = soup.select(".articleCitation")
                
                if not articles:
                    logger.warning(f"No articles found on {url}. Page title: {page_title}")
                    all_divs = soup.find_all("div")
                    logger.info(f"Found {len(all_divs)} div elements on page")
                    continue
                
                oa_count = sum(1 for art in articles if art.find(class_="OALabel"))
                total_articles_found += min(oa_count, limit - found_count) if limit else oa_count
                logger.info(f"Found {oa_count} open access articles in {slug}")
                
                if total_progress_callback:
                    total_progress_callback(found_count, total_articles_found, f"Found {total_articles_found} open access articles", 0, 0, "found")
                
                for art in articles:
                    if limit and found_count >= limit:
                        break
                    
                    year_tag = art.find(class_="toc__item__date")
                    year_text = year_tag.get_text() if year_tag else ""
                    try:
                        year_match = None
                        for y in range(year_from, year_to+1):
                            if str(y) in year_text:
                                year_match = y
                                break
                        if not year_match:
                            continue
                        year = year_match
                    except Exception:
                        continue
                    
                    if not (year_from <= year <= year_to):
                        continue
                    
                    pdf_link = None
                    pdf_a = art.find("a", class_="pdfLink")
                    if pdf_a:
                        pdf_link = pdf_a.get("href", "")
                    
                    if not pdf_link:
                        continue
                    
                    oa_label = art.find(class_="OALabel")
                    if not oa_label:
                        logger.debug(f"Skipping non-open-access article: {pdf_link}")
                        continue
                    
                    title_elem = art.find(class_="toc__item__title")
                    article_title = title_elem.get_text(strip=True) if title_elem else f"Article {found_count + 1}"
                    
                    logger.info(f"Found open-access article: {article_title}")
                    
                    try:
                        safe_title = "".join(c for c in article_title if c.isalnum() or c in (' ', '-', '_')).strip()
                        safe_title = safe_title[:100]
                        filename = f"{safe_title}.pdf"
                        dest_path = os.path.join(journal_folder, filename)
                        
                        if total_progress_callback:
                            total_progress_callback(found_count, total_articles_found, f"Downloading: {article_title[:50]}...", 0, 0, "starting")
                        
                        logger.info(f"Clicking PDF link for: {article_title}")
                        
                        download_start_time = time.time()
                        
                        async with page.expect_download(timeout=30000) as download_info:
                            pdf_selector = f'a.pdfLink[href="{pdf_link}"]'
                            await page.click(pdf_selector, timeout=10000)
                        
                        download = await download_info.value
                        
                        if total_progress_callback:
                            total_progress_callback(found_count, total_articles_found, f"Saving: {article_title[:50]}...", 0, 0, "downloading")
                        
                        await download.save_as(dest_path)
                        
                        download_time = time.time() - download_start_time
                        
                        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000:
                            file_size = os.path.getsize(dest_path)
                            file_size_kb = file_size / 1024
                            
                            if download_time > 0:
                                speed_kbps = file_size_kb / download_time
                            else:
                                speed_kbps = 0
                            
                            logger.info(f"✓ Successfully downloaded PDF: {filename} ({file_size_kb:.1f} KB) in {download_time:.1f}s @ {speed_kbps:.1f} KB/s")
                            
                            downloaded_files.append(dest_path)
                            open_access_articles.append(article_title)
                            found_count += 1
                            
                            if progress_callback:
                                progress_callback(filename, dest_path)
                            
                            if total_progress_callback:
                                total_progress_callback(found_count, total_articles_found, f"Downloaded: {filename[:40]}...", file_size, speed_kbps, "completed")
                        else:
                            logger.error(f"Downloaded file is too small or doesn't exist: {dest_path}")
                            
                    except Exception as e:
                        logger.error(f"Failed to download PDF for '{article_title}': {e}")
                        continue
                    
                    await asyncio.sleep(1)

        await context.close()
        await browser.close()

    logger.info(f"Downloaded {found_count} PDFs to {out_folder}")
    return downloaded_files, open_access_articles


async def discover_journals_async(force_refresh: bool = False) -> List[Tuple[str, str]]:
    """Async discover journals from Cell.com's navbar by parsing the Journals menu.

    Returns a list of (slug, display_name). Caches results in .cache/papers_crawler/journals.json
    """
    import json
    import re

    cache_dir = os.path.join(os.getcwd(), ".cache", "papers_crawler")
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, "journals.json")
    
    if not force_refresh and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf8") as f:
                cached = json.load(f)
                if cached:
                    logger.info(f"Loaded {len(cached)} journals from cache")
                    return cached
        except Exception:
            pass

    results: List[Tuple[str, str]] = []
    
    logger.info("Fetching journals from Cell.com with Playwright...")
    try:
        async with async_playwright() as p:
            browser = await p.firefox.launch(
                headless=True,
                firefox_user_prefs={
                    "pdfjs.disabled": True,
                    "browser.helperApps.neverAsk.saveToDisk": "application/pdf",
                }
            )
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0',
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York'
            )
            page = await context.new_page()
            
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            logger.info("Loading Cell.com homepage...")
            await page.goto("https://www.cell.com", timeout=60000, wait_until="domcontentloaded")
            
            try:
                await page.wait_for_selector("ul.mega-menu, nav, header", timeout=10000)
                logger.info("Navigation menu loaded")
            except Exception as e:
                logger.warning(f"Could not find navigation menu: {e}")
            
            await page.wait_for_timeout(3000)
            
            html = await page.content()
            logger.info(f"Retrieved page content: {len(html)} bytes")
            
            await context.close()
            await browser.close()
            
            soup = BeautifulSoup(html, "html.parser")
            journals_panel = soup.find('div', id='main-menu-panel-1')
            
            if not journals_panel:
                logger.warning("Could not find Journals menu panel (main-menu-panel-1)")
                journals_panel = soup
            
            all_links = journals_panel.find_all("a", href=True)
            logger.info(f"Found {len(all_links)} total links in Journals section")
            
            seen = set()
            for a in all_links:
                href = a.get("href", "")
                text = a.get_text(strip=True)
                
                if not text or len(text) < 2:
                    continue
                
                if 'sub-menu__item-link' not in a.get('class', []):
                    continue
                
                slug = None
                
                match = re.match(r'^/([a-z0-9\-]+)/home$', href)
                if match:
                    slug = match.group(1)
                
                elif re.match(r'^/([a-z0-9\-]+/[a-z0-9\-]+)/home$', href):
                    match = re.match(r'^/([a-z0-9\-]+/[a-z0-9\-]+)/home$', href)
                    if match:
                        slug = match.group(1)
                
                elif re.match(r'^/([a-z0-9\-]+)$', href):
                    slug = href.strip('/')
                
                if slug:
                    clean_text = re.sub(r'\s*\([^)]*\)\s*$', '', text).strip()
                    clean_text = re.sub(r'\s+partner\s*$', '', clean_text, flags=re.IGNORECASE).strip()
                    clean_text = re.sub(r'<[^>]+>', '', clean_text).strip()
                    
                    if slug and clean_text and slug not in seen:
                        seen.add(slug)
                        results.append((slug, clean_text))
                        logger.debug(f"Found journal: {slug} -> {clean_text}")
            
            if results:
                logger.info(f"Successfully discovered {len(results)} journals from Cell.com")
                try:
                    with open(cache_file, "w", encoding="utf8") as f:
                        json.dump(results, f, ensure_ascii=False, indent=2)
                    logger.info(f"Cached {len(results)} journals to {cache_file}")
                except Exception as e:
                    logger.warning(f"Failed to cache journals: {e}")
                return results
            else:
                raise Exception("No journals found on Cell.com - page structure may have changed")
                
    except Exception as e:
        logger.error(f"Failed to discover journals from Cell.com: {e}")
        raise Exception(f"Could not load journals from Cell.com. Error: {str(e)}. Please check your internet connection and try again.")
