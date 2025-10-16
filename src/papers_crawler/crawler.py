"""Playwright-based crawler for Cell.com PDFs.

This module provides a polite crawler that searches Cell articles by keyword(s)
and year range and downloads associated PDFs to a specified folder using
Playwright's sync API. Playwright bundles browser binaries and avoids
chromedriver/platform issues.
"""
from __future__ import annotations

import os
import time
import logging
from typing import List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)
from playwright_stealth import Stealth, ALL_EVASIONS_DISABLED_KWARGS
logging.basicConfig(level=logging.INFO)


def _print_page_as_pdf(page, url: str, dest_folder: str, title: str, progress_callback=None) -> Optional[str]:
    """Load fulltext page and print it as PDF."""
    os.makedirs(dest_folder, exist_ok=True)
    
    try:
        logger.info(f"Loading fulltext page: {url}")
        
        # Navigate to fulltext page
        page.goto(url, timeout=60000, wait_until="load")
        
        # Wait for network to be idle (all content loaded)
        logger.info("Waiting for page to fully load...")
        page.wait_for_load_state("networkidle", timeout=30000)
        
        # Wait a bit more for any dynamic content
        page.wait_for_timeout(3000)
        
        # Check if we hit a Cloudflare challenge
        page_title = page.title()
        if "cloudflare" in page_title.lower() or "challenge" in page_title.lower():
            logger.warning("Cloudflare challenge detected on fulltext page")
            raise Exception("Cloudflare challenge detected. Please try again later.")
        
        logger.info(f"Page loaded successfully: {page_title}")
        
        # Generate safe filename
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()
        safe_title = safe_title[:100]  # Limit length
        filename = f"{safe_title}.pdf"
        dest_path = os.path.join(dest_folder, filename)
        
        # Print page as PDF
        logger.info(f"Printing page as PDF: {filename}")
        pdf_bytes = page.pdf(
            path=dest_path,
            format='A4',
            print_background=True,
            margin={
                'top': '20px',
                'bottom': '20px', 
                'left': '20px',
                'right': '20px'
            },
            scale=0.8  # Slightly smaller to fit content better
        )
        
        # Verify the PDF was created
        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000:
            file_size = os.path.getsize(dest_path)
            logger.info(f"✓ Successfully created PDF: {filename} ({file_size/1024:.1f} KB)")
            
            if progress_callback:
                progress_callback(filename, dest_path)
                
            return dest_path
        else:
            raise Exception("PDF generation failed or file too small")
            
    except Exception as e:
        logger.error(f"Failed to print page as PDF {url}: {e}")
        raise

def crawl(
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
    """Crawl Cell.com for articles matching keywords, year range, and optionally specific journals.
    
    If journal_slugs is provided, crawls from each journal's /newarticles page.
    Otherwise, uses keyword search across all journals.
    
    Args:
        progress_callback: Called with (filename, filepath) after each file is downloaded
        total_progress_callback: Called with (current, total, status_message) to update overall progress
    
    Returns:
        Tuple[List[str], List[str]]: (downloaded_file_paths, open_access_article_names)
    """
    import time

    os.makedirs(out_folder, exist_ok=True)
    downloaded_files = []
    open_access_articles = []
    total_articles_found = 0

    stealth = Stealth(
        navigator_languages_override=("en-US", "en"),
        init_scripts_only=True
    )

    with sync_playwright() as p:
        # Use Firefox with PDF download preferences
        logger.info("Launching Firefox browser...")
        browser = p.firefox.launch(
            headless=headless,
            firefox_user_prefs={
                "pdfjs.disabled": True,  # Disable the built-in PDF viewer
                "browser.helperApps.neverAsk.saveToDisk": "application/pdf",  # Auto-download PDFs
                "browser.download.folderList": 2,  # Use custom download location
                "browser.download.manager.showWhenStarting": False,  # Don't show download manager
                "browser.download.dir": os.path.abspath(out_folder),  # Set download directory
                "plugin.disable_full_page_plugin_for_types": "application/pdf",  # Disable PDF plugin
            }
        )
        
        # Create context with realistic browser fingerprint and accept downloads
        context = browser.new_context(
            accept_downloads=True,  # Enable download handling
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0',
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York',
            permissions=['geolocation'],
            geolocation={'longitude': -74.0060, 'latitude': 40.7128},  # New York
            color_scheme='light',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        
        page = context.new_page()
        
        # Hide webdriver property for stealth
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        logger.info("Firefox browser ready")

        found_count = 0

        # Helper function to handle cookie consent popup
        def handle_cookie_consent(page):
            """Try to accept cookie consent if it appears."""
            try:
                # Common cookie consent button selectors
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
                    '#onetrust-accept-btn-handler',  # OneTrust cookie consent
                    '.optanon-alert-box-button-middle',
                ]
                
                for selector in cookie_selectors:
                    try:
                        if page.locator(selector).is_visible(timeout=2000):
                            logger.info(f"Found cookie consent button: {selector}")
                            page.click(selector, timeout=3000)
                            logger.info("✓ Accepted cookie consent")
                            page.wait_for_timeout(1000)
                            return True
                    except Exception:
                        continue
                        
            except Exception as e:
                logger.debug(f"No cookie consent found or already accepted: {e}")
            
            return False

        # If journal slugs provided, crawl from each journal's newarticles page
        if journal_slugs:
            # First pass: count total open access articles to download
            if total_progress_callback:
                total_progress_callback(0, 0, "Scanning journals for open access articles...", 0, 0, "scanning")
            
            for slug in journal_slugs:
                if limit and found_count >= limit:
                    break
                
                # Create subfolder for this journal
                journal_folder = os.path.join(out_folder, slug.replace('/', '_'))
                os.makedirs(journal_folder, exist_ok=True)
                logger.info(f"Journal folder: {journal_folder}")
                
                # Go directly to newarticles page
                url = f"https://www.cell.com/{slug}/newarticles"
                logger.info(f"Crawling journal: {slug} at {url}")
                
                if total_progress_callback:
                    total_progress_callback(found_count, total_articles_found, f"Loading journal: {slug}", 0, 0, "loading")
                
                page.goto(url, timeout=30000)
                page.wait_for_timeout(3000)  # Wait for page to load
                
                # Handle cookie consent popup if it appears
                handle_cookie_consent(page)
                
                # Check if we got a Cloudflare challenge page
                page_title = page.title()
                if "Just a moment" in page_title or "Cloudflare" in page_title:
                    raise Exception(f"Cloudflare challenge detected on {url}. The website is blocking automated requests. Please try again later or use a VPN.")
                
                # Parse articles
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                articles = soup.select(".articleCitation")
                
                if not articles:
                    logger.warning(f"No articles found on {url}. Page title: {page_title}")
                    # Try to find any content at all
                    all_divs = soup.find_all("div")
                    logger.info(f"Found {len(all_divs)} div elements on page")
                    continue
                
                # Count open access articles in this journal
                oa_count = sum(1 for art in articles if art.find(class_="OALabel"))
                total_articles_found += min(oa_count, limit - found_count) if limit else oa_count
                logger.info(f"Found {oa_count} open access articles in {slug}")
                
                if total_progress_callback:
                    total_progress_callback(found_count, total_articles_found, f"Found {total_articles_found} open access articles", 0, 0, "found")
                
                for art in articles:
                    if limit and found_count >= limit:
                        break
                    
                    # Extract year from the date
                    year_tag = art.find(class_="toc__item__date")
                    year_text = year_tag.get_text() if year_tag else ""
                    try:
                        # Look for year in date text like "First published: October 02, 2025"
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
                    
                    # Find PDF link using the correct class
                    pdf_link = None
                    pdf_a = art.find("a", class_="pdfLink")
                    if pdf_a:
                        pdf_link = pdf_a.get("href", "")
                    
                    if not pdf_link:
                        continue
                    
                    # Check if it's open access using the OALabel class
                    oa_label = art.find(class_="OALabel")
                    if not oa_label:
                        logger.debug(f"Skipping non-open-access article: {pdf_link}")
                        continue
                    
                    # Extract article title using the correct class
                    title_elem = art.find(class_="toc__item__title")
                    article_title = title_elem.get_text(strip=True) if title_elem else f"Article {found_count + 1}"
                    
                    logger.info(f"Found open-access article: {article_title}")
                    
                    # Download PDF by clicking the link (Firefox will auto-download)
                    try:
                        # Generate safe filename from article title
                        safe_title = "".join(c for c in article_title if c.isalnum() or c in (' ', '-', '_')).strip()
                        safe_title = safe_title[:100]  # Limit length
                        filename = f"{safe_title}.pdf"
                        dest_path = os.path.join(journal_folder, filename)
                        
                        # Update progress: starting download
                        if total_progress_callback:
                            total_progress_callback(found_count, total_articles_found, f"Downloading: {article_title[:50]}...", 0, 0, "starting")
                        
                        logger.info(f"Clicking PDF link for: {article_title}")
                        
                        # Track download start time
                        download_start_time = time.time()
                        
                        # Wait for download to start when clicking the link
                        with page.expect_download(timeout=30000) as download_info:
                            # Find and click the PDF link using the href attribute
                            pdf_selector = f'a.pdfLink[href="{pdf_link}"]'
                            page.click(pdf_selector, timeout=10000)
                        
                        download = download_info.value
                        
                        # Update: download in progress
                        if total_progress_callback:
                            total_progress_callback(found_count, total_articles_found, f"Saving: {article_title[:50]}...", 0, 0, "downloading")
                        
                        # Save the download to the specified location with custom filename
                        download.save_as(dest_path)
                        
                        # Calculate download time and speed
                        download_time = time.time() - download_start_time
                        
                        # Verify the PDF was saved
                        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000:
                            file_size = os.path.getsize(dest_path)
                            file_size_kb = file_size / 1024
                            file_size_mb = file_size / (1024 * 1024)
                            
                            # Calculate download speed
                            if download_time > 0:
                                speed_kbps = file_size_kb / download_time
                                speed_mbps = speed_kbps / 1024
                            else:
                                speed_kbps = 0
                                speed_mbps = 0
                            
                            logger.info(f"✓ Successfully downloaded PDF: {filename} ({file_size_kb:.1f} KB) in {download_time:.1f}s @ {speed_kbps:.1f} KB/s")
                            
                            downloaded_files.append(dest_path)
                            open_access_articles.append(article_title)
                            found_count += 1
                            
                            # Update progress callbacks
                            if progress_callback:
                                progress_callback(filename, dest_path)
                            
                            if total_progress_callback:
                                total_progress_callback(found_count, total_articles_found, f"Downloaded: {filename[:40]}...", file_size, speed_kbps, "completed")
                        else:
                            logger.error(f"Downloaded file is too small or doesn't exist: {dest_path}")
                            
                    except Exception as e:
                        logger.error(f"Failed to download PDF for '{article_title}': {e}")
                        continue
                    
                    time.sleep(1)
        
        else:
            # Original keyword search behavior
            search_url = "https://www.cell.com/action/doSearch"
            params = {
                "AllField": keywords,
                "startPage": "0",
                "pageSize": "100",
            }
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{search_url}?{query}"
            logger.info(f"Searching: {url}")
            page.goto(url)
            page.wait_for_timeout(2000)
            
            # Handle cookie consent popup if it appears
            handle_cookie_consent(page)

            # Parse results
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            articles = soup.select("article.literatumArticle, div.article")

            for art in articles:
                if limit and found_count >= limit:
                    break

                # Extract year
                year_tag = art.find(class_=lambda x: x and "year" in x.lower())
                year_text = year_tag.get_text() if year_tag else ""
                try:
                    year = int("".join(c for c in year_text if c.isdigit())[:4])
                except Exception:
                    continue

                if not (year_from <= year <= year_to):
                    continue

                # Find PDF link
                pdf_link = None
                for a in art.find_all("a", href=True):
                    if "pdf" in a.get("href", "").lower() or "pdf" in a.get_text().lower():
                        pdf_link = a["href"]
                        break

                if not pdf_link:
                    continue

                # Extract article title
                title_elem = art.find("h3") or art.find("h2") or art.find("h1") or art.find(class_=lambda x: x and "title" in x.lower())
                article_title = title_elem.get_text(strip=True) if title_elem else f"Article {found_count + 1}"

                logger.info(f"Found article: {article_title}")

                # Download PDF by clicking the link (Firefox will auto-download)
                try:
                    # Generate safe filename from article title
                    safe_title = "".join(c for c in article_title if c.isalnum() or c in (' ', '-', '_')).strip()
                    safe_title = safe_title[:100]  # Limit length
                    filename = f"{safe_title}.pdf"
                    dest_path = os.path.join(out_folder, filename)
                    
                    logger.info(f"Clicking PDF link for: {article_title}")
                    
                    # Wait for download to start when clicking the link
                    with page.expect_download(timeout=30000) as download_info:
                        # Find and click the PDF link
                        pdf_selector = f'a[href="{pdf_link}"]'
                        page.click(pdf_selector, timeout=10000)
                    
                    download = download_info.value
                    
                    # Save the download to the specified location with custom filename
                    download.save_as(dest_path)
                    
                    # Verify the PDF was saved
                    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000:
                        file_size = os.path.getsize(dest_path)
                        logger.info(f"✓ Successfully downloaded PDF: {filename} ({file_size/1024:.1f} KB)")
                        
                        if progress_callback:
                            progress_callback(filename, dest_path)
                        
                        downloaded_files.append(dest_path)
                        open_access_articles.append(article_title)
                        found_count += 1
                    else:
                        logger.error(f"Downloaded file is too small or doesn't exist: {dest_path}")
                        
                except Exception as e:
                    logger.error(f"Failed to download PDF for '{article_title}': {e}")
                    continue
                
                time.sleep(1)

        context.close()
        browser.close()

    logger.info(f"Downloaded {found_count} PDFs to {out_folder}")
    return downloaded_files, open_access_articles


## --- Journal and keyword discovery helpers ---


def _cache_dir() -> str:
    root = os.getcwd()
    cd = os.path.join(root, ".cache", "papers_crawler")
    os.makedirs(cd, exist_ok=True)
    return cd


def discover_journals(force_refresh: bool = False) -> List[Tuple[str, str]]:
    """Discover journals from Cell.com's navbar by parsing the Journals menu.

    Returns a list of (slug, display_name). Caches results in .cache/papers_crawler/journals.json
    """
    import json
    import re
    import requests

    cache_file = os.path.join(_cache_dir(), "journals.json")
    if not force_refresh and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf8") as f:
                cached = json.load(f)
                if cached:  # Only return if cache is not empty
                    logger.info(f"Loaded {len(cached)} journals from cache")
                    return cached
        except Exception:
            pass

    results: List[Tuple[str, str]] = []
    
    # Try with Playwright (more reliable for dynamic content)
    logger.info("Fetching journals from Cell.com with Playwright...")
    try:
        with sync_playwright() as p:
            browser = p.firefox.launch(
                headless=True,
                firefox_user_prefs={
                    "pdfjs.disabled": True,
                    "browser.helperApps.neverAsk.saveToDisk": "application/pdf",
                }
            )
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0',
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York'
            )
            page = context.new_page()
            
            # Hide webdriver property
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            logger.info("Loading Cell.com homepage...")
            page.goto("https://www.cell.com", timeout=60000, wait_until="domcontentloaded")
            
            # Wait for navigation menu to be present
            try:
                page.wait_for_selector("ul.mega-menu, nav, header", timeout=10000)
                logger.info("Navigation menu loaded")
            except Exception as e:
                logger.warning(f"Could not find navigation menu: {e}")
            
            # Give extra time for any dynamic content
            page.wait_for_timeout(3000)
            
            html = page.content()
            logger.info(f"Retrieved page content: {len(html)} bytes")
            
            context.close()
            browser.close()
            
            # Parse the HTML
            soup = BeautifulSoup(html, "html.parser")
            
            # Find the "Journals" menu section specifically (first mega-menu item)
            # Look for the menu panel that contains "Life & medical sciences", "Physical sciences & engineering", "Multidisciplinary"
            journals_panel = soup.find('div', id='main-menu-panel-1')
            
            if not journals_panel:
                logger.warning("Could not find Journals menu panel (main-menu-panel-1)")
                # Try alternative: find all links but filter more strictly
                journals_panel = soup
            
            # Find all links within the Journals section
            all_links = journals_panel.find_all("a", href=True)
            logger.info(f"Found {len(all_links)} total links in Journals section")
            
            seen = set()
            for a in all_links:
                href = a.get("href", "")
                text = a.get_text(strip=True)
                
                # Skip empty text or non-journal links
                if not text or len(text) < 2:
                    continue
                
                # Only process links within the journal sub-menu
                if 'sub-menu__item-link' not in a.get('class', []):
                    continue
                
                # Match various journal URL patterns:
                # 1. /immunity/home -> "immunity"
                # 2. /cell-chemical-biology -> "cell-chemical-biology" (add /home for actual URL)
                # 3. /molecular-therapy-family/methods/home -> "molecular-therapy-family/methods"
                
                slug = None
                
                # Pattern 1: Single-level with /home (e.g., /immunity/home)
                match = re.match(r'^/([a-z0-9\-]+)/home$', href)
                if match:
                    slug = match.group(1)
                
                # Pattern 2: Multi-level with /home (e.g., /molecular-therapy-family/methods/home)
                elif re.match(r'^/([a-z0-9\-]+/[a-z0-9\-]+)/home$', href):
                    match = re.match(r'^/([a-z0-9\-]+/[a-z0-9\-]+)/home$', href)
                    if match:
                        slug = match.group(1)
                
                # Pattern 3: Journal links WITHOUT /home (e.g., /cell-chemical-biology)
                # These are still valid, the actual URL has /home added
                elif re.match(r'^/([a-z0-9\-]+)$', href):
                    slug = href.strip('/')
                
                if slug:
                    # Clean up text (remove "(partner)" suffixes, "partner", and extra whitespace)
                    clean_text = re.sub(r'\s*\([^)]*\)\s*$', '', text).strip()
                    clean_text = re.sub(r'\s+partner\s*$', '', clean_text, flags=re.IGNORECASE).strip()
                    # Remove HTML tags like <em>
                    clean_text = re.sub(r'<[^>]+>', '', clean_text).strip()
                    
                    if slug and clean_text and slug not in seen:
                        seen.add(slug)
                        results.append((slug, clean_text))
                        logger.debug(f"Found journal: {slug} -> {clean_text}")
            
            if results:
                logger.info(f"Successfully discovered {len(results)} journals from Cell.com")
                # Cache the results
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


def extract_journal_keywords(journal_slug: str, force_refresh: bool = False) -> List[Tuple[str, str]]:
    """Extract keyword tokens for a journal.

    Returns list of (token, label). token is the value suitable to use as a keyword in search (may be short forms).
    Caches results in .cache/papers_crawler/keywords_{slug}.json
    """
    import json

    safe = journal_slug.replace("/", "_")
    cache_file = os.path.join(_cache_dir(), f"keywords_{safe}.json")
    if not force_refresh and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf8") as f:
                return json.load(f)
        except Exception:
            pass

    # try journal newarticles page
    base = f"https://www.cell.com/{journal_slug}/newarticles"
    tokens: List[Tuple[str, str]] = []
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()
        page.goto(base)
        page.wait_for_timeout(1000)

        soup = BeautifulSoup(page.content(), "html.parser")

        # look for filters/subjects tags
        candidates = []
        for sel in (".filters a", ".subject a", ".tag a", ".keywords a", "a[href*='/search']"):
            candidates.extend(soup.select(sel))

        seen = set()
        for a in candidates:
            href = a.get("href") or ""
            text = (a.get_text() or "").strip()
            if not text:
                continue
            token = text
            # if href contains a query param, try to extract token
            if "/search" in href and "q=" in href:
                q = href.split("q=")[1].split("&")[0]
                token = q

            if token not in seen:
                seen.add(token)
                tokens.append((token, text))

        browser.close()

    # cache
    with open(cache_file, "w", encoding="utf8") as f:
        json.dump(tokens, f, ensure_ascii=False)

    return tokens
