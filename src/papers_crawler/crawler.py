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


def _download_with_route_intercept(page, url: str, dest_folder: str, progress_callback=None) -> Optional[str]:
    """Download PDF by intercepting the route after JavaScript loads it in embed element."""
    os.makedirs(dest_folder, exist_ok=True)
    
    try:
        logger.info(f"Opening PDF page: {url}")
        
        # Store intercepted PDF
        pdf_data = None
        pdf_captured = False
        
        def handle_route(route):
            """Intercept PDF requests and capture them."""
            nonlocal pdf_data, pdf_captured
            try:
                request_url = route.request.url
                
                # Only intercept PDF-related requests
                if 'pdf' in request_url.lower():
                    logger.info(f"Intercepting request: {request_url}")
                    
                    # Fetch using browser context
                    response = page.context.request.get(request_url)
                    
                    if response.ok:
                        content_type = response.headers.get('content-type', '')
                        
                        if 'application/pdf' in content_type:
                            body = response.body()
                            if body and len(body) > 1000 and body.startswith(b'%PDF'):
                                logger.info(f"✓ Captured PDF: {len(body)} bytes ({len(body)/1024/1024:.2f} MB)")
                                pdf_data = body
                                pdf_captured = True
                        
                        # Fulfill with Content-Disposition: attachment
                        headers = dict(response.headers)
                        headers['Content-Disposition'] = 'attachment'
                        
                        route.fulfill(
                            status=response.status,
                            headers=headers,
                            body=response.body()
                        )
                    else:
                        route.continue_()
                else:
                    route.continue_()
                    
            except Exception as e:
                logger.debug(f"Route handler error: {e}")
                route.continue_()
        
        # Set up route interception
        page.route("**/*", handle_route)
        
        # Navigate to PDF page
        logger.info("Loading PDF page...")
        page.goto(url, timeout=60000, wait_until="load")
        
        # Wait for network idle
        logger.info("Waiting for network to be idle...")
        page.wait_for_load_state("networkidle", timeout=60000)
        
        # Wait for the embed element to load the PDF
        logger.info("Waiting for PDF to load in embed...")
        page.wait_for_timeout(10000)  # Wait 10 seconds for JS to initialize
        
        # Poll until PDF is captured
        max_wait = 60  # Wait up to 60 seconds total
        for i in range(max_wait):
            if pdf_captured:
                logger.info(f"✓ PDF captured after {i+1} seconds")
                break
            logger.info(f"Waiting for PDF... ({i+1}s)")
            page.wait_for_timeout(1000)
        
        # Clean up
        page.unroute("**/*")
        
        if not pdf_data:
            logger.error("Failed to capture PDF from page")
            raise Exception("PDF was not loaded. The PDF may require authentication or manual download.")
        
        # Save PDF
        pdf_size = len(pdf_data)
        logger.info(f"Successfully captured PDF: {pdf_size} bytes ({pdf_size/1024/1024:.2f} MB)")
        
        fname = os.path.basename(url.split("?")[0]) or "download.pdf"
        if not fname.lower().endswith(".pdf"):
            fname += ".pdf"
        dest = os.path.join(dest_folder, fname)
        
        with open(dest, "wb") as f:
            f.write(pdf_data)
        
        logger.info(f"✓ Successfully saved: {fname} ({pdf_size/1024/1024:.2f} MB)")
        
        if progress_callback:
            progress_callback(fname, dest)
        
        return dest
        
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        raise


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


def _download_with_request(page, url: str, dest_folder: str, progress_callback=None) -> Optional[str]:
    """Download PDF by waiting for page to fully load, then intercepting the PDF resource."""
    os.makedirs(dest_folder, exist_ok=True)
    
    try:
        logger.info(f"Opening PDF viewer page: {url}")
        
        # Store intercepted PDF
        pdf_data = None
        
        # Navigate and wait for page to fully load first
        logger.info("Loading page...")
        page.goto(url, timeout=60000, wait_until="load")
        
        # Wait for network to be idle (all resources loaded)
        logger.info("Waiting for network to be idle...")
        page.wait_for_load_state("networkidle", timeout=30000)
        
        # Now try to fetch the PDF directly using browser context
        logger.info("Attempting to fetch PDF using browser context...")
        try:
            api_request = page.context.request
            response = api_request.get(url)
            
            if response.ok:
                content_type = response.headers.get('content-type', '')
                logger.info(f"Direct fetch - Status: {response.status}, Content-Type: {content_type}")
                
                if 'application/pdf' in content_type:
                    body = response.body()
                    if body and len(body) > 1000 and body.startswith(b'%PDF'):
                        logger.info(f"✓ Successfully fetched PDF: {len(body)} bytes ({len(body)/1024/1024:.2f} MB)")
                        pdf_data = body
                    else:
                        logger.warning(f"Invalid PDF from direct fetch: size={len(body)}, starts_with_pdf={body.startswith(b'%PDF') if body else False}")
                else:
                    logger.info(f"Direct fetch returned HTML, not PDF: {content_type}")
            else:
                logger.warning(f"Direct fetch failed: {response.status}")
        except Exception as e:
            logger.error(f"Direct fetch error: {e}")
        
        if not pdf_data:
            logger.warning("Could not capture PDF from network traffic, trying direct download...")
            
            # Try direct download as fallback
            try:
                import requests
                
                # Use the same session cookies from the browser
                cookies = {}
                for cookie in page.context.cookies():
                    cookies[cookie['name']] = cookie['value']
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0',
                    'Accept': 'application/pdf,*/*',
                    'Referer': 'https://www.cell.com/',
                }
                
                logger.info(f"Attempting direct download of: {url}")
                response = requests.get(url, cookies=cookies, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    body = response.content
                    if body and len(body) > 1000 and body.startswith(b'%PDF'):
                        logger.info(f"✓ Direct download successful!")
                        pdf_data = body
                    else:
                        logger.error(f"Direct download failed - not a valid PDF (size: {len(body)})")
                        logger.error(f"Content preview: {body[:200]}")
                else:
                    logger.error(f"Direct download failed - status: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"Direct download error: {e}")
        
        if not pdf_data:
            logger.error("All PDF capture methods failed")
            raise Exception("PDF was not loaded in the page. The PDF may require authentication or manual download.")
        
        pdf_size = len(pdf_data)
        logger.info(f"Successfully captured PDF: {pdf_size} bytes ({pdf_size/1024/1024:.2f} MB)")
        
        # Verify PDF signature
        if not pdf_data.startswith(b'%PDF'):
            logger.error(f"Invalid PDF signature. First 100 bytes: {pdf_data[:100]}")
            raise Exception("Captured file is not a valid PDF")
        
        fname = os.path.basename(url.split("?")[0]) or "download.pdf"
        if not fname.lower().endswith(".pdf"):
            fname += ".pdf"
        dest = os.path.join(dest_folder, fname)
        
        with open(dest, "wb") as f:
            f.write(pdf_data)
        
        logger.info(f"✓ Successfully saved: {fname} ({pdf_size/1024/1024:.2f} MB)")
        
        if progress_callback:
            progress_callback(fname, dest)
        
        return dest
        
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
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
) -> Tuple[List[str], List[str]]:
    """Crawl Cell.com for articles matching keywords, year range, and optionally specific journals.
    
    If journal_slugs is provided, crawls from each journal's /newarticles page.
    Otherwise, uses keyword search across all journals.
    
    Returns:
        Tuple[List[str], List[str]]: (downloaded_file_paths, open_access_article_names)
    """
    import time

    os.makedirs(out_folder, exist_ok=True)
    downloaded_files = []
    open_access_articles = []

    stealth = Stealth(
        navigator_languages_override=("en-US", "en"),
        init_scripts_only=True
    )

    with sync_playwright() as p:
        # Use Chromium for PDF generation support (Firefox doesn't support page.pdf())
        logger.info("Launching Chromium browser...")
        browser = p.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )
        
        # Create context with realistic browser fingerprint
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            }
        )
        
        page = context.new_page()
        
        # Hide webdriver property for stealth
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        logger.info("Chromium browser ready")

        found_count = 0

        # If journal slugs provided, crawl from each journal's newarticles page
        if journal_slugs:
            for slug in journal_slugs:
                if limit and found_count >= limit:
                    break
                
                # Go directly to newarticles page
                url = f"https://www.cell.com/{slug}/newarticles"
                logger.info(f"Crawling journal: {slug} at {url}")
                
                page.goto(url, timeout=30000)
                page.wait_for_timeout(3000)  # Wait longer for Cloudflare
                
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
                    
                    pdf_url = urljoin(url, pdf_link)
                    logger.info(f"Found open-access PDF: {pdf_url}")
                    
                    # Download PDF by waiting for JS to load it
                    downloaded_file = _download_with_route_intercept(page, pdf_url, out_folder, progress_callback)
                    downloaded_files.append(downloaded_file)
                    open_access_articles.append(article_title)
                    found_count += 1
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

                pdf_url = urljoin(url, pdf_link)
                logger.info(f"Found PDF: {pdf_url}")

                # Download
                downloaded_file = _download_with_request(page, pdf_url, out_folder, progress_callback)
                downloaded_files.append(downloaded_file)
                open_access_articles.append(article_title)
                found_count += 1
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
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox'
                ]
            )
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
            
            # Find all links with href pattern matching journal home pages
            all_links = soup.find_all("a", href=True)
            logger.info(f"Found {len(all_links)} total links on page")
            
            seen = set()
            for a in all_links:
                href = a.get("href", "")
                text = a.get_text(strip=True)
                
                # Extract slug from href like "/immunity/home" -> "immunity"
                # Also match patterns like "/cell/home", "/neuron/home", etc.
                match = re.match(r'^/([^/]+)/home$', href)
                if match:
                    slug = match.group(1)
                    # Clean up text (remove "(partner)" suffixes, "partner", and extra whitespace)
                    clean_text = re.sub(r'\s*\([^)]*\)\s*$', '', text).strip()
                    clean_text = re.sub(r'\s+partner\s*$', '', clean_text, flags=re.IGNORECASE).strip()
                    
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
        browser = p.chromium.launch(headless=True)
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
