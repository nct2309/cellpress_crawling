"""Async Playwright-based crawler for Cell.com PDFs.

This module provides async versions of the crawler functions for use in
environments with asyncio event loops (like Jupyter/Colab).
"""
from __future__ import annotations

import os
import sys
import time
import logging
import csv
import zipfile
from typing import List, Optional, Tuple
from urllib.parse import urljoin
from datetime import datetime

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from playwright_stealth import Stealth, ALL_EVASIONS_DISABLED_KWARGS

import sys
IN_COLAB = 'google.colab' in sys.modules

try:
    if IN_COLAB:
        from tqdm.notebook import tqdm
    else:
        from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class CLIProgressTracker:
    """CLI progress tracker with optional tqdm support."""
    
    def __init__(self, use_tqdm: bool = True, min_refresh_interval: float = 0.5):
        self.use_tqdm = use_tqdm and TQDM_AVAILABLE
        self.pbar = None
        self.total = 0
        self.current = 0
        self.min_refresh_interval = min_refresh_interval  # Minimum seconds between updates
        self.last_update_time = 0
        
    def start(self, total: int):
        """Initialize progress tracking."""
        self.total = total
        self.current = 0
        self.last_update_time = time.time()
        if self.use_tqdm and total > 0:
            self.pbar = tqdm(
                total=total,
                desc="Downloading PDFs",
                unit="file",
                bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
                file=sys.stdout,
                mininterval=0.5,  # Minimum 0.5 seconds between updates
                maxinterval=2.0,  # Maximum 2 seconds between updates
            )
        elif total > 0:
            print(f"\n📥 Starting download: 0/{total} files (0%)")
    
    def update(self, current: int, total: int, status: str = "", file_size: int = 0, speed_kbps: float = 0, stage: str = "", force: bool = False):
        """Update progress display with throttling to prevent too frequent updates."""
        current_time = time.time()
        time_since_last_update = current_time - self.last_update_time
        
        # Skip update if too soon (unless forced, final update, or stage change)
        if not force and time_since_last_update < self.min_refresh_interval and current < total:
            return
        
        self.current = current
        self.total = total
        self.last_update_time = current_time
        
        if self.use_tqdm and self.pbar:
            # Update progress bar
            if current > self.pbar.n:
                self.pbar.n = current
                self.pbar.refresh()  # Always refresh to show updates
                
            # Show status in postfix
            postfix = {}
            if speed_kbps > 0:
                if speed_kbps > 1024:
                    postfix['speed'] = f"{speed_kbps/1024:.1f} MB/s"
                else:
                    postfix['speed'] = f"{speed_kbps:.1f} KB/s"
            if status:
                postfix['status'] = status[:30]
            if postfix:
                self.pbar.set_postfix(postfix, refresh=False)
        else:
            # Simple text progress (throttled)
            if total > 0:
                percentage = (current / total) * 100
                status_text = f"\r📥 Progress: {current}/{total} files ({percentage:.1f}%)"
                
                if speed_kbps > 0:
                    if speed_kbps > 1024:
                        status_text += f" | {speed_kbps/1024:.1f} MB/s"
                    else:
                        status_text += f" | {speed_kbps:.1f} KB/s"
                
                if status:
                    status_text += f" | {status[:40]}"
                
                print(status_text, end='')
    
    def close(self):
        """Finalize progress display."""
        if self.use_tqdm and self.pbar:
            self.pbar.close()
        else:
            print()  # New line after progress


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
    
    # Initialize CLI progress tracker (only if no callbacks provided)
    cli_progress = None
    if not progress_callback and not total_progress_callback:
        cli_progress = CLIProgressTracker(use_tqdm=True)

    # Initialize stealth mode for playwright
    stealth = Stealth(
        navigator_languages_override=("en-US", "en"),
        init_scripts_only=True
    )

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

    found_count = 0

    if journal_slugs:
        if total_progress_callback:
            total_progress_callback(0, 0, "Scanning journals for open access articles...", 0, 0, "scanning")
        elif cli_progress:
            print(f"🔍 Scanning {len(journal_slugs)} journal(s) for open access articles...", flush=True)
        
        async with async_playwright() as p:
            for slug in journal_slugs:
                # Create a fresh browser and context for each journal to avoid state issues
                print(f"\n� Launching Firefox for journal: {slug}...", flush=True)
                
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
                
                print(f"✅ Firefox browser ready for {slug}", flush=True)
                
                page = await context.new_page()
                
                # Apply stealth mode to hide automation
                await stealth.apply_stealth_async(page)
                
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """)
                
                journal_folder = os.path.join(out_folder, slug.replace('/', '_'))
                os.makedirs(journal_folder, exist_ok=True)
                print(f"📂 Journal folder: {journal_folder}")
                
                url = f"https://www.cell.com/{slug}/newarticles"
                print(f"🔎 Crawling journal: {slug} at {url}")
                
                if total_progress_callback:
                    total_progress_callback(found_count, total_articles_found, f"Loading journal: {slug}", 0, 0, "loading")
                
                await page.goto(url, timeout=30000)
                await page.wait_for_timeout(3000)
                
                await handle_cookie_consent(page)
                
                page_title = await page.title()
                # if "Just a moment" in page_title or "Cloudflare" in page_title:
                #     raise Exception(f"Cloudflare challenge detected on {url}. The website is blocking automated requests. Please try again later or use a VPN.")
                
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                articles = soup.select(".articleCitation")
                
                if not articles:
                    print(f"⚠️ No articles found on {url}. Page title: {page_title}")
                    await page.close()
                    await context.close()
                    await browser.close()  # ← ADD THIS!
                    continue
                
                oa_count = sum(1 for art in articles if art.find(class_="OALabel"))
                # Calculate how many we can download from this journal (limit is per journal)
                journal_download_count = 0
                journal_target = min(oa_count, limit) if limit else oa_count
                total_articles_found += journal_target
                print(f"📚 Found {oa_count} open access articles in {slug} (will download up to {journal_target})")
                
                if total_progress_callback:
                    total_progress_callback(found_count, total_articles_found, f"Found {total_articles_found} open access articles", 0, 0, "found")
                elif cli_progress:
                    if cli_progress.total == 0 and total_articles_found > 0:
                        # Start CLI progress bar once we know the total
                        cli_progress.start(total_articles_found)
                    else:
                        # Update total if we found more articles
                        cli_progress.total = total_articles_found
                        if cli_progress.pbar:
                            cli_progress.pbar.total = total_articles_found
                
                for art in articles:
                    # Check if we've reached the limit for THIS journal
                    if limit and journal_download_count >= limit:
                        print(f"✋ Reached limit of {limit} downloads for journal {slug}")
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
                        logger.info(f"Skipping non-open-access article: {pdf_link}")
                        continue
                    
                    title_elem = art.find(class_="toc__item__title")
                    article_title = title_elem.get_text(strip=True) if title_elem else f"Article {found_count + 1}"
                    
                    print(f"📄 Found open-access article: {article_title[:60]}...")
                    
                    try:
                        safe_title = "".join(c for c in article_title if c.isalnum() or c in (' ', '-', '_')).strip()
                        safe_title = safe_title[:100]
                        filename = f"{safe_title}.pdf"
                        dest_path = os.path.join(journal_folder, filename)
                        
                        if total_progress_callback:
                            total_progress_callback(found_count, total_articles_found, f"Downloading: {article_title[:50]}...", 0, 0, "starting")
                        elif cli_progress:
                            # Update progress bar to show we're starting this download (force update)
                            cli_progress.update(found_count, total_articles_found, f"⬇️  {article_title[:30]}...", 0, 0, "starting", force=True)
                        else:
                            logger.info(f"⬇️  Start downloading file: {article_title[:50]}...")
                        
                        download_start_time = time.time()
                        
                        logger.info(f"🔗 Clicking PDF link: {pdf_link[:80]}...")
                        
                        async with page.expect_download(timeout=30000) as download_info:
                            pdf_selector = f'a.pdfLink[href="{pdf_link}"]'
                            await page.click(pdf_selector, timeout=10000)
                        
                        logger.info(f"⏳ Waiting for download to complete...")
                        
                        download = await download_info.value
                        
                        logger.info(f"💾 Saving file to: {dest_path}")
                        
                        if total_progress_callback:
                            total_progress_callback(found_count, total_articles_found, f"Saving: {article_title[:50]}...", 0, 0, "downloading")
                        elif cli_progress:
                            # Update progress bar to show we're saving (force update)
                            cli_progress.update(found_count, total_articles_found, f"💾 {article_title[:30]}...", 0, 0, "saving", force=True)
                        
                        await download.save_as(dest_path)
                        
                        download_time = time.time() - download_start_time
                        
                        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000:
                            file_size = os.path.getsize(dest_path)
                            file_size_kb = file_size / 1024
                            
                            if download_time > 0:
                                speed_kbps = file_size_kb / download_time
                            else:
                                speed_kbps = 0
                            
                            if cli_progress is None:
                                if speed_kbps > 1024:
                                    logger.info(f"✅ Downloaded file: {filename[:50]} ({file_size_kb:.1f} KB) @ {speed_kbps/1024:.1f} MB/s")
                                else:
                                    logger.info(f"✅ Downloaded file: {filename[:50]} ({file_size_kb:.1f} KB) @ {speed_kbps:.1f} KB/s")
                            
                            downloaded_files.append(dest_path)
                            open_access_articles.append(article_title)
                            found_count += 1
                            journal_download_count += 1  # Increment per-journal counter
                            
                            if progress_callback:
                                progress_callback(filename, dest_path)
                            
                            if total_progress_callback:
                                total_progress_callback(found_count, total_articles_found, f"Downloaded: {filename[:40]}...", file_size, speed_kbps, "completed")
                            elif cli_progress:
                                # Force update to show completion immediately
                                cli_progress.update(found_count, total_articles_found, f"✅ {filename[:25]}...", file_size, speed_kbps, "completed", force=True)
                        else:
                            logger.error(f"❌ Downloaded file is too small or doesn't exist: {dest_path}")
                            
                    except Exception as e:
                        logger.error(f"❌ Failed to download PDF for '{article_title[:50]}': {e}")
                        import traceback
                        logger.debug(traceback.format_exc())
                        continue
                    
                    await asyncio.sleep(1)
                
                # Close the page, context, and browser after finishing this journal
                print(f"🔒 Closing browser for journal: {slug}", flush=True)
                await page.close()
                await context.close()
                await browser.close()

    # Close CLI progress tracker
    if cli_progress:
        cli_progress.close()
    
    print(f"\n🎉 Downloaded {found_count} PDFs to {out_folder}")
    
    # Create CSV file with download summary
    if downloaded_files:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"download_summary_{timestamp}.csv"
        csv_path = os.path.join(out_folder, csv_filename)
        
        print(f"\n📄 Creating download summary CSV: {csv_filename}")
        
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Number', 'Journal', 'Article Name', 'File Path', 'File Size (KB)'])
                
                for idx, (file_path, article_name) in enumerate(zip(downloaded_files, open_access_articles), 1):
                    # Extract journal name from file path
                    journal_name = os.path.basename(os.path.dirname(file_path))
                    file_size_kb = os.path.getsize(file_path) / 1024 if os.path.exists(file_path) else 0
                    
                    writer.writerow([
                        idx,
                        journal_name,
                        article_name,
                        file_path,
                        f"{file_size_kb:.1f}"
                    ])
            
            logger.info(f"✅ CSV summary saved to: {csv_path}")
        except Exception as e:
            logger.error(f"❌ Failed to create CSV summary: {e}")
    
    # Zip all journal subfolders into one archive
    if downloaded_files:
        print(f"\n📦 Creating ZIP archive with all downloaded PDFs...")
        
        zip_filename = f"all_journals_{timestamp}.zip"
        zip_path = os.path.join(out_folder, zip_filename)
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add all PDF files maintaining journal folder structure
                for file_path in downloaded_files:
                    if os.path.exists(file_path):
                        # Get relative path from out_folder to maintain folder structure in ZIP
                        arcname = os.path.relpath(file_path, out_folder)
                        zipf.write(file_path, arcname)
                
                # Also add the CSV summary if it exists
                if os.path.exists(csv_path):
                    zipf.write(csv_path, os.path.basename(csv_path))
            
            zip_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
            logger.info(f"✅ Created ZIP archive: {zip_filename} ({zip_size_mb:.1f} MB)")
            logger.info(f"📦 Archive contains {len(downloaded_files)} PDFs from {len(set(os.path.dirname(f) for f in downloaded_files))} journals")
        except Exception as e:
            logger.error(f"❌ Failed to create ZIP archive: {e}")
    
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
    
    print("🌐 Fetching journals from Cell.com with Playwright...")
    
    # Initialize stealth mode
    stealth = Stealth(
        navigator_languages_override=("en-US", "en"),
        init_scripts_only=True
    )
    
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
            
            # Apply stealth mode to hide automation
            await stealth.apply_stealth_async(page)
            
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            print("🔗 Loading Cell.com homepage...")
            await page.goto("https://www.cell.com", timeout=60000, wait_until="domcontentloaded")
            
            try:
                await page.wait_for_selector("ul.mega-menu, nav, header", timeout=10000)
                print("✅ Navigation menu loaded")
            except Exception as e:
                print(f"⚠️ Could not find navigation menu: {e}")
            
            await page.wait_for_timeout(3000)
            
            html = await page.content()
            print(f"📄 Retrieved page content: {len(html)} bytes")
            
            await context.close()
            await browser.close()
            
            soup = BeautifulSoup(html, "html.parser")
            journals_panel = soup.find('div', id='main-menu-panel-1')
            
            if not journals_panel:
                print("⚠️ Could not find Journals menu panel (main-menu-panel-1)")
                journals_panel = soup
            
            all_links = journals_panel.find_all("a", href=True)
            print(f"🔗 Found {len(all_links)} total links in Journals section")
            
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
                print(f"✅ Successfully discovered {len(results)} journals from Cell.com")
                try:
                    with open(cache_file, "w", encoding="utf8") as f:
                        json.dump(results, f, ensure_ascii=False, indent=2)
                    print(f"💾 Cached {len(results)} journals to {cache_file}")
                except Exception as e:
                    print(f"⚠️ Failed to cache journals: {e}")
                return results
            else:
                raise Exception("No journals found on Cell.com - page structure may have changed")
                
    except Exception as e:
        print(f"❌ Failed to discover journals from Cell.com: {e}")
        raise Exception(f"Could not load journals from Cell.com. Error: {str(e)}. Please check your internet connection and try again.")
