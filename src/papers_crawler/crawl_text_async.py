"""Full-text extraction module for Cell.com articles.

This module provides functions to extract plain text content from Cell.com
article HTML pages, including title, authors, abstract, main text, figures,
and references.
"""
from __future__ import annotations

import os
import sys
import time
import logging
import csv
import zipfile
import asyncio
import re
import json
import traceback
from typing import List, Optional, Tuple
from urllib.parse import urljoin
from datetime import datetime

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page

from playwright_stealth import Stealth

# Import CLIProgressTracker from crawler_async
try:
    from .crawler_async import CLIProgressTracker
except ImportError:
    # Fallback if relative import fails
    from crawler_async import CLIProgressTracker

logger = logging.getLogger(__name__)


async def extract_fulltext_as_text(page: Page, fulltext_url: str) -> Optional[str]:
    """Navigate to full-text HTML page and extract all text content.
    
    Extracts the following sections:
    - Title
    - Authors
    - Abstract
    - Main text body (all sections)
    - Figure captions
    - References
    
    Args:
        page: Playwright page object for navigation
        fulltext_url: URL of the full-text HTML page
        
    Returns:
        str: Concatenated plain text content with section headers, or None if extraction fails
    """
    try:
        logger.info(f"📖 Navigating to full-text page: {fulltext_url}")
        await page.goto(fulltext_url, timeout=30000)
        await page.wait_for_timeout(2000)
        
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        
        text_parts = []
        
        # Extract title
        title_elem = soup.find("h1", {"property": "name"})
        if title_elem:
            title = title_elem.get_text(strip=True)
            text_parts.append(f"TITLE: {title}\n")
        
        # Extract authors
        authors_elem = soup.find("div", class_="contributors")
        if authors_elem:
            authors = authors_elem.get_text(separator=", ", strip=True)
            text_parts.append(f"AUTHORS: {authors}\n")
        
        # Extract abstract
        abstract_elem = soup.find("section", id="author-abstract")
        if abstract_elem:
            abstract_text = abstract_elem.get_text(separator="\n", strip=True)
            text_parts.append(f"\nABSTRACT:\n{abstract_text}\n")
        
        # Extract main body text
        body_elem = soup.find("section", id="bodymatter")
        if body_elem:
            text_parts.append("\n--- MAIN TEXT ---\n")
            # Extract all section headings and paragraphs
            for elem in body_elem.find_all(["h2", "h3", "h4", "p", "div"], class_=lambda x: x != "figure"):
                elem_text = elem.get_text(strip=True)
                if elem_text:
                    if elem.name in ["h2", "h3", "h4"]:
                        text_parts.append(f"\n{elem_text}\n")
                    else:
                        text_parts.append(f"{elem_text}\n")
        
        # Extract figure captions
        figures = soup.find_all("figure", class_="graphic")
        if figures:
            text_parts.append("\n--- FIGURES ---\n")
            for idx, fig in enumerate(figures, 1):
                caption = fig.find("figcaption")
                if caption:
                    caption_text = caption.get_text(strip=True)
                    text_parts.append(f"\nFigure {idx}: {caption_text}\n")
        
        # Extract references
        refs_elem = soup.find("section", id="references")
        if refs_elem:
            text_parts.append("\n--- REFERENCES ---\n")
            refs_text = refs_elem.get_text(separator="\n", strip=True)
            text_parts.append(refs_text)
        
        full_text = "\n".join(text_parts)
        
        if full_text.strip():
            logger.info(f"✅ Successfully extracted {len(full_text)} characters of text")
            return full_text
        else:
            logger.warning("⚠️ No text content extracted from page")
            return None
            
    except Exception as e:
        logger.error(f"❌ Failed to extract full-text: {e}")
        return None


async def save_text_to_file(text_content: str, file_path: str) -> bool:
    """Save extracted text content to a .txt file.
    
    Args:
        text_content: The text content to save
        file_path: Absolute path where the file should be saved
        
    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(text_content)
        logger.info(f"💾 Saved text to: {file_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to save text file: {e}")
        return False


async def crawl_text_async(
    keywords: str = "",
    year_from: int = 2020,
    year_to: int = 2024,
    out_folder: str = "papers",
    headless: bool = True,
    limit: Optional[int] = None,
    journal_slugs: Optional[List[str]] = None,
    progress_callback=None,
    total_progress_callback=None,
    crawl_archives: bool = False,
) -> Tuple[List[str], List[str]]:
    """Async crawl Cell.com for articles and extract full-text HTML as plain text.
    
    This function works exactly like crawl_async but extracts text content from
    /fulltext/ pages instead of downloading PDFs.
    
    Args:
        keywords: Search keywords (currently unused, reserved for future)
        year_from: Start year for article filtering
        year_to: End year for article filtering
        out_folder: Output folder for text files
        headless: Run browser in headless mode
        limit: Maximum number of articles to extract per journal
        journal_slugs: List of journal slugs to crawl
        progress_callback: Called with (filename, filepath) after each file is saved
        total_progress_callback: Called with (current, total, status, file_size, speed, stage)
        crawl_archives: If True, also crawl /issue pages for archived articles
    
    Returns:
        Tuple[List[str], List[str]]: (saved_file_paths, open_access_article_names)
    """
    
    os.makedirs(out_folder, exist_ok=True)
    saved_files = []
    open_access_articles = []
    article_metadata = []  # Store (file_path, article_title, publish_date)
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
                        await page.click(selector, timeout=3000)
                        await page.wait_for_timeout(1000)
                        return True
                except Exception:
                    continue
                    
        except Exception as e:
            logger.debug(f"No cookie consent found or already accepted: {e}")
        
        return False

    found_count = 0
    
    async def crawl_issue_page(page, issue_url: str, journal_folder: str, is_open_archive: bool = False, issue_date: str = "Unknown"):
        """Crawl a specific issue page for articles and extract text."""
        nonlocal found_count, saved_files, open_access_articles, article_metadata
        
        print(f"📖 Loading issue: {issue_url}", flush=True)
        print(f"📅 Issue date (from list): {issue_date}", flush=True)
        await page.goto(issue_url, timeout=30000)
        await page.wait_for_timeout(2000)
        
        await handle_cookie_consent(page)
        
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        
        if issue_date == "Unknown":
            logger.warning(f"⚠️ No date provided for issue, attempting to extract from page...")
            date_selectors = [
                ("div", {"class": "issue-item__title"}),
                ("span", {"class": "volume-issue"}),
                ("h1", {"class": "issue-item__title"}),
                ("div", {"class": "issue-item__detail"}),
                ("div", {"class": "u-cloak-me"}),
            ]
            
            for tag, attrs in date_selectors:
                elem = soup.find(tag, attrs)
                if elem:
                    text = elem.get_text(strip=True)
                    if text and text != "Unknown":
                        issue_date = text
                        print(f"📅 Extracted date from page: {issue_date}", flush=True)
                        break
        
        articles = soup.select(".articleCitation")
        print(f"Found {len(articles)} articles in issue", flush=True)
        
        for art in articles:
            if limit and found_count >= limit:
                logger.info(f"✋ Reached global limit of {limit} extractions")
                return True
            
            oa_label = art.find(class_="OALabel")
            if not is_open_archive and not oa_label:
                continue
            
            # Find Full-Text HTML link
            fulltext_link = None
            for link in art.find_all("a", href=True):
                if "Full-Text HTML" in link.get_text() or "/fulltext/" in link.get("href", ""):
                    fulltext_link = link.get("href", "")
                    break
            
            if not fulltext_link:
                continue
            
            # Make absolute URL
            if not fulltext_link.startswith("http"):
                fulltext_link = f"https://www.cell.com{fulltext_link}"
            
            title_elem = art.find(class_="toc__item__title")
            article_title = title_elem.get_text(strip=True) if title_elem else f"Article {found_count + 1}"
            publish_date = issue_date
            
            print(f"📄 Found {'open-archive' if is_open_archive else 'open-access'} article: {article_title[:60]}...", flush=True)
            
            try:
                safe_title = "".join(c for c in article_title if c.isalnum() or c in (' ', '-', '_')).strip()
                safe_title = safe_title[:100]
                filename = f"{safe_title}.txt"
                dest_path = os.path.join(journal_folder, filename)
                
                if os.path.exists(dest_path) and os.path.getsize(dest_path) > 100:
                    logger.info(f"⏭️  Skipping already extracted: {filename}")
                    continue
                
                if total_progress_callback:
                    total_progress_callback(found_count, found_count + 1, f"Extracting: {article_title[:50]}...", 0, 0, "starting")
                elif cli_progress:
                    cli_progress.update(found_count, found_count + 1, f"📝 {article_title[:30]}...", 0, 0, "starting", force=True)
                else:
                    logger.info(f"📝 Start extracting text: {article_title[:50]}...")
                
                extract_start_time = time.time()
                
                print(f"🔗 Navigating to full-text: {fulltext_link[:80]}...", flush=True)
                
                # Extract text from full-text page
                text_content = await extract_fulltext_as_text(page, fulltext_link)
                
                if text_content and len(text_content) > 100:
                    # Save to file
                    success = await save_text_to_file(text_content, dest_path)
                    
                    extract_time = time.time() - extract_start_time
                    
                    if success and os.path.exists(dest_path):
                        file_size = os.path.getsize(dest_path)
                        file_size_kb = file_size / 1024
                        
                        if extract_time > 0:
                            speed_kbps = file_size_kb / extract_time
                        else:
                            speed_kbps = 0
                        
                        if cli_progress is None:
                            print(f"✅ Extracted {file_size_kb:.1f} KB in {extract_time:.1f}s ({speed_kbps:.1f} KB/s)", flush=True)
                        
                        saved_files.append(dest_path)
                        open_access_articles.append(article_title)
                        article_metadata.append((dest_path, article_title, publish_date))
                        found_count += 1
                        
                        if progress_callback:
                            progress_callback(filename, dest_path)
                        
                        if total_progress_callback:
                            total_progress_callback(found_count, found_count, f"Saved: {article_title[:50]}...", file_size, speed_kbps, "completed")
                        elif cli_progress:
                            cli_progress.update(found_count, found_count, f"✅ {article_title[:30]}...", file_size, speed_kbps, "completed")
                    else:
                        logger.error(f"❌ Failed to save text file: {dest_path}")
                else:
                    logger.error(f"❌ Extracted text is too small or empty")
                    
            except Exception as e:
                logger.error(f"❌ Failed to extract text for '{article_title[:50]}': {e}")
                logger.debug(traceback.format_exc())
                continue
            
            await asyncio.sleep(1)
        
        return False

    if journal_slugs:
        if total_progress_callback:
            total_progress_callback(0, 0, "Scanning journals for open access articles...", 0, 0, "scanning")
        elif cli_progress:
            print(f"🔍 Scanning {len(journal_slugs)} journal(s) for open access articles...", flush=True)
        
        async with async_playwright() as p:
            for slug in journal_slugs:
                print(f"\n🚀 Launching Firefox for journal: {slug}...", flush=True)
                
                browser = await p.firefox.launch(headless=headless)
                
                context = await browser.new_context(
                    accept_downloads=False,  # Not downloading files
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
                
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                articles = soup.select(".articleCitation")
                
                if not articles:
                    print(f"⚠️ No articles found on {url}. Page title: {page_title}")
                    await page.close()
                    await context.close()
                    await browser.close()
                    continue
                
                oa_count = sum(1 for art in articles if art.find(class_="OALabel"))
                journal_download_count = 0
                journal_target = min(oa_count, limit) if limit else oa_count
                total_articles_found += journal_target
                print(f"📚 Found {oa_count} open access articles in {slug} (will extract up to {journal_target})")
                
                if total_progress_callback:
                    total_progress_callback(found_count, total_articles_found, f"Found {total_articles_found} open access articles", 0, 0, "found")
                elif cli_progress:
                    if cli_progress.total == 0 and total_articles_found > 0:
                        cli_progress.start(total_articles_found)
                    else:
                        cli_progress.total = total_articles_found
                
                for art in articles:
                    if limit and journal_download_count >= limit:
                        print(f"✋ Reached limit of {limit} for journal {slug}", flush=True)
                        break
                    
                    year_tag = art.find(class_="toc__item__date")
                    year_text = year_tag.get_text() if year_tag else ""
                    try:
                        if "," in year_text:
                            year_str = year_text.split(",")[-1].strip()
                        else:
                            year_str = year_text.strip()
                        year = int(re.search(r'\d{4}', year_str).group()) if re.search(r'\d{4}', year_str) else 0
                    except Exception:
                        year = 0
                    
                    if not (year_from <= year <= year_to):
                        continue
                    
                    # Find Full-Text HTML link
                    fulltext_link = None
                    for link in art.find_all("a", href=True):
                        if "Full-Text HTML" in link.get_text() or "/fulltext/" in link.get("href", ""):
                            fulltext_link = link.get("href", "")
                            break
                    
                    if not fulltext_link:
                        continue
                    
                    oa_label = art.find(class_="OALabel")
                    if not oa_label:
                        continue
                    
                    # Make absolute URL
                    if not fulltext_link.startswith("http"):
                        fulltext_link = f"https://www.cell.com{fulltext_link}"
                    
                    title_elem = art.find(class_="toc__item__title")
                    article_title = title_elem.get_text(strip=True) if title_elem else f"Article {found_count + 1}"
                    publish_date = year_text.strip() if year_text else "Unknown"
                    
                    print(f"📄 Found open-access article: {article_title[:60]}...")
                    
                    try:
                        safe_title = "".join(c for c in article_title if c.isalnum() or c in (' ', '-', '_')).strip()
                        safe_title = safe_title[:100]
                        filename = f"{safe_title}.txt"
                        dest_path = os.path.join(journal_folder, filename)
                        
                        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 100:
                            logger.info(f"⏭️  Skipping already extracted: {filename}")
                            journal_download_count += 1
                            continue
                        
                        if total_progress_callback:
                            total_progress_callback(found_count, found_count + 1, f"Extracting: {article_title[:50]}...", 0, 0, "starting")
                        elif cli_progress:
                            cli_progress.update(found_count, found_count + 1, f"📝 {article_title[:30]}...", 0, 0, "starting", force=True)
                        else:
                            logger.info(f"📝 Start extracting text: {article_title[:50]}...")
                        
                        extract_start_time = time.time()
                        
                        print(f"🔗 Navigating to full-text: {fulltext_link[:80]}...", flush=True)
                        
                        text_content = await extract_fulltext_as_text(page, fulltext_link)
                        
                        if text_content and len(text_content) > 100:
                            success = await save_text_to_file(text_content, dest_path)
                            
                            extract_time = time.time() - extract_start_time
                            
                            if success and os.path.exists(dest_path):
                                file_size = os.path.getsize(dest_path)
                                file_size_kb = file_size / 1024
                                
                                if extract_time > 0:
                                    speed_kbps = file_size_kb / extract_time
                                else:
                                    speed_kbps = 0
                                
                                if cli_progress is None:
                                    print(f"✅ Extracted {file_size_kb:.1f} KB in {extract_time:.1f}s ({speed_kbps:.1f} KB/s)", flush=True)
                                
                                saved_files.append(dest_path)
                                open_access_articles.append(article_title)
                                article_metadata.append((dest_path, article_title, publish_date))
                                found_count += 1
                                journal_download_count += 1
                                
                                if progress_callback:
                                    progress_callback(filename, dest_path)
                                
                                if total_progress_callback:
                                    total_progress_callback(found_count, found_count, f"Saved: {article_title[:50]}...", file_size, speed_kbps, "completed")
                                elif cli_progress:
                                    cli_progress.update(found_count, found_count, f"✅ {article_title[:30]}...", file_size, speed_kbps, "completed")
                            else:
                                logger.error(f"❌ Failed to save text file: {dest_path}")
                        else:
                            logger.error(f"❌ Extracted text is too small or empty")
                            
                    except Exception as e:
                        logger.error(f"❌ Failed to extract text for '{article_title[:50]}': {e}")
                        logger.debug(traceback.format_exc())
                    
                    await asyncio.sleep(1)
                
                # Crawl issue archives if requested
                if crawl_archives:
                    print(f"\n📚 Crawling issue archives for journal: {slug}", flush=True)
                    print(f"🔧 Creating separate context for archive crawling...", flush=True)
                    
                    archive_context = await browser.new_context(
                        accept_downloads=False,
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
                    
                    archive_page = await archive_context.new_page()
                    await stealth.apply_stealth_async(archive_page)
                    
                    await archive_page.add_init_script("""
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        });
                    """)
                    
                    print(f"✅ Archive context ready", flush=True)
                    
                    issue_index_url = f"https://www.cell.com/{slug}/issues"
                    print(f"Loading issue archive index: {issue_index_url}", flush=True)
                    await archive_page.goto(issue_index_url, timeout=30000)
                    await archive_page.wait_for_timeout(3000)
                    
                    await handle_cookie_consent(archive_page)
                    
                    html = await archive_page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    
                    print(f"📂 Parsing issue links from page HTML...", flush=True)
                    issue_links = []
                    in_open_archive = False
                    
                    all_issue_links = soup.select('a[href*="/issue?pii="]')
                    print(f"🔍 Found {len(all_issue_links)} total issue links on page", flush=True)
                    
                    for link in all_issue_links:
                        parent = link.find_parent("li") or link.find_parent("div")
                        if parent and "Open Archive" in parent.get_text():
                            in_open_archive = True
                        
                        href = link.get("href", "")
                        if not href.startswith("http"):
                            href = f"https://www.cell.com{href}"
                        
                        date_text = "Unknown"
                        date_elem = link.find_parent().find(class_="issue-item__title") if link.find_parent() else None
                        if date_elem:
                            date_text = date_elem.get_text(strip=True)
                        
                        try:
                            if date_text != "Unknown":
                                year_match = re.search(r'\d{4}', date_text)
                                if year_match:
                                    year = int(year_match.group())
                                    if year_from <= year <= year_to:
                                        issue_links.append((href, in_open_archive, date_text))
                        except Exception:
                            pass
                    
                    print(f"📚 Found {len(issue_links)} issues to crawl for {slug} (filtered by year {year_from}-{year_to})", flush=True)
                    
                    for issue_url, is_open_archive, issue_date in issue_links:
                        if limit and found_count >= limit:
                            print(f"✋ Reached global limit of {limit}, stopping archive crawl", flush=True)
                            break
                        
                        should_stop = await crawl_issue_page(archive_page, issue_url, journal_folder, is_open_archive, issue_date)
                        if should_stop:
                            break
                        
                        await asyncio.sleep(2)
                    
                    print(f"🔒 Closing archive context for journal: {slug}", flush=True)
                    await archive_page.close()
                    await archive_context.close()
                
                print(f"🔒 Closing browser for journal: {slug}", flush=True)
                await page.close()
                await context.close()
                await browser.close()

    if cli_progress:
        cli_progress.close()
    
    print(f"\n🎉 Extracted {found_count} text files to {out_folder}")
    
    # Create CSV file with extraction summary
    if saved_files:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"extraction_summary_{timestamp}.csv"
        csv_path = os.path.join(out_folder, csv_filename)
        
        print(f"\n📄 Creating extraction summary CSV: {csv_filename}")
        
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Number', 'Journal', 'Article Name', 'Publish Date', 'File Path', 'File Size (KB)'])
                
                for idx, (file_path, article_name, publish_date) in enumerate(article_metadata, 1):
                    journal_name = os.path.basename(os.path.dirname(file_path))
                    file_size_kb = os.path.getsize(file_path) / 1024 if os.path.exists(file_path) else 0
                    writer.writerow([idx, journal_name, article_name, publish_date, file_path, f"{file_size_kb:.2f}"])
            
            logger.info(f"✅ CSV summary saved to: {csv_path}")
        except Exception as e:
            logger.error(f"❌ Failed to create CSV summary: {e}")
    
    # Zip all journal subfolders into one archive
    if saved_files:
        print(f"\n📦 Creating ZIP archive with all extracted text files...")
        
        zip_filename = f"all_journals_text_{timestamp}.zip"
        zip_path = os.path.join(out_folder, zip_filename)
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in saved_files:
                    arcname = os.path.relpath(file_path, out_folder)
                    zipf.write(file_path, arcname)
                
                if os.path.exists(csv_path):
                    zipf.write(csv_path, os.path.basename(csv_path))
            
            zip_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
            logger.info(f"✅ Created ZIP archive: {zip_filename} ({zip_size_mb:.1f} MB)")
            logger.info(f"📦 Archive contains {len(saved_files)} text files from {len(set(os.path.dirname(f) for f in saved_files))} journals")
        except Exception as e:
            logger.error(f"❌ Failed to create ZIP archive: {e}")
    
    return saved_files, open_access_articles
