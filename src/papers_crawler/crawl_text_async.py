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

from bs4 import BeautifulSoup, Tag, NavigableString
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
    
    Extracts all content from the article including:
    - Header section (title, authors, affiliations, dates)
    - Introduction and all article sections
    - All headings (h1, h2, h3, h4, h5, h6) and paragraphs
    - Figure captions
    - References
    
    Focuses on content within <article> > <div data-core-wrapper="header"> 
    and <div data-core-wrapper="content"> for comprehensive extraction.
    
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
        
        # Remove UI elements, buttons, and navigation that are not article content
        for unwanted in soup.find_all(['button', 'nav', 'script', 'style', 'iframe', 'aside']):
            unwanted.decompose()
        
        # Remove specific UI classes that contain "show more/less" and other UI elements
        ui_classes = ['show-more', 'show-less', 'expand', 'collapse', 'toggle', 'button', 
                      'nav', 'menu', 'footer', 'sidebar', 'advertisement',
                      'social-share', 'download-link', 'metrics', 'altmetric']
        for ui_class in ui_classes:
            for elem in soup.find_all(class_=lambda x: x and ui_class in x.lower()):
                elem.decompose()
        
        text_parts = []
        
        # Find the main article element
        article = soup.find("article")
        
        if article:
            # Extract from data-core-wrapper="header" section
            header_wrapper = article.find("div", {"data-core-wrapper": "header"})
            if header_wrapper:
                text_parts.append("=" * 80 + "\n")
                text_parts.append("ARTICLE HEADER\n")
                text_parts.append("=" * 80 + "\n\n")
                
                # Extract all headings and content from header
                for elem in header_wrapper.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'div']):
                    # Skip nested elements if parent was already processed
                    if elem.find_parent(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p']) and elem.name in ['span', 'a', 'strong', 'em']:
                        continue
                    
                    elem_text = elem.get_text(separator=" ", strip=True)
                    if elem_text:
                        # Add heading markers for h tags
                        if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                            level = elem.name[1]  # Get number from h1, h2, etc.
                            text_parts.append(f"\n{'#' * int(level)} {elem_text}\n\n")
                        else:
                            text_parts.append(f"{elem_text}\n")
            
            # Extract from data-core-wrapper="content" section (main article body)
            content_wrapper = article.find("div", {"data-core-wrapper": "content"})
            if content_wrapper:
                text_parts.append("\n" + "=" * 80 + "\n")
                text_parts.append("ARTICLE CONTENT\n")
                text_parts.append("=" * 80 + "\n\n")
                
                # Recursively walk content to preserve nested section structure
                heading_tags = ("h1", "h2", "h3", "h4", "h5", "h6")
                skip_names = {"script", "style", "svg", "noscript", "form", "hr", "iframe"}
                indent_step = 2
                max_indent = 12

                container_keywords = (
                    "core-container",
                    "section",
                    "subsection",
                    "article__section",
                    "article-section",
                    "body-section",
                    "core-paragraph",
                    "paragraph"
                )

                def clean_text(value: str) -> str:
                    if not value:
                        return ""
                    return re.sub(r"\s+", " ", value).strip()

                def append_list(list_tag: Tag, indent: int) -> None:
                    items = [child for child in list_tag.children if isinstance(child, Tag) and child.name == "li"]
                    if not items:
                        return

                    for idx, item in enumerate(items, 1):
                        bullet = f"{idx}. " if list_tag.name == "ol" else "- "
                        fragments = []
                        for child in item.contents:
                            if isinstance(child, NavigableString):
                                fragment = clean_text(str(child))
                                if fragment:
                                    fragments.append(fragment)
                            elif isinstance(child, Tag) and child.name not in ("ul", "ol"):
                                fragment = clean_text(child.get_text(" ", strip=True))
                                if fragment:
                                    fragments.append(fragment)

                        bullet_text = " ".join(fragments).strip()
                        if bullet_text:
                            text_parts.append(f"{' ' * indent}{bullet}{bullet_text}\n")
                        else:
                            text_parts.append(f"{' ' * indent}{bullet.strip()}\n")

                        for nested in item.children:
                            if isinstance(nested, Tag) and nested.name in ("ul", "ol"):
                                append_list(nested, min(indent + indent_step, max_indent))

                    text_parts.append("\n")

                def append_table(table_tag: Tag, indent: int) -> None:
                    rows = []
                    for tr in table_tag.find_all("tr"):
                        cells = []
                        for cell in tr.find_all(["th", "td"]):
                            cell_text = clean_text(cell.get_text(" ", strip=True))
                            cells.append(cell_text)
                        if any(cell for cell in cells):
                            rows.append(cells)

                    if not rows:
                        return

                    text_parts.append(f"{' ' * indent}[Table]\n")
                    for row in rows:
                        line = " | ".join(cell for cell in row if cell)
                        if line:
                            text_parts.append(f"{' ' * indent}{line}\n")
                    text_parts.append("\n")

                def append_content(node, indent: int = 0) -> None:
                    if isinstance(node, NavigableString):
                        text = clean_text(str(node))
                        if text:
                            text_parts.append(f"{' ' * indent}{text}\n")
                        return

                    if not isinstance(node, Tag):
                        return

                    name = node.name.lower()

                    if name in skip_names:
                        return

                    if node.get("aria-hidden") == "true":
                        return

                    node_id = (node.get("id") or "").lower()
                    if node_id == "references":
                        return

                    if name == "figure" or (node.get("data-component") or "").lower() == "figure":
                        return

                    classes = [cls.lower() for cls in node.get("class", [])]
                    if any("figure" in cls for cls in classes):
                        return

                    if name == "br":
                        text_parts.append("\n")
                        return

                    if name in heading_tags:
                        heading_text = clean_text(node.get_text(" ", strip=True))
                        if heading_text:
                            level = min(int(name[1]), 6)
                            text_parts.append(f"\n{'#' * level} {heading_text}\n\n")
                        return

                    if name in {"p", "blockquote"}:
                        paragraph = clean_text(node.get_text(" ", strip=True))
                        if paragraph:
                            text_parts.append(f"{' ' * indent}{paragraph}\n")
                        return

                    if name in {"ul", "ol"}:
                        append_list(node, indent)
                        return

                    if name == "table":
                        append_table(node, indent)
                        return

                    next_indent = indent
                    is_container = name == "section" or any(
                        keyword in cls for cls in classes for keyword in container_keywords
                    ) or node.has_attr("data-core-component")

                    if is_container:
                        next_indent = min(indent + indent_step, max_indent)

                    for child in node.children:
                        append_content(child, next_indent)

                    if is_container:
                        text_parts.append("\n")

                for child in content_wrapper.children:
                    append_content(child, 0)
        
        # Fallback: if article element or wrappers not found, use old extraction method
        if not text_parts:
            logger.warning("⚠️ Article wrappers not found, using fallback extraction")
            
            # Extract title
            title_elem = soup.find("h1", {"property": "name"})
            if not title_elem:
                title_elem = soup.find("h1")
            if title_elem:
                title = title_elem.get_text(strip=True)
                if title:
                    text_parts.append(f"# {title}\n\n")
            
            # Extract authors
            authors_elem = soup.find("div", class_="contributors")
            if authors_elem:
                authors = authors_elem.get_text(separator=", ", strip=True)
                if authors:
                    text_parts.append(f"AUTHORS: {authors}\n\n")
            
            # Extract abstract
            abstract_elem = soup.find("section", id="author-abstract")
            if abstract_elem:
                text_parts.append("## ABSTRACT\n\n")
                for elem in abstract_elem.find_all(['p', 'div']):
                    elem_text = elem.get_text(separator=" ", strip=True)
                    if elem_text:
                        text_parts.append(f"{elem_text}\n")
            
            # Extract introduction and main body
            intro_elem = soup.find("section", id="introduction")
            if intro_elem:
                text_parts.append("\n## INTRODUCTION\n\n")
                for elem in intro_elem.find_all(['h2', 'h3', 'h4', 'p']):
                    if elem.name in ['h2', 'h3', 'h4']:
                        level = int(elem.name[1])
                        elem_text = elem.get_text(strip=True)
                        if elem_text:
                            text_parts.append(f"\n{'#' * level} {elem_text}\n\n")
                    else:
                        elem_text = elem.get_text(separator=" ", strip=True)
                        if elem_text and not elem.find_parent('figure'):
                            text_parts.append(f"{elem_text}\n")
            
            # Extract all other body sections
            body_elem = soup.find("section", id="bodymatter")
            if body_elem:
                text_parts.append("\n## MAIN CONTENT\n\n")
                for elem in body_elem.find_all(['h2', 'h3', 'h4', 'h5', 'h6', 'p']):
                    if elem.name in ['h2', 'h3', 'h4', 'h5', 'h6']:
                        level = int(elem.name[1])
                        elem_text = elem.get_text(strip=True)
                        if elem_text:
                            text_parts.append(f"\n{'#' * level} {elem_text}\n\n")
                    else:
                        elem_text = elem.get_text(separator=" ", strip=True)
                        if elem_text and not elem.find_parent('figure'):
                            text_parts.append(f"{elem_text}\n")
        
        # Extract figure captions (from anywhere in the page)
        figures = soup.find_all("figure")
        if figures:
            text_parts.append("\n" + "=" * 80 + "\n")
            text_parts.append("FIGURES\n")
            text_parts.append("=" * 80 + "\n\n")
            for idx, fig in enumerate(figures, 1):
                caption = fig.find("figcaption")
                if caption:
                    # Get figure label and title
                    fig_label = caption.find("span", class_="label")
                    fig_title = caption.find("span", class_="figure__title__text")
                    
                    if fig_label or fig_title:
                        label_text = fig_label.get_text(strip=True) if fig_label else f"Figure {idx}"
                        title_text = fig_title.get_text(strip=True) if fig_title else ""
                        text_parts.append(f"\n### {label_text}")
                        if title_text:
                            text_parts.append(f": {title_text}")
                        text_parts.append("\n\n")
                    
                    # Extract all caption content, including accordion/hidden content
                    caption_content = caption.find("div", class_="figure__caption__text__content")
                    if not caption_content:
                        caption_content = caption.find("div", class_="accordion__content")
                    if not caption_content:
                        # Fallback: get all divs with role="paragraph" or id starting with "fspara"
                        caption_content = caption
                    
                    # Extract all paragraphs within the caption
                    caption_paras = caption_content.find_all(['div', 'p'], recursive=True)
                    if caption_paras:
                        for para in caption_paras:
                            # Skip if it's a nested button or control element
                            if para.name == 'button' or 'button' in para.get('class', []):
                                continue
                            # Skip if it's just the label or title we already extracted
                            if para.find_parent(['span']) and 'label' in str(para.find_parent(['span']).get('class', [])):
                                continue
                            
                            para_text = para.get_text(separator=" ", strip=True)
                            if para_text and len(para_text) > 10:  # Skip very short text fragments
                                # Remove button text like "Hide caption" or "Figure viewer"
                                if para_text not in ['Hide caption', 'Figure viewer', 'Show caption', 'Collapse', 'Expand']:
                                    text_parts.append(f"{para_text}\n\n")
                    else:
                        # Fallback: get all text from caption
                        caption_text = caption.get_text(separator=" ", strip=True)
                        if caption_text:
                            # Clean up button text
                            caption_text = caption_text.replace('Hide caption', '').replace('Figure viewer', '')
                            caption_text = caption_text.replace('Show caption', '').replace('Collapse', '').replace('Expand', '')
                            caption_text = ' '.join(caption_text.split())  # Normalize whitespace
                            if caption_text:
                                text_parts.append(f"{caption_text}\n\n")
        
        # Extract references
        refs_elem = soup.find("section", id="references")
        if refs_elem:
            text_parts.append("\n" + "=" * 80 + "\n")
            text_parts.append("REFERENCES\n")
            text_parts.append("=" * 80 + "\n\n")
            
            ref_items = refs_elem.find_all(['li', 'div', 'p'], class_=lambda x: x and 'reference' in str(x).lower())
            if ref_items:
                for idx, ref in enumerate(ref_items, 1):
                    ref_text = ref.get_text(separator=" ", strip=True)
                    if ref_text:
                        text_parts.append(f"{idx}. {ref_text}\n")
            else:
                for elem in refs_elem.find_all(['p', 'div']):
                    ref_text = elem.get_text(separator=" ", strip=True)
                    if ref_text:
                        text_parts.append(f"{ref_text}\n")
        
        full_text = "\n".join(text_parts)
        
        if full_text.strip():
            logger.info(f"✅ Successfully extracted {len(full_text)} characters of text")
            return full_text
        else:
            logger.warning("⚠️ No text content extracted from page")
            return None
            
    except Exception as e:
        logger.error(f"❌ Failed to extract full-text: {e}")
        logger.debug(traceback.format_exc())
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
