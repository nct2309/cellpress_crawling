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
from collections import deque
from typing import Dict, List, Optional, Set, Tuple
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
        recent_lines = deque(maxlen=60)

        references_section = soup.find("section", id="references")
        footnote_map: Dict[str, str] = {}
        footnote_in_refs: Dict[str, bool] = {}
        footnote_elements: Set[Tag] = set()
        pending_bullet_prefix: Optional[str] = None

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
            "content-block"
        )
        unwanted_phrases = [
            "search for articles by this author",
            "crossref",
            "scopus",
            "google scholar",
            "show more",
            "show less",
            "supplementary material",
            "supplementary information",
            "metrics",
            "copyright",
            "licence",
            "license"
        ]

        reference_skip_phrases = (
            "full text",
            "full text (pdf)",
            "pdf",
            "crossref",
            "scopus",
            "pubmed",
            "google scholar",
            "open table in a new tab",
            "view abstract",
            "supplementary information",
        )

        def clean_reference_entry(tag: Tag) -> str:
            fragments: List[str] = []
            for string in tag.stripped_strings:
                fragment = clean_text(str(string))
                if not fragment:
                    continue
                lower_fragment = fragment.lower()
                if any(phrase in lower_fragment for phrase in reference_skip_phrases):
                    continue
                fragments.append(fragment)
            if not fragments:
                return ""
            combined = " ".join(fragments)
            combined = re.sub(r"\s+", " ", combined).strip()
            combined = re.sub(r"^\d+(\.|:)?\s*", "", combined)
            combined = combined.replace(" ,", ",")
            return combined

        def mark_footnote_elements(container: Tag) -> None:
            footnote_elements.add(container)
            for descendant in container.descendants:
                if isinstance(descendant, Tag):
                    footnote_elements.add(descendant)

        def reference_sort_key(identifier: str) -> Tuple[int, str]:
            match = re.search(r"(\d+)", identifier)
            if match:
                return int(match.group(1)), identifier
            return 10**6, identifier

        def build_footnote_map() -> None:
            selectors = [
                'a[id^="bib"]',
                'a[id^="ref"]',
                'a[name^="bib"]',
                'a[name^="ref"]',
                '[id^="bib"]',
                '[id^="ref"]',
                'li.reference',
                'li.bibliography__item',
            ]
            seen_ids: Set[str] = set()
            for selector in selectors:
                for candidate in soup.select(selector):
                    fid = candidate.get("id") or candidate.get("name")
                    if not fid:
                        anchor = candidate.find("a", id=True) or candidate.find("a", attrs={"name": True})
                        if anchor:
                            fid = anchor.get("id") or anchor.get("name")
                    if not fid:
                        continue
                    fid_lower = fid.lower()
                    if fid_lower in seen_ids:
                        continue
                    container = candidate
                    if container.name in {"a", "span", "sup"}:
                        parent_candidate = container.find_parent(['li', 'div', 'section', 'p'])
                        if parent_candidate:
                            container = parent_candidate
                    text = clean_reference_entry(container)
                    if not text:
                        continue
                    footnote_map[fid_lower] = text
                    in_refs = bool(references_section and references_section in container.parents)
                    footnote_in_refs[fid_lower] = in_refs
                    seen_ids.add(fid_lower)
                    if not in_refs:
                        mark_footnote_elements(container)

        def get_reference_entries() -> List[str]:
            entries: List[str] = []
            seen_text: Set[str] = set()
            if references_section:
                for candidate in references_section.select('li, div, p'):
                    text = clean_reference_entry(candidate)
                    lower_text = text.lower()
                    if not text or lower_text in seen_text:
                        continue
                    seen_text.add(lower_text)
                    entries.append(text)
                if entries:
                    return entries
            if footnote_map:
                ordered_ids = sorted(
                    (fid for fid, in_refs in footnote_in_refs.items() if in_refs),
                    key=reference_sort_key
                )
                for fid in ordered_ids:
                    text = footnote_map.get(fid)
                    if not text:
                        continue
                    lower_text = text.lower()
                    if lower_text in seen_text:
                        continue
                    seen_text.add(lower_text)
                    entries.append(text)
                if entries:
                    return entries
                for text in footnote_map.values():
                    lower_text = text.lower()
                    if lower_text in seen_text or not text:
                        continue
                    seen_text.add(lower_text)
                    entries.append(text)
            return entries

        def clean_text(value: str) -> str:
            if not value:
                return ""
            # Preserve inline superscripts but normalize whitespace
            text = re.sub(r"\s+", " ", value).strip()
            return text

        def should_skip_text(text: str) -> bool:
            if not text:
                return True
            stripped = text.strip()
            lower = stripped.lower()
            if lower.startswith("/* lines") and lower.endswith(" omitted */"):
                return True
            if any(phrase in lower for phrase in unwanted_phrases):
                return True
            if stripped in {"•", "·"}:
                return False
            if len(lower) <= 2 and not any(ch.isalpha() for ch in lower):
                return True
            if lower in {"…", "...", "∙"}:
                return True
            return False

        def ensure_paragraph_break() -> None:
            if not text_parts:
                return
            last = text_parts[-1]
            if last == "\n" or last.endswith("\n\n"):
                return
            text_parts.append("\n")

        def append_line(text: str, indent: int = 0, allow_repeat: bool = False) -> None:
            nonlocal pending_bullet_prefix
            cleaned = clean_text(text)
            if not cleaned:
                return
            stripped = cleaned.strip()
            if stripped in {"•", "·"}:
                pending_bullet_prefix = f"{' ' * indent}• "
                return
            if stripped in {"+", "-", "−"} and text_parts:
                updated = text_parts[-1].rstrip("\n") + f" {stripped}\n"
                text_parts[-1] = updated
                recent_lines.append(clean_text(updated.strip()))
                return
            if should_skip_text(cleaned):
                return
            if indent and not pending_bullet_prefix:
                cleaned = f"{' ' * indent}{cleaned}"
            if pending_bullet_prefix:
                cleaned = pending_bullet_prefix + cleaned
                pending_bullet_prefix = None
            dedup_key = clean_text(cleaned)
            if not allow_repeat and dedup_key in recent_lines:
                return
            recent_lines.append(dedup_key)
            text_parts.append(f"{cleaned}\n")

        def append_heading(level: int, text: str) -> None:
            heading_text = clean_text(text)
            if should_skip_text(heading_text):
                return
            level = max(1, min(level, 6))
            ensure_paragraph_break()
            text_parts.append(f"{'#' * level} {heading_text}\n")
            text_parts.append("\n")

        def append_list(list_tag: Tag, indent: int) -> None:
            items = [child for child in list_tag.find_all("li", recursive=False)]
            if not items:
                return

            for idx, item in enumerate(items, 1):
                bullet = f"{idx}. " if list_tag.name == "ol" else "- "
                
                # Collect all text including superscripts inline
                full_text = clean_text(item.get_text(" ", strip=True))
                
                # Remove nested list text temporarily
                nested_lists = item.find_all(["ul", "ol"], recursive=False)
                for nested in nested_lists:
                    nested_text = nested.get_text(" ", strip=True)
                    full_text = full_text.replace(nested_text, "")
                
                full_text = clean_text(full_text)
                
                if full_text:
                    append_line(f"{bullet}{full_text}", indent=indent, allow_repeat=True)
                else:
                    append_line(bullet.strip(), indent=indent, allow_repeat=True)

                # Process nested lists
                for nested in nested_lists:
                    append_list(nested, min(indent + indent_step, max_indent))

            ensure_paragraph_break()

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

            append_line("[Table]", indent=indent, allow_repeat=True)
            for row in rows:
                line = " | ".join(cell for cell in row if cell)
                if line:
                    append_line(line, indent=indent, allow_repeat=True)
            ensure_paragraph_break()

        def append_content(node, indent: int = 0) -> None:
            if isinstance(node, NavigableString):
                text = clean_text(str(node))
                append_line(text, indent=indent)
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
            if any("sidebar" in cls for cls in classes):
                return
            
            # Skip standalone footnote blocks (we handle them inline)
            if name in {"aside", "div", "section"} and any("footnote" in cls for cls in classes):
                return

            # Skip inline elements like sup, sub, span - they're handled by parent
            if name in {"sup", "sub", "span", "a", "strong", "em", "i", "b"}:
                return

            if name == "br":
                # Don't break on <br> inside inline elements - just treat as space
                return

            if name in heading_tags:
                heading_text = clean_text(node.get_text(" ", strip=True))
                if heading_text:
                    append_heading(min(int(name[1]), 6), heading_text)
                return

            if name in {"p", "blockquote"}:
                # Extract paragraph text with inline superscripts preserved
                paragraph = clean_text(node.get_text(" ", strip=True))
                
                # Collect footnote references if present (but not their full content)
                footnote_refs = []
                for sup in node.find_all("sup"):
                    # Only get the reference marker, not expanded footnote
                    sup_text = clean_text(sup.get_text(" ", strip=True))
                    if sup_text and len(sup_text) <= 3 and sup_text not in footnote_refs:
                        footnote_refs.append(sup_text)
                
                append_line(paragraph, indent=indent)
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
                ensure_paragraph_break()
        build_footnote_map()

        # Find the main article element
        article = soup.find("article")
        
        if article:
            # Extract from data-core-wrapper="header" section
            header_wrapper = article.find("div", {"data-core-wrapper": "header"})
            if header_wrapper:
                text_parts.append("=" * 80 + "\n")
                text_parts.append("ARTICLE HEADER\n")
                text_parts.append("=" * 80 + "\n\n")

                title = ""
                meta_title = soup.find("meta", {"name": "citation_title"}) or soup.find("meta", {"property": "og:title"})
                if meta_title and meta_title.get("content"):
                    title = clean_text(meta_title.get("content"))
                if not title:
                    title_tag = header_wrapper.find("h1")
                    if title_tag:
                        title = clean_text(title_tag.get_text(" ", strip=True))
                if title:
                    append_heading(1, title)

                author_meta = [clean_text(tag.get("content", "")) for tag in soup.find_all("meta", {"name": "citation_author"})]
                authors: List[str] = []
                for author in author_meta:
                    if author and author not in authors:
                        authors.append(author)
                if not authors:
                    for tag in header_wrapper.select('a[rel="author"], span[data-test="author-name"], span.author-name, span[itemprop="name"], a[itemprop="name"]'):
                        name_text = clean_text(tag.get_text(" ", strip=True).replace("Search for articles by this author", ""))
                        if should_skip_text(name_text):
                            continue
                        if name_text and name_text not in authors:
                            authors.append(name_text)
                if authors:
                    append_line("Authors: " + ", ".join(authors), allow_repeat=True)

                journal_meta = soup.find("meta", {"name": "citation_journal_title"})
                if journal_meta and journal_meta.get("content"):
                    append_line(f"Journal: {clean_text(journal_meta.get('content'))}", allow_repeat=True)

                date_meta = soup.find("meta", {"name": "citation_publication_date"}) or soup.find("meta", {"name": "dc.Date"})
                if date_meta and date_meta.get("content"):
                    append_line(f"Publication Date: {clean_text(date_meta.get('content'))}", allow_repeat=True)

                doi_meta = soup.find("meta", {"name": "citation_doi"})
                if doi_meta and doi_meta.get("content"):
                    append_line(f"DOI: {clean_text(doi_meta.get('content'))}", allow_repeat=True)

                keywords = []
                for keyword_meta in soup.find_all("meta", {"name": "citation_keywords"}):
                    keyword = clean_text(keyword_meta.get("content", ""))
                    if keyword and keyword not in keywords:
                        keywords.append(keyword)
                if keywords:
                    append_line("Keywords: " + ", ".join(keywords), allow_repeat=True)

                ensure_paragraph_break()
            
            # Extract from data-core-wrapper="content" section (main article body)
            content_wrapper = article.find("div", {"data-core-wrapper": "content"})
            if content_wrapper:
                text_parts.append("\n" + "=" * 80 + "\n")
                text_parts.append("ARTICLE CONTENT\n")
                text_parts.append("=" * 80 + "\n\n")
                
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
        
        # Extract references - only if not already processed in content_wrapper
        refs_elem = soup.find("section", id="references")
        if refs_elem and "REFERENCES" not in "\n".join(text_parts):
            text_parts.append("\n" + "=" * 80 + "\n")
            text_parts.append("REFERENCES\n")
            text_parts.append("=" * 80 + "\n\n")
            
            # Find reference list items
            ref_list = refs_elem.find("ol") or refs_elem.find("ul")
            if ref_list:
                ref_items = ref_list.find_all("li", recursive=False)
                seen_refs = set()
                for idx, ref in enumerate(ref_items, 1):
                    ref_text = clean_text(ref.get_text(" ", strip=True))
                    if ref_text and ref_text not in seen_refs:
                        seen_refs.add(ref_text)
                        text_parts.append(f"{idx}. {ref_text}\n")
            else:
                # Fallback to divs/paragraphs
                ref_items = refs_elem.find_all(['div', 'p'], class_=lambda x: x and 'reference' in str(x).lower())
                seen_refs = set()
                for idx, ref in enumerate(ref_items, 1):
                    ref_text = clean_text(ref.get_text(" ", strip=True))
                    if ref_text and ref_text not in seen_refs and len(ref_text) > 20:
                        seen_refs.add(ref_text)
                        text_parts.append(f"{idx}. {ref_text}\n")
        
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
    
    async def crawl_issue_page(page, issue_url: str, journal_folder: str, journal_download_count: int, is_open_archive: bool = False, issue_date: str = "Unknown"):
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
            if limit and journal_download_count >= limit:
                logger.info(f"✋ Reached journal limit of {limit} extractions")
                return journal_download_count, True
            
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
                continue
            
            await asyncio.sleep(1)
        
        return journal_download_count, False

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
                        if limit and journal_download_count >= limit:
                            print(f"✋ Reached journal limit of {limit}, stopping archive crawl", flush=True)
                            break
                        
                        journal_download_count, should_stop = await crawl_issue_page(archive_page, issue_url, journal_folder, journal_download_count, is_open_archive, issue_date)
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
