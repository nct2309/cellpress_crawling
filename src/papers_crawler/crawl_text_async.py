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


async def extract_fulltext_as_json(page: Page, fulltext_url: str) -> Optional[Dict]:
    """Navigate to full-text HTML page and extract all text content as JSON.
    
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
        Dict: JSON structure with sections as keys and content as values, or None if extraction fails
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
        
        # JSON structure to store sections
        json_data = {}
        current_section = "header"  # Start with header
        current_section_parts = []
        section_stack = [(current_section, current_section_parts)]  # Stack for nested sections
        
        text_parts = []  # Keep for compatibility with existing functions
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

        def normalize_identifier(value: Optional[str]) -> str:
            if not value:
                return ""
            normalized = str(value).strip().lower()
            if normalized.startswith("#"):
                normalized = normalized[1:]
            normalized = re.sub(r"\s+", "", normalized)
            return normalized

        def split_identifier_values(raw_value) -> List[str]:
            if not raw_value:
                return []
            if isinstance(raw_value, (list, tuple, set)):
                collected: List[str] = []
                for item in raw_value:
                    collected.extend(split_identifier_values(item))
                return collected
            value = str(raw_value).strip()
            if not value:
                return []
            if value.startswith("#"):
                value = value[1:]
            if value.startswith("http") and "#" in value:
                value = value.split("#", 1)[1]
            parts = re.split(r"[\s,;]+", value)
            return [part for part in parts if part]

        def extract_candidate_ids(element: Optional[Tag]) -> List[str]:
            if not element:
                return []
            attributes = (
                "id",
                "name",
                "href",
                "data-rid",
                "data-ref",
                "data-reference",
                "data-footnote-id",
                "data-id",
                "data-target",
                "data-uuid",
                "data-bib",
                "data-bib-id",
                "data-citation-id",
                "data-annotation-id",
            )
            identifiers: List[str] = []
            for attr in attributes:
                raw = element.get(attr)
                if attr == "href" and raw and "#" in str(raw):
                    raw = str(raw).split("#", 1)[1]
                elif attr == "href":
                    continue
                values = split_identifier_values(raw)
                identifiers.extend(values)
            return identifiers

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
                        candidates = extract_candidate_ids(candidate)
                        if not candidates:
                            continue
                        ids_to_process = candidates
                    else:
                        ids_to_process = [fid]
                    normalized_ids = [normalize_identifier(value) for value in ids_to_process if value]
                    normalized_ids = [value for value in normalized_ids if value]
                    if not normalized_ids:
                        continue
                    container = candidate
                    if container.name in {"a", "span", "sup"}:
                        parent_candidate = container.find_parent(['li', 'div', 'section', 'p'])
                        if parent_candidate:
                            container = parent_candidate
                    text = clean_reference_entry(container)
                    if not text:
                        continue
                    in_refs = bool(references_section and references_section in container.parents)
                    for normalized_id in normalized_ids:
                        if normalized_id in seen_ids:
                            continue
                        footnote_map[normalized_id] = text
                        footnote_in_refs[normalized_id] = in_refs
                        seen_ids.add(normalized_id)
                    if not in_refs:
                        mark_footnote_elements(container)

        def get_reference_entries() -> List[str]:
            entries: List[str] = []
            seen_text: Set[str] = set()
            if references_section:
                # Try to find list items first (most common structure)
                candidates = references_section.find_all('li', recursive=False)
                
                # Also look for div[role="listitem"] (Cell Press format)
                if not candidates:
                    candidates = references_section.find_all('div', {'role': 'listitem'})
                
                if not candidates:
                    # Fallback: find direct children that are divs or paragraphs
                    candidates = [child for child in references_section.children 
                                 if isinstance(child, Tag) and child.name in {'div', 'p'}]
                
                for candidate in candidates:
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

        def collect_inline_footnotes(container: Tag) -> List[str]:
            if not footnote_map:
                return []
            collected: List[str] = []
            seen_ids: Set[str] = set()
            for sup in container.find_all("sup"):
                candidate_ids = extract_candidate_ids(sup)
                if not candidate_ids:
                    for anchor in sup.find_all("a"):
                        candidate_ids.extend(extract_candidate_ids(anchor))
                for candidate_id in candidate_ids:
                    normalized = normalize_identifier(candidate_id)
                    if not normalized or normalized in seen_ids:
                        continue
                    if footnote_in_refs.get(normalized):
                        continue
                    note_text = footnote_map.get(normalized)
                    if not note_text:
                        continue
                    seen_ids.add(normalized)
                    collected.append(note_text)
            return collected

        def extract_text_with_refs(element):
            """Recursively extract text, inserting (Ref: N) where citations appear.
            Handles superscripts properly (no space before +, -, etc.)
            Groups consecutive references like (Ref: 1, 2, 3) instead of (Ref: 1), (Ref: 2), (Ref: 3)
            """
            parts = []
            pending_refs = []  # Collect consecutive reference numbers
            
            def flush_refs():
                """Output collected references as a single (Ref: X, Y, Z) annotation."""
                nonlocal pending_refs
                if pending_refs:
                    parts.append(f" (Ref: {', '.join(pending_refs)})")
                    pending_refs = []
            
            for child in element.children:
                if isinstance(child, NavigableString):
                    text = str(child)
                    # Skip whitespace-only text between references
                    if text.strip():
                        # Flush any pending refs before adding text
                        flush_refs()
                        parts.append(text)
                    # Don't flush refs for whitespace - might be between citations
                
                elif isinstance(child, Tag):
                    # Skip dropdown citation blocks
                    if child.name == "div" and "dropBlock__holder" in child.get("class", []):
                        continue
                    
                    # Handle reference citations - collect for grouping
                    if child.name == "a" and child.get("role") == "doc-biblioref":
                        # Extract reference number from sup tag
                        sup = child.find("sup")
                        if sup:
                            ref_num = sup.get_text(strip=True)
                            pending_refs.append(ref_num)
                        continue
                    
                    # Handle sup/sub tags (separators or other superscripts)
                    if child.name in {"sup", "sub"}:
                        # Check if this is a separator between references
                        sup_text = child.get_text(strip=True)
                        if sup_text in {",", ";", "and", "&", "–", "-"}:
                            # It's a separator between refs, keep collecting
                            continue
                        
                        # Check if this is NOT a reference citation (those are handled above)
                        parent_is_ref = child.parent.name == "a" and child.parent.get("role") == "doc-biblioref"
                        if not parent_is_ref:
                            # Flush pending refs before adding superscript
                            flush_refs()
                            if sup_text:
                                parts.append(sup_text)
                        continue
                    
                    # Skip other inline formatting tags but process their children
                    if child.name in {"span", "strong", "em", "i", "b"}:
                        # Peek at child content to see if it's just a separator
                        child_parts = extract_text_with_refs(child)
                        # Check if child contains only separators (comma, semicolon, etc.)
                        child_text = "".join(str(p) for p in child_parts).strip()
                        if child_text in {",", ";", "and", "&", "–", "-"}:
                            # It's a separator, keep collecting refs
                            continue
                        elif child_text:
                            # Not a separator and has content, flush refs
                            flush_refs()
                            parts.extend(child_parts)
                        # If empty, skip it
                        continue
                    
                    # For other tags, flush refs and recurse
                    flush_refs()
                    parts.extend(extract_text_with_refs(child))
            
            # Flush any remaining refs at the end
            flush_refs()
            return parts

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
            nonlocal pending_bullet_prefix, current_section_parts
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
            
            # Add to current section
            current_section_parts.append(f"{cleaned}\n")
            text_parts.append(f"{cleaned}\n")

        def append_heading(level: int, text: str) -> None:
            nonlocal current_section, current_section_parts, section_stack
            heading_text = clean_text(text)
            if should_skip_text(heading_text):
                return
            level = max(1, min(level, 6))
            ensure_paragraph_break()
            
            # For top-level headings (h1, h2), start a new section
            if level <= 2:
                # Save current section
                if current_section_parts:
                    section_content = "".join(current_section_parts).strip()
                    if section_content:
                        json_data[current_section] = section_content
                
                # Start new section
                current_section = heading_text.lower().replace(" ", "_").replace("★", "")
                current_section_parts = []
            else:
                # For subsections, append as part of current section with markdown
                current_section_parts.append(f"{'#' * level} {heading_text}\n\n")
            
            text_parts.append(f"{'#' * level} {heading_text}\n")
            text_parts.append("\n")

        def append_list(list_tag: Tag, indent: int) -> None:
            items = [child for child in list_tag.find_all("li", recursive=False)]
            if not items:
                return

            for idx, item in enumerate(items, 1):
                bullet = f"{idx}. " if list_tag.name == "ol" else "- "
                
                # Collect all text including superscripts inline using extract_text_with_refs
                text_parts = extract_text_with_refs(item)
                full_text = "".join(text_parts).strip()
                full_text = re.sub(r' +', ' ', full_text)
                
                # Remove nested list text temporarily
                nested_lists = item.find_all(["ul", "ol"], recursive=False)
                for nested in nested_lists:
                    nested_parts = extract_text_with_refs(nested)
                    nested_text = "".join(nested_parts).strip()
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
            nonlocal current_section_parts
            rows = []
            for tr in table_tag.find_all("tr"):
                cells = []
                for cell in tr.find_all(["th", "td"]):
                    # Use extract_text_with_refs for proper superscript/reference handling
                    cell_parts = extract_text_with_refs(cell)
                    cell_text = "".join(cell_parts).strip()
                    # Normalize whitespace and clean up the text
                    cell_text = re.sub(r'\s+', ' ', cell_text)
                    cell_text = re.sub(r'\s*\|\s*', ' ', cell_text)  # Remove any pipe characters from cell content
                    cells.append(cell_text)
                if any(cell for cell in cells):
                    rows.append(cells)

            if not rows:
                return

            # Add table marker to both text_parts and current_section_parts
            text_parts.append("\n")
            text_parts.append("[Table]\n")
            current_section_parts.append("\n")
            current_section_parts.append("[Table]\n")
            
            # Output each row on its own line
            for row in rows:
                # Join cells with pipe separator
                line = " | ".join(row)
                if line.strip():
                    text_parts.append(line + "\n")
                    current_section_parts.append(line + "\n")
            
            text_parts.append("\n")
            current_section_parts.append("\n")

        def append_content(node, indent: int = 0) -> None:
            if isinstance(node, NavigableString):
                text = clean_text(str(node))
                append_line(text, indent=indent)
                return

            if not isinstance(node, Tag):
                return

            name = node.name.lower()

            if node in footnote_elements:
                return

            if name in skip_names:
                return

            if node.get("aria-hidden") == "true":
                return

            node_id = (node.get("id") or "").lower()
            if node_id == "references":
                return

            # Handle figures - but extract tables if they're inside
            if name == "figure" or (node.get("data-component") or "").lower() == "figure":
                # Check if this figure contains a table (search all descendants)
                table = node.find("table", recursive=True)
                if table:
                    append_table(table, indent)
                return

            classes = [cls.lower() for cls in node.get("class", [])]
            
            # Handle div.figure-wrap which may contain tables
            if any("figure-wrap" in cls for cls in classes):
                table = node.find("table", recursive=True)
                if table:
                    append_table(table, indent)
                return
            
            if any("figure" in cls for cls in classes):
                # Check if this element contains a table (search all descendants)
                table = node.find("table", recursive=True)
                if table:
                    append_table(table, indent)
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
                # Extract heading text with proper superscript handling
                text_parts = extract_text_with_refs(node)
                heading_text = "".join(text_parts)
                heading_text = re.sub(r' +', ' ', heading_text).strip()
                if heading_text:
                    append_heading(min(int(name[1]), 6), heading_text)
                return

            # Check for role="paragraph" attribute
            role_attr = (node.get("role") or "").lower()
            
            # Skip doc-footnotes
            if role_attr == "doc-footnote":
                return
            
            # Handle paragraphs and blockquotes
            # BUT: Skip paragraph handling if this element contains a table - let it process children normally
            if name in {"p", "blockquote"} or role_attr == "paragraph":
                # Check if this paragraph contains a table (anywhere in descendants)
                if node.find("table", recursive=True):
                    # This paragraph contains a table, so process children normally instead
                    pass  # Fall through to child processing
                else:
                    # Normal paragraph - extract text with inline references
                    text_parts = extract_text_with_refs(node)
                    
                    # Join and normalize whitespace
                    paragraph = "".join(text_parts)
                    # Normalize multiple spaces to single space, but preserve the structure
                    paragraph = re.sub(r' +', ' ', paragraph).strip()
                    # Clean up space before punctuation
                    paragraph = re.sub(r'\s+([.,;:!?])', r'\1', paragraph)
                    # Ensure space after punctuation
                    paragraph = re.sub(r'([.,;:!?])([A-Za-z])', r'\1 \2', paragraph)
                    
                    # Find footnote citations if any
                    inline_notes = collect_inline_footnotes(node)
                    if inline_notes:
                        notes_text = "; ".join(inline_notes)
                        paragraph = f"{paragraph} (Footnote: {notes_text})"
                    
                    if paragraph:
                        append_line(paragraph, indent=indent, allow_repeat=True)
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
        figures_text = []  # Collect figures for JSON
        if figures:
            text_parts.append("\n" + "=" * 80 + "\n")
            text_parts.append("FIGURES\n")
            text_parts.append("=" * 80 + "\n\n")
            for idx, fig in enumerate(figures, 1):
                caption = fig.find("figcaption")
                if caption:
                    # Remove only the dropdown blocks with full reference details
                    # Keep the citation links (a[role="doc-biblioref"]) so extract_text_with_refs can find them
                    for dropdown in caption.find_all("div", class_="dropBlock__holder"):
                        dropdown.decompose()
                    
                    # Also remove any span.dropBlock that contains the full citation text
                    # but NOT the citation links themselves
                    for ref_detail in caption.find_all("span", class_="dropBlock"):
                        # Only remove if it contains full reference text, not if it's just a citation link
                        if ref_detail.find("a", role="doc-biblioref") is None:
                            ref_detail.decompose()
                    
                    # Get figure label and title (with citation refs preserved)
                    fig_label = caption.find("span", class_="label")
                    fig_title = caption.find("span", class_="figure__title__text")
                    
                    # Collect figure caption parts for both text_parts and JSON
                    figure_caption_parts = []
                    
                    if fig_label or fig_title:
                        if fig_label:
                            label_parts = extract_text_with_refs(fig_label)
                            label_text = "".join(label_parts).strip()
                        else:
                            label_text = f"Figure {idx}"
                        
                        if fig_title:
                            title_parts = extract_text_with_refs(fig_title)
                            title_text = "".join(title_parts).strip()
                        else:
                            title_text = ""
                        
                        text_parts.append(f"\n### {label_text}")
                        if title_text:
                            text_parts.append(f": {title_text}")
                            figure_caption_parts.append(f"{label_text}: {title_text}")
                        else:
                            figure_caption_parts.append(label_text)
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
                            
                            # Use extract_text_with_refs for proper superscript/reference handling
                            para_parts = extract_text_with_refs(para)
                            para_text = "".join(para_parts).strip()
                            para_text = re.sub(r' +', ' ', para_text)
                            
                            if para_text and len(para_text) > 10:  # Skip very short text fragments
                                # Remove button text like "Hide caption" or "Figure viewer"
                                if para_text not in ['Hide caption', 'Figure viewer', 'Show caption', 'Collapse', 'Expand']:
                                    text_parts.append(f"{para_text}\n\n")
                                    figure_caption_parts.append(para_text)
                    else:
                        # Fallback: get all text from caption
                        caption_parts = extract_text_with_refs(caption)
                        caption_text = "".join(caption_parts).strip()
                        caption_text = re.sub(r' +', ' ', caption_text)
                        
                        if caption_text:
                            # Clean up button text
                            caption_text = caption_text.replace('Hide caption', '').replace('Figure viewer', '')
                            caption_text = caption_text.replace('Show caption', '').replace('Collapse', '').replace('Expand', '')
                            caption_text = ' '.join(caption_text.split())  # Normalize whitespace
                            if caption_text:
                                text_parts.append(f"{caption_text}\n\n")
                                figure_caption_parts.append(caption_text)
                    
                    # Add this figure to the figures collection for JSON
                    if figure_caption_parts:
                        figures_text.append("\n".join(figure_caption_parts))
        
        # Add figures to JSON if any were collected
        if figures_text:
            json_data["figures"] = "\n\n".join(figures_text)
        
        # Save the last section before adding references
        if current_section_parts:
            section_content = "".join(current_section_parts).strip()
            if section_content:
                json_data[current_section] = section_content
        
        # Add references as a separate section in JSON
        reference_entries = get_reference_entries()
        if reference_entries and "REFERENCES" not in "\n".join(text_parts):
            text_parts.append("\n" + "=" * 80 + "\n")
            text_parts.append("REFERENCES\n")
            text_parts.append("=" * 80 + "\n\n")
            
            # Build references string for JSON
            references_text = []
            for idx, entry in enumerate(reference_entries, 1):
                references_text.append(f"{idx}. {entry}")
                text_parts.append(f"{idx}. {entry}\n")
            
            # Add references to JSON structure
            if references_text:
                json_data["references"] = "\n".join(references_text)
        
        full_text = "".join(text_parts)
        
        if json_data or full_text.strip():
            logger.info(f"✅ Successfully extracted {len(json_data)} sections with {len(full_text)} characters total")
            return json_data
        else:
            logger.warning("⚠️ No text content extracted from page")
            return None
            
    except Exception as e:
        logger.error(f"❌ Failed to extract full-text: {e}")
        logger.debug(traceback.format_exc())
        return None


async def save_json_to_file(json_content: Dict, file_path: str) -> bool:
    """Save extracted content to a .json file.
    
    Args:
        json_content: The JSON content to save (dict with sections as keys)
        file_path: Absolute path where the file should be saved
        
    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(json_content, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Saved JSON to: {file_path}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to save JSON file: {e}")
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
        out_folder: Output folder for JSON files
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
                filename = f"{safe_title}.json"
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
                
                # Extract JSON from full-text page
                json_content = await extract_fulltext_as_json(page, fulltext_link)
                
                print(f"✅ Extraction completed. Sections: {len(json_content) if json_content else 0}", flush=True)
                
                if json_content:
                    # Save to JSON file
                    success = await save_json_to_file(json_content, dest_path)
                    
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
                        print(f"❌ Failed to save JSON file: {dest_path}", flush=True)
                else:
                    print(f"❌ Extracted JSON is empty or invalid", flush=True)
                    
            except Exception as e:
                print(f"❌ Failed to extract text for '{article_title[:50]}': {e}", flush=True)
                print(traceback.format_exc(), flush=True)
            
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
                        filename = f"{safe_title}.json"
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
                        
                        json_content = await extract_fulltext_as_json(page, fulltext_link)
                        
                        if json_content:
                            # Save to JSON file
                            success = await save_json_to_file(json_content, dest_path)
                            
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
                
                # Crawl issue archives if requested —
                # Also fall back to crawling issue pages when the /newarticles run
                # produced no saved JSONs for this journal (journal_download_count == 0).
                # This ensures we don't stop early just because the newarticles page
                # didn't yield any extractable JSON.
                should_crawl_archives = crawl_archives or (journal_download_count == 0)
                if should_crawl_archives:
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
                    
                    # STEP 1: Expand outer accordion sections (year ranges like "2010-2019")
                    # These are collapsed by default and contain volumes inside
                    try:
                        outer_accordions = archive_page.locator('a.accordion__control')
                        accordion_count = await outer_accordions.count()
                        print(f"🔧 Found {accordion_count} year range sections, expanding all...", flush=True)
                        
                        for i in range(accordion_count):
                            try:
                                accordion = outer_accordions.nth(i)
                                # Check if it's expanded (aria-expanded="true")
                                is_expanded = await accordion.get_attribute('aria-expanded')
                                if is_expanded != 'true':
                                    accordion_text = await accordion.text_content()
                                    await accordion.click()
                                    await archive_page.wait_for_timeout(800)
                                    print(f"  ✅ Expanded section: {accordion_text.strip()}", flush=True)
                            except Exception as e:
                                logger.debug(f"Failed to expand accordion {i}: {e}")
                        
                        # Wait for all accordion content to load
                        await archive_page.wait_for_timeout(1500)
                    except Exception as e:
                        print(f"⚠️ Failed to expand year range sections: {e}", flush=True)
                    
                    # STEP 2: Expand individual volume toggles for target years
                    # These are <a> tags with class "list-of-issues__group-expand"
                    volumes_to_expand = []
                    try:
                        volume_toggles = archive_page.locator('a.list-of-issues__group-expand')
                        toggle_count = await volume_toggles.count()
                        print(f"🔧 Found {toggle_count} volume toggles, identifying target volumes...", flush=True)
                        
                        # First pass: identify which volumes to expand
                        for i in range(toggle_count):
                            try:
                                toggle = volume_toggles.nth(i)
                                volume_text = await toggle.text_content()
                                if volume_text:
                                    year_match = re.search(r'\((\d{4})\)', volume_text)
                                    if year_match:
                                        vol_year = int(year_match.group(1))
                                        if year_from <= vol_year <= year_to + 1:
                                            volumes_to_expand.append((i, volume_text.strip()))
                            except Exception as e:
                                logger.debug(f"Failed to check volume toggle {i}: {e}")
                        
                        # Second pass: click all target volumes
                        print(f"🔧 Expanding {len(volumes_to_expand)} volumes...", flush=True)
                        for idx, vol_text in volumes_to_expand:
                            try:
                                toggle = volume_toggles.nth(idx)
                                await toggle.click()
                                print(f"  ✅ Clicked: {vol_text}", flush=True)
                                await archive_page.wait_for_timeout(500)
                            except Exception as e:
                                logger.debug(f"Failed to click volume {vol_text}: {e}")
                        
                        # Wait for all AJAX content to load
                        if volumes_to_expand:
                            print(f"⏳ Waiting for issue lists to load...", flush=True)
                            await archive_page.wait_for_timeout(3000)
                            
                            # Wait for issue links to appear in the DOM
                            try:
                                await archive_page.wait_for_selector('a[href*="/issue?pii="]', timeout=5000, state='attached')
                            except:
                                pass  # Continue even if selector doesn't appear
                                
                    except Exception as e:
                        print(f"⚠️ Failed to expand volume toggles: {e}", flush=True)

                    html = await archive_page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    
                    print(f"📂 Parsing issue links from page HTML...", flush=True)
                    issue_links = []
                    in_open_archive = False
                    
                    # Broaden selector to catch multiple issue URL patterns.
                    # Some pages may use different href formats for older issues.
                    all_issue_links = soup.select(
                        'a[href*="/issue?pii="]'
                    )
                    print(f"🔍 Found {len(all_issue_links)} total issue links on page", flush=True)
                    
                    for link in all_issue_links:
                        href = link.get("href", "")
                        if not href:
                            continue
                        
                        # Check if this is after the Open Archive marker
                        parent_li = link.find_parent("li")
                        if parent_li:
                            open_archive_div = parent_li.find_previous("div", class_="list-of-issues__open-archive")
                            if open_archive_div and not in_open_archive:
                                in_open_archive = True
                                print(f"📂 Entered Open Archive section", flush=True)
                        
                        # Try to extract date/year from the link or its parent <li> text.
                        # Use a robust regex to find a 4-digit year (e.g., 2024).
                        try:
                            link_text = link.get_text(" ", strip=True)
                            # Prefer the parent <li> text when available (it contains issue spans)
                            parent_li = link.find_parent("li")
                            if parent_li:
                                block_text = parent_li.get_text(" ", strip=True)
                            else:
                                block_text = link_text

                            # Normalize whitespace and collapse concatenated tokens
                            block_text = re.sub(r"\s+", " ", block_text)

                            year_match = re.search(r"\b(19|20)\d{2}\b", block_text)
                            if year_match:
                                issue_year = int(year_match.group(0))
                                date_text = block_text
                                if year_from <= issue_year <= year_to:
                                    full_url = urljoin("https://www.cell.com", href)
                                    if (full_url, in_open_archive, date_text) not in issue_links:
                                        issue_links.append((full_url, in_open_archive, date_text))
                                        logger.debug(f"✅ Found issue: {date_text[:50]} ({'Open Archive' if in_open_archive else 'Regular'})")
                                else:
                                    logger.debug(f"⏭️  Skipped issue (year {issue_year} not in range): {date_text[:50]}")
                            else:
                                logger.debug(f"⚠️  No year found in link text for: {href[:50]}")
                        except Exception as e:
                            logger.debug(f"⚠️  Failed to parse date from link {href[:50]} - {e}")
                    
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
    
    print(f"\n🎉 Extracted {found_count} JSON files to {out_folder}")
    
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
        print(f"\n📦 Creating ZIP archive with all extracted JSON files...")
        
        zip_filename = f"all_journals_json_{timestamp}.zip"
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
            logger.info(f"📦 Archive contains {len(saved_files)} JSON files from {len(set(os.path.dirname(f) for f in saved_files))} journals")
        except Exception as e:
            logger.error(f"❌ Failed to create ZIP archive: {e}")
    
    return saved_files, open_access_articles
