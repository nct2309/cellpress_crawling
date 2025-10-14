"""Streamlit UI for the papers crawler.

Run with:
    poetry run streamlit run scripts/run_crawler_streamlit.py

This avoids OS-level GUI deps like tkinter.
"""
from __future__ import annotations

import os
import threading
from typing import List
import time

import streamlit as st

# ensure src package is importable when running from repo root
import sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from papers_crawler.crawler import crawl, discover_journals


st.set_page_config(page_title="Cell.com PDF Crawler", layout="wide")

st.title("Cell.com PDF Crawler")
st.markdown("Crawl **open-access** PDFs from Cell.com journals by year range")

# Load journals button (outside form)
if st.button("🔄 Load journals from Cell.com"):
    with st.spinner("Fetching journals from navbar..."):
        try:
            st.session_state["journals"] = discover_journals(force_refresh=True)
            journal_count = len(st.session_state.get('journals', []))
            if journal_count > 0:
                st.success(f"✅ Loaded {journal_count} journals!")
            else:
                st.error("❌ Failed to load journals. Check that Playwright is installed: `poetry run playwright install chromium`")
        except Exception as e:
            error_msg = str(e)
            if "403 Forbidden" in error_msg:
                st.error("🚫 **Cell.com is blocking automated requests**")
                st.warning("""
                **This is likely due to anti-bot protection.** Here are some solutions:
                
                1. **Wait and retry**: Sometimes this is temporary
                2. **Use a VPN**: Try from a different IP address
                3. **Manual journal selection**: You can manually enter journal slugs below
                4. **Contact Cell.com**: They may have changed their access policies
                """)
                
                # Provide manual journal entry option
                st.info("**Manual Journal Entry**: If you know the journal slugs, you can enter them manually:")
                manual_journals = st.text_area(
                    "Enter journal slugs (one per line):",
                    value="cell\nimmunity\nneuron\ncurrent-biology",
                    help="Enter the journal slugs you want to crawl, one per line"
                )
                if manual_journals.strip():
                    journal_slugs = [slug.strip() for slug in manual_journals.split('\n') if slug.strip()]
                    if journal_slugs:
                        st.session_state["journals"] = [(slug, slug.title()) for slug in journal_slugs]
                        st.success(f"✅ Using {len(journal_slugs)} manually entered journals!")
            elif "Using" in error_msg and "hardcoded" in error_msg:
                # This means the fallback worked
                st.warning("⚠️ **Using cached/hardcoded journal list**")
                st.info("Cell.com is blocking requests, but we're using a pre-loaded journal list. This should still work for crawling.")
                # Try to load journals again to get the fallback results
                try:
                    st.session_state["journals"] = discover_journals(force_refresh=False)
                    journal_count = len(st.session_state.get('journals', []))
                    if journal_count > 0:
                        st.success(f"✅ Loaded {journal_count} journals from cache/fallback!")
                except Exception:
                    pass
            else:
                st.error(f"❌ Error loading journals: {error_msg}")
                st.info("Make sure Playwright browsers are installed: `poetry run playwright install chromium`")

st.divider()

# Form for crawl configuration
with st.form("crawl_form"):
    st.subheader("📚 Select Journals")
    
    journals = st.session_state.get("journals", [])
    selected_journals = []
    
    if journals:
        st.info(f"{len(journals)} journals available. Select one or more to crawl.")
        
        # Add "Select All" checkbox
        select_all = st.checkbox("✅ Select All Journals", key="select_all_journals")
        
        # Create 3 columns for better layout
        cols = st.columns(3)
        for idx, (slug, name) in enumerate(journals):
            with cols[idx % 3]:
                if st.checkbox(f"{name}", key=f"journal_{slug}", value=select_all):
                    selected_journals.append(slug)
    else:
        st.warning("⚠️ Click 'Load journals from Cell.com' above to see available journals.")
    
    st.divider()
    st.subheader("⚙️ Crawl Settings")
    
    col1, col2 = st.columns(2)
    with col1:
        year_from = st.number_input("Year from", min_value=1900, max_value=2100, value=2020)
    with col2:
        year_to = st.number_input("Year to", min_value=1900, max_value=2100, value=2024)

    out_folder = st.text_input("Output folder", value="./downloads")
    headless = st.checkbox("Headless mode (browser in background)", value=True)
    limit = st.number_input("Limit articles per journal (0 = no limit)", min_value=0, value=5)
    submit = st.form_submit_button("🚀 Start Crawl")

if submit:
    if not selected_journals:
        st.error("❌ Please select at least one journal to crawl")
    else:
        st.info(f"📥 Crawling {len(selected_journals)} journal(s): {', '.join(selected_journals)}")
        
        # Progress tracking
        overall_progress_bar = st.progress(0, text="Initializing...")
        status_text = st.empty()
        speed_text = st.empty()
        
        # Container for downloaded files list
        downloaded_files_container = st.container()
        
        downloaded_files_list = []
        open_access_articles_list = []
        
        def progress_callback(filename, filepath):
            """Callback function to track individual file downloads"""
            downloaded_files_list.append(filename)
        
        def total_progress_callback(current, total, status_message, file_size=0, speed_kbps=0, stage=""):
            """Callback function to track overall progress with speed metrics and stage indicators"""
            if total > 0:
                progress = current / total
                percentage = progress * 100
                overall_progress_bar.progress(progress, text=f"{current}/{total} files ({percentage:.1f}%)")
                
                # Show different indicators based on stage
                if stage == "starting":
                    status_text.text(f"� Initiating download: {status_message}")
                    speed_text.text("🔄 Connecting to server...")
                elif stage == "downloading":
                    status_text.text(f"⬇️ Downloading in progress: {status_message}")
                    speed_text.text("📡 Receiving data...")
                elif stage == "completed":
                    status_text.text(f"✅ {status_message}")
                    # Show download speed if available
                    if speed_kbps > 0:
                        if speed_kbps > 1024:
                            speed_mbps = speed_kbps / 1024
                            speed_text.text(f"⚡ Average speed: {speed_mbps:.2f} MB/s | File size: {file_size/1024:.1f} KB")
                        else:
                            speed_text.text(f"⚡ Average speed: {speed_kbps:.1f} KB/s | File size: {file_size/1024:.1f} KB")
                    else:
                        speed_text.text("")
                else:
                    status_text.text(f"📊 {status_message}")
                    speed_text.text("")
            else:
                # Still discovering articles
                overall_progress_bar.progress(0, text="Scanning journals...")
                status_text.text(f"🔍 {status_message}")
                speed_text.text("")
        
        # Run synchronously in Streamlit's script context to avoid NoSessionContext errors.
        try:
            downloaded_files_list, open_access_articles_list = crawl(
                keywords="",  # Not used when journal_slugs provided
                year_from=int(year_from),
                year_to=int(year_to),
                out_folder=out_folder,
                headless=headless,
                limit=(None if int(limit) == 0 else int(limit)),
                journal_slugs=selected_journals,
                progress_callback=progress_callback,
                total_progress_callback=total_progress_callback,
            )
            
            # Complete progress
            overall_progress_bar.progress(1.0, text=f"{len(downloaded_files_list)}/{len(downloaded_files_list)} files (100%)")
            status_text.text("✅ Crawl complete!")
            speed_text.text("")
            
            # Display results
            st.success(f"🎉 Crawl complete! Downloaded {len(downloaded_files_list)} files to {os.path.abspath(out_folder)}")
            
            # Show open access articles found
            if open_access_articles_list:
                st.subheader("📚 Open Access Articles Found")
                for i, title in enumerate(open_access_articles_list, 1):
                    st.write(f"{i}. {title}")
            
            # Show downloaded files
            if downloaded_files_list:
                st.subheader("📁 Downloaded Files")
                for i, filepath in enumerate(downloaded_files_list, 1):
                    filename = os.path.basename(filepath)
                    # Extract journal folder from path
                    journal_folder = os.path.basename(os.path.dirname(filepath))
                    st.write(f"{i}. [{journal_folder}] {filename}")
            else:
                st.warning("⚠️ No files were downloaded. This could mean:")
                st.write("- No open access articles found in the specified year range")
                st.write("- Network connectivity issues")
                st.write("- Changes in the website structure")
                    
        except Exception as e:
            error_msg = str(e)
            if "Cloudflare challenge" in error_msg:
                st.warning(error_msg)
                st.error("🚫 **Cloudflare Challenge Detected**")
                st.warning("""
                **Cell.com is using Cloudflare protection to block automated requests.** Here are solutions:
                
                1. **Wait and retry**: Cloudflare challenges are often temporary
                2. **Use a VPN**: Try from a different IP address
                3. **Try different times**: Peak hours may have more protection
                4. **Manual download**: You can manually download PDFs from the website
                5. **Contact Cell.com**: They may have changed their access policies
                """)
                
                st.info("**Alternative**: You can try running the crawler from a different network or at different times when the protection may be lighter.")
            else:
                st.error(f"❌ Error during crawling: {error_msg}")
                st.write("This error occurred due to:")
                st.write("- Network connectivity issues")
                st.write("- Website changes")
                st.write("- Invalid journal selection")
                st.write("- Browser/Playwright issues")
