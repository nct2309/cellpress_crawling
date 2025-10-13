# Cell.com PDF Crawler

This tool crawls **open-access** PDFs from Cell.com journals. You can select specific journals from the Cell.com navbar and download articles within a year range.

## Features

- 📚 Discovers all journals from Cell.com's navbar menu
- ✅ Filters for open-access articles only
- 📅 Year range filtering
- 🎯 Multi-journal selection
- 🌐 Web UI with Streamlit (no OS GUI dependencies)

## Installation

1. Install dependencies with Poetry:

```bash
# Playwright is recommended because it bundles browsers and works well in WSL
poetry add playwright beautifulsoup4 requests streamlit

# Install Chromium browser binaries (run once):
poetry run playwright install chromium
```

## Usage

### Streamlit Web UI (Recommended)

```bash
poetry run streamlit run scripts/run_crawler_streamlit.py
```

1. Click **"Load journals from Cell.com"** to fetch the list of available journals
2. Select one or more journals using checkboxes
3. Set your year range (e.g., 2020-2024)
4. Choose output folder
5. Click **"Start Crawl"** to download open-access PDFs

### Programmatic Usage

```python
from papers_crawler.crawler import crawl, discover_journals

# Discover available journals
journals = discover_journals()
print(f"Found {len(journals)} journals")

# Crawl specific journals
crawl(
    keywords="",
    year_from=2020,
    year_to=2024,
    out_folder="./papers",
    headless=True,
    limit=10,  # limit per journal
    journal_slugs=["cell", "immunity", "neuron"],
)
```

## How It Works

1. **Journal Discovery**: Parses Cell.com's navbar menu to extract journal slugs and names
2. **Article Crawling**: For each selected journal, visits the `/newarticles` page
3. **Open Access Filtering**: Only downloads articles marked as open access
4. **PDF Download**: Uses Playwright's request API to maintain session and download PDFs

## Troubleshooting

### 403 Forbidden Error / Cloudflare Challenges

If you encounter a "403 Client Error: Forbidden" or "Cloudflare Challenge Detected" when loading journals or downloading PDFs, this means Cell.com is blocking automated requests due to anti-bot protection. Here are the solutions:

1. **Automatic Fallback**: The tool will automatically try Playwright if requests fail, and use a hardcoded journal list if both fail
2. **Wait and Retry**: Cloudflare challenges are often temporary - try again later
3. **Use a VPN**: Try from a different IP address to bypass geographic restrictions
4. **Try Different Times**: Peak hours may have more protection
5. **Manual Journal Entry**: The UI provides an option to manually enter journal slugs
6. **Manual Download**: You can manually download PDFs from the website
7. **Contact Cell.com**: They may have changed their access policies

### Common Journal Slugs

If you need to manually enter journals, here are some common slugs:
- `cell` - Cell
- `immunity` - Immunity  
- `neuron` - Neuron
- `current-biology` - Current Biology
- `cell-reports` - Cell Reports
- `cell-metabolism` - Cell Metabolism

## Notes

- ⚠️ This tool only downloads **open-access** articles to respect copyright
- 🤝 Uses polite delays (1 second between downloads) to avoid overloading the server
- 💾 Journal lists are cached in `.cache/papers_crawler/journals.json`
- 🔧 Built with Playwright for better cross-platform support (especially WSL/Linux)
- 🛡️ Includes fallback mechanisms for when Cell.com blocks automated requests
