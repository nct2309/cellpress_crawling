# Cell.com PDF Crawler

This tool crawls **open-access** PDFs from Cell.com journals. You can select specific journals from the Cell.com navbar and download articles within a year range.

## Features

- 📚 Discovers all journals from Cell.com's navbar menu
- ✅ Filters for open-access articles only
- 📅 Year range filtering
- 🎯 Multi-journal selection
- 🌐 Web UI with Streamlit (no OS GUI dependencies)

## Installation

### Option 1: Using pip (Recommended for most users)

```bash
# Create a virtual environment (recommended)
python -m venv venv

# Activate the virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Install dependencies from requirements.txt
pip install -r requirements.txt --upgrade --no-deps

# Install Firefox browser binaries (run once):
playwright install firefox
playwright install --only-shell

```

### Option 2: Using Poetry

```bash
# Playwright is recommended because it bundles browsers and works well in WSL
poetry add playwright beautifulsoup4 requests streamlit

# Install Firefox browser binaries (run once):
poetry run playwright install firefox
poetry run playwright install --only-shell
```

## Usage

### Streamlit Web UI (Recommended)

**With pip:**
```bash
streamlit run scripts/run_crawler_streamlit.py
```

**With Poetry:**
```bash
poetry run streamlit run scripts/run_crawler_streamlit.py
```

1. Click **"Load journals from Cell.com"** to fetch the list of available journals
2. Select one or more journals using checkboxes
3. Set your year range (e.g., 2020-2024)
4. Choose output folder
5. Click **"Start Crawl"** to download open-access PDFs

### Programmatic Usage

**Regular Python scripts:**
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

**Google Colab / Jupyter Notebooks:**

📓 **See complete example:** [`examples/colab_example.ipynb`](examples/colab_example.ipynb)

**Important:** In Colab/Jupyter, use the async functions with `await`:

```python
# Import the async functions directly (NOT the regular crawler!)
from src.papers_crawler.crawler_async import crawl_async, discover_journals_async

# Discover available journals (use await since Colab runs in an async environment)
journals = await discover_journals_async()
print(f"Found {len(journals)} journals")

# Show first 5 journals
for slug, name in journals[:5]:
    print(f"  {slug}: {name}")

# Crawl specific journals (use await)
downloaded_files, articles = await crawl_async(
    year_from=2020,
    year_to=2025,
    out_folder="./papers",
    headless=True,
    limit=12,  # limit per journal, limit = 0 is unlimited/ crawl all
    journal_slugs=["cell", "immunity", "neuron"],
)

print(f"Downloaded {len(downloaded_files)} PDFs")
```

**Note:** The logger configuration at the top enables real-time progress messages showing:
- ⬇️ Start downloading file: [Article Title]
- ✅ Downloaded file: [Filename] (size & speed)

## How It Works

1. **Journal Discovery**: Parses Cell.com's navbar menu to extract journal slugs and names
2. **Article Crawling**: For each selected journal, visits the `/newarticles` page
3. **Open Access Filtering**: Only downloads articles marked as open access
4. **PDF Download**: Uses Firefox with Playwright to automatically download PDFs by clicking links
5. **Cookie Consent**: Automatically handles and accepts cookie consent popups

## Troubleshooting

### Google Colab / Jupyter Notebooks

If you get errors like:
- `"It looks like you are using Playwright Sync API inside the asyncio loop"`
- `"RuntimeError: This event loop is already running"`

You need to use the **async versions** with `await`:

**❌ Don't use (will fail in Colab):**
```python
from src.papers_crawler.crawler import crawl, discover_journals
journals = discover_journals()  # Error!
```

**✅ Do use (works in Colab):**
```python
from src.papers_crawler.crawler_async import crawl_async, discover_journals_async

# Use await since Colab runs in an async environment
journals = await discover_journals_async()  # Works!
downloaded_files, articles = await crawl_async(...)  # Works!
```

📓 **See complete working example:** [`examples/colab_example.ipynb`](examples/colab_example.ipynb)

**Installation in Colab:**
```bash
# Clone and install
!git clone https://github.com/nct2309/cellpress_crawling.git
%cd cellpress_crawling
!pip install -r requirements.txt --upgrade --no-deps
!playwright install firefox
!playwright install --only-shell
```

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
- 🦊 Uses Firefox with Playwright for better cross-platform support and reliable PDF downloads
- 🍪 Automatically handles cookie consent popups
- 🛡️ Includes fallback mechanisms for when Cell.com blocks automated requests
