from playwright.sync_api import sync_playwright
import time

INJECT_PREFIX = "BLOB_PDF_DETECTED:"

create_override = f"""
(() => {{
  const orig = URL.createObjectURL.bind(URL);
  URL.createObjectURL = function(blob) {{
    try {{
      if (blob && blob.type && blob.type.includes('pdf')) {{
        blob.slice(0,32).arrayBuffer().then(ab => {{
          let view = new Uint8Array(ab);
          let hex = Array.from(view).map(b=>b.toString(16).padStart(2,'0')).slice(0,16).join('');
          console.log('{INJECT_PREFIX}', hex);
        }});
      }}
    }} catch(e){{ console.warn('createObjectURL hook error', e); }}
    return orig(blob);
  }};
}})();
"""

def intercept_blob(page_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context()
        ctx.add_init_script(create_override)
        page = ctx.new_page()
        page.on("console", lambda msg: print("[PAGE]", msg.text))
        print("Navigating:", page_url)
        page.goto(page_url, wait_until="networkidle", timeout=120000)
        print("Waiting up to 30s for blob creation...")
        for i in range(30):
            time.sleep(1)
        print("Done. Check console above for", INJECT_PREFIX)
        browser.close()

if __name__ == "__main__":
    intercept_blob("https://www.cell.com/immunity/pdf/S1074-7613(25)00425-X.pdf")
