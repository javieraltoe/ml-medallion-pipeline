"""Genera docs/bitacora/bitacora.pdf a partir de bitacora.html.

    pip install playwright && playwright install chromium
    python docs/bitacora/build_pdf.py

Alternativa sin instalar nada: abrir bitacora.html en Chrome -> Imprimir -> Guardar como PDF.
"""

from pathlib import Path

from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent
SOURCE = HERE / "bitacora.html"
TARGET = HERE / "bitacora.pdf"

FOOTER = """
<div style="width:100%; font-size:7.5pt; color:#7b8794; padding:0 18mm;
            display:flex; justify-content:space-between; font-family:Arial, sans-serif;">
  <span>Pipeline Medallion · Mercado Libre — Bitácora del proyecto</span>
  <span><span class="pageNumber"></span> / <span class="totalPages"></span></span>
</div>
"""


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(SOURCE.as_uri(), wait_until="networkidle")
        page.pdf(
            path=str(TARGET),
            format="A4",
            print_background=True,
            prefer_css_page_size=True,
            display_header_footer=True,
            header_template="<div></div>",
            footer_template=FOOTER,
        )
        browser.close()
    print(f"PDF generado: {TARGET}")


if __name__ == "__main__":
    main()
