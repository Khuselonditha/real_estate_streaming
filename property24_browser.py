import logging
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# Logging config
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

site = "https://www.property24.com/"
location = "Sandton"

async def run(pw):
    logging.info("Launching browser...")
    browser = await pw.chromium.launch(headless=False)

    try:
        logging.info(f"Navigating to {site}...")
        page = await browser.new_page()
        await page.goto(site)
        title = await page.title()
        logging.info(f"Page title: {title}")

        # Locate input field inside the token list
        input_field = page.locator("#token-input-AutoCompleteItems")

        # Type "Sandton" and wait for suggestions
        await input_field.fill("Sandton")
        await page.wait_for_timeout(4000)  # Adjust timeout based on how long it takes for suggestions to appear
        await input_field.press('Enter')  # Simulate pressing Enter if necessary

        # Click the search button
        await page.locator("button.btn.btn-danger").click()

        # Wait for results page to load
        logging.info("Waiting for results to load...")
        await page.wait_for_selector("div.js_listingResultsContainer", state="visible")

        # Get the inner HTML of the results container
        content = await page.inner_html("div.js_listingResultsContainer")
        logging.info("Results page loaded successfully.")

        print(content)             # Delete when refactoring

        # Pass HTML to BeautifulSoup for parsing
        soup = BeautifulSoup(content, "html.parser")

        # Find all divs with class starting with "p24_tileContainer js_resultTile"
        tiles = soup.find_all("div", class_=lambda c: c and c.startswith("p24_tileContainer js_resultTile "))
        logging.info(f"Found {len(tiles)} property tiles.")
    except Exception as e:
        logging.info(f"An error occurred due to: {e}")
    finally:
        await browser.close()


async def main():
    async with async_playwright() as playwright:
        await run(playwright)



if __name__ == "__main__":
    asyncio.run(main())