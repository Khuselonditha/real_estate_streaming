import logging
import asyncio
from playwright.async_api import async_playwright

# Logging config
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

site = "https://www.property24.com/"
location = "Sandton"

async def run(pw):
    logging.info("Launching browser...")
    browser = await pw.chromium.launch()

    try:
        logging.info(f"Navigating to {site}...")
        page = await browser.new_page()
        await page.goto(site)
        title = await page.title()
        logging.info(f"Page title: {title}")
    except Exception as e:
        logging.info(f"An error occurred dur to: {e}")
    finally:
        await browser.close()


async def main():
    async with async_playwright() as playwright:
        await run(playwright)



if __name__ == "__main__":
    asyncio.run(main())