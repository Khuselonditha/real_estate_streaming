import asyncio
from playwright.async_api import async_playwright


async def run(pw):
    site = "https://www.property24.com/"

    print("Connecting to scraping browser...")
    browser = await pw.chromium.launch()
    page = await browser.new_page()
    await page.goto(site)
    print(await page.title())
    await browser.close()


async def main():
    async with async_playwright() as playwright:
        await run(playwright)



if __name__ == "__main__":
    asyncio.run(main())