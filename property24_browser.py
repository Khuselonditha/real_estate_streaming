import logging
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# Logging config
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

site = "https://www.property24.com"
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
        await page.wait_for_timeout(4000) 
        await input_field.press('Enter')  

        # Click the search button
        await page.locator("button.btn.btn-danger").click()

        # Wait for network idle before scraping
        await page.wait_for_load_state("networkidle")

        # Check if the container is visible
        if not await page.wait_for_selector("div.p24_tileContainer", state="visible", timeout=10000):
            logging.warning("Results container not found. Retrying...")
            await page.wait_for_timeout(10000)  

        # Get the updated inner HTML of the results container
        content = await page.inner_html("div.js_listingResultsContainer")
        logging.info("Results page loaded successfully.")

        # Pass HTML to BeautifulSoup for parsing
        soup = BeautifulSoup(content, "html.parser")

        # Find all divs with class starting with "p24_tileContainer js_resultTile"
        tiles = soup.find_all("div", class_=lambda c: c and "js_resultTile" in c.split())
        logging.info(f"Found {len(tiles)} property tiles.")

        # Get the information from each search result
        for idx, div in enumerate(tiles):
            data = {}

            title_element = div.find("div", class_="p24_description")
            address_element = div.find("span", class_="p24_address")
            price_element = div.find("div", class_="p24_price")
            size_element = div.find("span", class_="p24_size")
            bedrooms_element = div.find("div", class_="p24_featureDetails", title="Bedrooms")
            bathrooms_element = div.find("div", class_="p24_featureDetails", title="Bathrooms")
            parking_element = div.find("div", class_="p24_featureDetails", title="Parking Spaces")
            link = div.find("a")["href"]

            title = title_element.get_text() if title_element else None
            address = address_element.get_text(strip=True) if address_element else None
            price = price_element.get_text(strip=True) if price_element else None
            size = size_element.get_text(strip=True) if size_element else None
            bedrooms = bedrooms_element.get_text(strip=True) if bedrooms_element else None    
            bathrooms = bathrooms_element.get_text(strip=True) if bathrooms_element else None    
            parking = parking_element.get_text(strip=True) if parking_element else None
                

            data.update({
                "title": title,
                "address": address,
                "price": price,
                "size": size,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "parking": parking,
                "link": f"{site}{link}" 
            })


            # Navigate to everytile to get the pictures
            tile = f"{site}{link}"
            logging.info(f"Navigating to {tile}")
            await page.goto(tile)
            await page.wait_for_load_state("networkidle")

            # Check if the pictures container is visible
            if not await page.wait_for_selector("div.main-gallery-images-container", state="visible", timeout=10000):
                logging.warning("Pictures container not found. Retrying...")
                await page.wait_for_timeout(10000)

            # Get the updated inner HTML of the pictures container
            content = await page.inner_html("div.main-gallery-images-container")
            logging.info("Pictures page loaded successfully.")

            soup = BeautifulSoup(content, "html.parser")

            # Return all pictures of each tile
            pictures = get_pictures()

            print(data)
            break

    except Exception as e:
        logging.info(f"An error occurred due to: {e}")
    finally:
        await browser.close()


async def main():
    async with async_playwright() as playwright:
        await run(playwright)



if __name__ == "__main__":
    asyncio.run(main())