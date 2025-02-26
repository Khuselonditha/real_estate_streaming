import json
import logging
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from kafka import KafkaProducer


# Logging config
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

site = "https://www.property24.com"
location = "Sandton"

def get_pictures(parent_div):
    pictures = []
    for div in parent_div:
        img_tag = div.get('data-image-url')
        if img_tag:
            pictures.append(img_tag)

    return pictures

def extract_overviews(overview_div):
    needed_values = ["Type of Property", "Lifestyle", "Listing Date", "Levies", 
        "No Transfer Duty", "Rates and Taxes", "Pets Allowed"]
    
    property_values = {key: None for key in needed_values}

    if not overview_div:
        return property_values

    for div in overview_div:
        try:
            # Extract the key (e.g., "Type of Property")
            key_element = div.find("div", class_="col-6 p24_propertyOverviewKey")
            key = key_element.text.strip() if key_element else None

            # Extract the value (e.g., "Townhouse")
            value_element = div.find("div", class_="col-6 noPadding").find("div", class_="p24_info")
            value = value_element.text.strip() if value_element else None

            if key in needed_values:
                property_values[key.lower()] = value

        except AttributeError:
            # Skip if any element is missing
            continue

    return property_values


async def check_containers_visible(page):
    try:
        # Check if the pictures container is visible
        if not await page.wait_for_selector("#main-gallery-images-container", state="visible", timeout=10000):
            logging.warning("Pictures container not found. Retrying...")
            await page.wait_for_timeout(10000)
            return False

        # Check if the second container is visible
        if not await page.wait_for_selector("#js_accordion_propertyoverview", state="visible", timeout=10000):
            logging.warning("Property overview container not found. Retrying...")
            await page.wait_for_timeout(10000)
            return False
        
        # Both containers are visible
        return True

    except Exception as e:
        logging.info(f"Error while checking containers due to: {e}")
        return False



async def run(pw, producer):
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
        for div in tiles:
            data = {}

            title_element = div.find("div", class_="p24_description")
            address_element = div.find("span", class_="p24_address")
            price_element = div.find("div", class_="p24_price")
            size_element = div.find("span", class_="p24_size")
            bedrooms_element = div.find("span", class_="p24_featureDetails", title="Bedrooms")
            bathrooms_element = div.find("span", class_="p24_featureDetails", title="Bathrooms")
            parking_element = div.find("span", class_="p24_featureDetails", title="Parking Spaces")
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

            # Check if the pictures containers are visible
            if not await check_containers_visible(page):
                logging.warning("One of the containers is not found. Skipping this tile...")
                continue

            # Get the updated inner HTML of the pictures container
            content = await page.inner_html("#main-gallery-images-container")
            logging.info("Pictures div loaded successfully.")

            soup = BeautifulSoup(content, "html.parser")

            # Return all pictures of each tile
            picture_div = soup.find_all("div", class_=lambda c: c and any(cls in c.split() for cls in ["p24_galleryImageHolder", "js_galleryImage"]))
            logging.info(f"Found {len(picture_div)} images.")
            pictures = get_pictures(picture_div)

            # Get inner HTMl of the "Property overview" container
            content = await page.inner_html("#js_accordion_propertyoverview")
            logging.info("PropertiesOverview div loaded successfully.")

            soup = BeautifulSoup(content, "html.parser")

            # Return values needed
            property_overview_div = soup.find_all("div", class_="row p24_propertyOverviewRow")
            logging.info("Extracting from the row p24_propertyOverviewRow div")
            overviews = extract_overviews(property_overview_div)
            logging.info("Extaction successful")

            logging.info("Adding property overviews to our data")
            data.update(overviews)

            logging.info("Adding pictures to our data")
            data["pictures"] = pictures

            logging.info("Sending data to kafka")
            producer.send("properties", json.dumps(data).encode("utf-8"))
            logging.info("Data sent to kafka")
            print(data)
            break

    except Exception as e:
        logging.info(f"An error occurred due to: {e}")
    finally:
        await browser.close()


async def main():
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=5000)
    async with async_playwright() as playwright:
        await run(playwright)



if __name__ == "__main__":
    asyncio.run(main())