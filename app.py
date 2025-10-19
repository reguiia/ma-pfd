
import streamlit as st
import logging, asyncio, os, pandas as pd, time
from dataclasses import dataclass, asdict
from typing import List, Optional
from playwright.async_api import async_playwright, Page
from playwright.sync_api import sync_playwright
import nest_asyncio
from IPython.display import clear_output
import tempfile
import json

# Apply nest_asyncio for compatibility
nest_asyncio.apply()

# Data Model
@dataclass
class Place:
    name: str = ""
    address: str = ""
    website: str = ""
    phone_number: str = ""
    reviews_count: Optional[int] = None
    reviews_average: Optional[float] = None
    store_shopping: str = "No"
    in_store_pickup: str = "No"
    store_delivery: str = "No"
    place_type: str = ""
    opens_at: str = ""
    introduction: str = ""
    url: str = ""

# Setup logging
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Helper functions
async def extract_text(page: Page, selector: str) -> str:
    try:
        element = page.locator(selector)
        if await element.count() > 0:
            return (await element.first.inner_text()).strip()
    except:
        return ""
    return ""

async def extract_attribute(page: Page, selector: str, attribute: str) -> str:
    try:
        element = page.locator(selector)
        if await element.count() > 0:
            return await element.first.get_attribute(attribute)
    except:
        return ""
    return ""

async def extract_place(page: Page) -> Place:
    """Extract all relevant fields for a given place"""
    p = Place()
    
    # Wait for page to load
    await page.wait_for_selector('h1', timeout=10000)
    
    # Extract data using multiple selectors to increase success rate
    p.name = await extract_text(page, 'h1')
    if not p.name:
        p.name = await extract_text(page, '//h1[contains(@class, "DUwDvf")]')
    
    p.address = await extract_text(page, '//button[@data-item-id="address"]//div')
    if not p.address:
        p.address = await extract_text(page, 'button[data-item-id="address"]')
    
    p.website = await extract_text(page, '//a[@data-item-id="authority"]//div')
    if not p.website:
        p.website = await extract_attribute(page, '//a[@data-item-id="authority"]', 'href')
    
    p.phone_number = await extract_text(page, '//button[contains(@data-item-id, "phone")]//div')
    if not p.phone_number:
        p.phone_number = await extract_text(page, 'button[data-item-id*="phone"]')
    
    p.place_type = await extract_text(page, '//button[contains(@class, "DkEaL")]')
    if not p.place_type:
        p.place_type = await extract_text(page, '//button[contains(@class, "fontBodyMedium")]')
    
    p.opens_at = await extract_text(page, '//button[contains(@data-item-id, "oh")]//div')
    
    # Reviews
    reviews_text = await extract_text(page, '//span[contains(@aria-label, "review")]')
    if reviews_text:
        try:
            import re
            numbers = re.findall(r'\d+', reviews_text)
            if len(numbers) > 0:
                p.reviews_count = int(numbers[-1])
        except:
            pass
    
    rating_text = await extract_text(page, '//div[@jsaction="pane.rating.more"]//span[@aria-hidden="true"]')
    if rating_text:
        try:
            p.reviews_average = float(rating_text.replace(',', '.'))
        except:
            pass
    
    p.url = page.url
    return p

# Function to search for a single query
async def search_single_query(page: Page, query: str, max_results: int) -> List[str]:
    """Search for a single query and return place URLs"""
    await page.goto("https://www.google.com/maps", timeout=60000)
    await page.wait_for_timeout(2000)
    
    # Fill search box and press Enter
    await page.fill('input#searchboxinput', query)
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(4000)
    
    # Wait for results to load
    await page.wait_for_selector('a[href*="/maps/place/"]', timeout=10000)
    
    # Scroll to load more results
    last_count = 0
    scroll_attempts = 0
    max_scrolls = 10
    
    while scroll_attempts < max_scrolls and scroll_attempts < 5:  # Reduced attempts
        await page.mouse.wheel(0, 3000)  # Scroll less each time
        await page.wait_for_timeout(1500)
        
        current_count = await page.locator('a[href*="/maps/place/"]').count()
        
        if current_count >= max_results:
            break
        elif current_count == last_count:
            scroll_attempts += 1
        else:
            scroll_attempts = 0
        
        last_count = current_count
        if current_count > 0:
            st.text(f"Found {current_count} results for '{query}'...")
    
    # Get unique URLs
    links = await page.locator('a[href*="/maps/place/"]').all()
    urls = []
    seen_urls = set()
    
    for link in links:
        href = await link.get_attribute('href')
        if href and href not in seen_urls and href not in urls:
            seen_urls.add(href)
            urls.append(href)
    
    return urls[:max_results]

# Main scraper function with multiple queries
async def scrape_places(search_for: str, total: int, progress_bar) -> List[Place]:
    setup_logging()
    results: List[Place] = []
    
    # Generate related search terms to get more results
    search_terms = [
        search_for,
        f"{search_for} center",
        f"{search_for} near me",
        f"{search_for} best",
        f"{search_for} popular",
        f"{search_for} 24 hours",
        f"{search_for} chain",
        f"{search_for} local"
    ]
    
    all_urls = set()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-extensions',
                '--disable-plugins',
            ]
        )
        
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        
        page = await context.new_page()
        
        # Search multiple terms to get more results
        for term in search_terms:
            st.text(f"Searching: {term}")
            urls = await search_single_query(page, term, min(30, total))  # Limit per search
            all_urls.update(urls)
            
            if len(all_urls) >= total:
                break
            
            await page.wait_for_timeout(2000)  # Wait between searches
        
        st.text(f"Collected {len(all_urls)} unique place URLs")
        
        # Process each unique URL
        urls_list = list(all_urls)[:total]
        progress_bar.progress(0)
        
        for i, url in enumerate(urls_list):
            try:
                await page.goto(url, timeout=15000)
                await page.wait_for_timeout(1500)
                
                place = await extract_place(page)
                
                if place.name and place.name not in [p.name for p in results]:  # Avoid duplicates by name
                    results.append(place)
                    st.text(f"Added: {place.name}")
                
                progress_bar.progress((i + 1) / len(urls_list))
                
                await page.wait_for_timeout(1000)  # Be respectful
                
            except Exception as e:
                logging.warning(f"Error processing {url}: {e}")
                continue
        
        await browser.close()
    
    return results

# Save results to Excel
def save_results(places: List[Place], path="results.xlsx"):
    df = pd.DataFrame([asdict(p) for p in places])
    # Remove columns that have the same value for all rows
    df = df.loc[:, df.nunique() > 1]
    df.to_excel(path, index=False)
    return df

# Streamlit UI
def main():
    st.set_page_config(page_title="Google Maps Scraper", layout="wide")
    st.title("🗺️ Google Maps Scraper")
    st.markdown("Scrape business information from Google Maps")
    
    # Input fields
    search_query = st.text_input("Search Query", "coffee shops in Paris")
    num_results = st.slider("Number of Results", min_value=10, max_value=100, value=30, step=10)
    
    # Progress bar
    progress_bar = st.progress(0)
    
    # Start scraping button
    if st.button("Start Scraping"):
        if not search_query:
            st.error("Please enter a search query")
            return
        
        with st.spinner("Starting browser and searching... This may take several minutes..."):
            try:
                # Run the async scraper
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                results = loop.run_until_complete(
                    scrape_places(search_query, num_results, progress_bar)
                )
                
                if not results:
                    st.warning("⚠️ No results found.")
                    return

                df = save_results(results)
                
                # Display results
                st.success(f"✅ Success! Scraped {len(df)} unique places")
                st.dataframe(df)
                
                # Create download button
                with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                    df.to_excel(tmp.name, index=False)
                    tmp_path = tmp.name
                
                with open(tmp_path, "rb") as f:
                    st.download_button(
                        label="Download Excel",
                        data=f,
                        file_name="google_maps_results.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
            except Exception as e:
                st.error(f"Error occurred: {e}")
                st.error("This might be due to Google Maps anti-bot measures. Try again later or with a different search term.")

if __name__ == "__main__":
    main()
