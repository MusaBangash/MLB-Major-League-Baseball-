import time
import threading
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
import traceback
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, ElementClickInterceptedException
import pandas as pd
from datetime import datetime
import csv
import os
import logging
from threading import Lock
import queue
from typing import Dict, List, Tuple, Optional, Any, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='mlb_scraper.log',
    filemode='a'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)
logger = logging.getLogger(__name__)

# CSV write lock to prevent multiple threads from writing simultaneously
csv_lock = Lock()

STAT_OPTIONS = {
    "1": {"name": "Home Runs", "xpath": "//label[contains(@class, 'checkbox__label') and contains(., 'Home Runs')]"},  
    "2": {"name": "Hits", "xpath": "//label[contains(@class, 'checkbox__label') and contains(., 'Hits')]"},
    "3": {"name": "Runs", "xpath": "//label[contains(@class, 'checkbox__label') and contains(., 'Runs')]"},
    "4": {"name": "RBI", "xpath": "//label[contains(@class, 'checkbox__label') and contains(., 'RBI')]"},
    "5": {"name": "Strikeouts", "xpath": "//label[contains(@class, 'checkbox__label') and contains(., 'Strikeouts')]"},
    "6": {"name": "Doubles", "xpath": "//label[contains(@class, 'checkbox__label') and contains(., 'Doubles')]"},
    "7": {"name": "Total Bases", "xpath": "//label[contains(@class, 'checkbox__label') and contains(., 'Total Bases')]"},
    "8": {"name": "Singles", "xpath": "//label[contains(@class, 'checkbox__label') and contains(., 'Singles')]"},
    "9": {"name": "Steals", "xpath": "//label[contains(@class, 'checkbox__label') and contains(., 'Steals')]"},
    "10": {"name": "Earned Runs", "xpath": "//label[contains(@class, 'checkbox__label') and contains(., 'Earned Runs')]"},
}

def handle_popups(driver: webdriver.Chrome) -> None:
    """
    Handle various popups that might appear on the site.
    
    Attempts to identify and close different types of popups including:
    - Close/dismiss buttons
    - Cookie consent popups
    - Modal overlays
    
    Args:
        driver: The Selenium WebDriver instance
    """
    try:
        # First popup type - usually appears on initial page load
        popup_close_buttons = driver.find_elements(By.XPATH, "//button[contains(@class, 'close') or contains(@class, 'dismiss') or contains(@aria-label, 'Close')]")
        for button in popup_close_buttons:
            if button.is_displayed():
                try:
                    button.click()
                    logger.info("Closed first type of popup")
                    time.sleep(0.5)
                except:
                    pass

        # Second popup type - cookie consent
        cookie_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Agree') or contains(text(), 'I understand')]")
        for button in cookie_buttons:
            if button.is_displayed():
                try:
                    button.click()
                    logger.info("Closed cookie consent popup")
                    time.sleep(0.5)
                except:
                    pass
                    
        # General approach for other types of popups
        try:
            overlay_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'overlay') or contains(@class, 'modal') or contains(@class, 'popup')]")
            for overlay in overlay_elements:
                if overlay.is_displayed():
                    close_buttons = overlay.find_elements(By.XPATH, ".//button[contains(@class, 'close') or contains(@aria-label, 'Close')]")
                    for button in close_buttons:
                        if button.is_displayed():
                            button.click()
                            logger.info("Closed general overlay")
                            time.sleep(0.5)
        except:
            pass
            
    except Exception as e:
        logger.warning(f"Error handling popups: {e}")

def scrape_player_stats(driver: webdriver.Chrome, player_url: str, num_games: int, home_or_away: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Scrape the last X games' stats for the player and corresponding HOME/AWAY games.
    
    Opens the player's statistics page in a new tab and extracts performance data
    from their recent games, calculating averages for all games and specifically 
    for home or away games.
    
    Args:
        driver: The Selenium WebDriver instance
        player_url: URL to the player's stats page
        num_games: Number of recent games to analyze
        home_or_away: Filter for "HOME" or "AWAY" games
        
    Returns:
        Tuple containing (average for all games, average for filtered home/away games)
        Returns (None, None) if data cannot be extracted
    """
    # Open player profile in a new tab
    driver.execute_script(f"window.open('{player_url}', '_blank');")
    driver.switch_to.window(driver.window_handles[-1])  # Switch to the new tab

    try:
        # Handle any popups that might appear on the player page
        handle_popups(driver)
        
        # Wait until the table is present on the page with retry mechanism
        max_retries = 3
        for attempt in range(max_retries):
            try:
                player_stats_table = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div/div/div[1]/div/main/div/div/div[2]/section[4]/div/div[2]/table/tbody"))
                )
                break
            except TimeoutException:
                if attempt < max_retries - 1:
                    logger.warning(f"Retry {attempt+1}/{max_retries} loading player stats table")
                    driver.refresh()
                    time.sleep(2)
                    handle_popups(driver)
                else:
                    logger.error("Failed to load player stats table after retries")
                    return None, None
        
        # Wait until rows are present in the table
        rows = WebDriverWait(player_stats_table, 15).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, 'tr'))
        )

        all_stats: List[int] = []  # For the last 'num_games' games
        filtered_stats: List[int] = []  # For the last 'num_games' of the specified type (home/away)

        # Iterate through rows to collect stats
        for row in rows:
            columns = row.find_elements(By.TAG_NAME, 'td')
            if len(columns) > 1:
                # Extract matchup column (2nd column)
                matchup = columns[1].text.strip()
                is_away = "@" in matchup

                # Determine if the game type matches the required home/away
                matches_filter = (home_or_away == "AWAY" and is_away) or (home_or_away == "HOME" and not is_away)

                # Extract stat value from the 7th column (index 6)
                stat_value = columns[5].text.strip()  # Column 7 (index 6)
                if "O" in stat_value or "U" in stat_value:
                    # Split the string by space and take the second part (the number)
                    stat_value = stat_value.split()[1]
                
                # Convert the stat value to an integer (if possible)
                try:
                    stat_value_int = int(stat_value)
                except ValueError:
                    stat_value_int = 0  # Default value for invalid or missing data
                
                # Add to the general stats list
                if len(all_stats) < num_games:
                    all_stats.append(stat_value_int)
                
                # Add to the filtered stats list if it matches the home/away type
                if matches_filter and len(filtered_stats) < num_games:
                    filtered_stats.append(stat_value_int)

            # Break early if we've gathered enough stats
            if len(all_stats) >= num_games and len(filtered_stats) >= num_games:
                break

        # Calculate averages
        avg_all = round(float(sum(all_stats) / len(all_stats)), 1) if all_stats else 0.0
        avg_filtered = round(float(sum(filtered_stats) / len(filtered_stats)), 1) if filtered_stats else 0.0

        return avg_all, avg_filtered  # Return both averages
    except Exception as e:
        logger.error(f"Error while scraping player stats from player page: {e}")
        return None, None
    finally:
        driver.close()  # Close the current tab
        driver.switch_to.window(driver.window_handles[0])  # Switch back to the main tab

def process_player_card(driver: webdriver.Chrome, card: webdriver.remote.webelement.WebElement, 
                      num_games: int, stat_category: str, writer_queue: queue.Queue, i: int) -> None:
    """
    Process a single player card and extract all required data.
    
    Extracts various statistics and information from a player card element
    including player name, number, odds, projection, and team info. Then visits
    the player's individual page to get historical performance data.
    
    Args:
        driver: The Selenium WebDriver instance
        card: The WebElement representing the player card
        num_games: Number of recent games to analyze
        stat_category: The type of statistic being collected
        writer_queue: Queue for sending data to the CSV writer thread
        i: The index/position of the card (for logging purposes)
    """
    try:
        logger.info(f"Processing card {i}...")

        # Extract player URL
        href = card.get_attribute("href")
        if not href:
            logger.warning(f"Card {i}: Missing href attribute!")
            return
        player_name = href.split("/")[5] if href else "N/A"
        logger.info(f"Card {i}: Player Name - {player_name}")

        # Extract number
        try:
            number_element = WebDriverWait(card, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.flex.player-prop-card__prop-container span.typography[style*='--48359156: left'][style*='--2a6287d2: #16191D']"))
            )
            number = number_element.text.strip() if number_element else "N/A"
        except Exception as e:
            number = "N/A"
            logger.warning(f"Card {i}: Error extracting number element: {e}")

        # Extract odds
        try:
            odds_element = WebDriverWait(card, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.typography:not(.player-prop-card__team-pos)[style*='--2a6287d2: #525A67']"))
            )
            odds = odds_element.text.strip() if odds_element else "N/A"
        except Exception as e:
            odds = "N/A"
            logger.warning(f"Card {i}: Error extracting odds element: {e}")

        # Extract projection
        try:
            projection_element = WebDriverWait(card, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span[style*='--2a6287d2: #1F845A'], span[style*='--2a6287d2: #C9372C']"))
            )
            projection = projection_element.text.strip() if projection_element else "N/A"
        except Exception as e:
            projection = "N/A"
            logger.warning(f"Card {i}: Error extracting projection element: {e}")

        # Extract team info
        try:
            team_info_element = WebDriverWait(card, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.typography.player-prop-card__team-pos"))
            )
            team_info = team_info_element.text.strip() if team_info_element else "N/A"
        except Exception as e:
            team_info = "N/A"
            logger.warning(f"Card {i}: Error extracting team info element: {e}")

        clean_team_info = team_info.split("-", 1)[-1].strip()

        if "vs" in clean_team_info:
            home_or_away = "HOME"
            first_team = clean_team_info.split("vs")[0].strip()  # Extract first team before "vs"
        elif "@" in clean_team_info:
            home_or_away = "AWAY"
            first_team = clean_team_info.split("@")[0].strip()  # Extract first team before "@"
        else:
            home_or_away = "N/A"
            first_team = "N/A"

        # Scrape the player's stats
        logger.info(f"Scraping stats for {player_name}...")
        try:
            avg_all, avg_filtered = scrape_player_stats(driver, href, num_games, home_or_away)
        except Exception as e:
            avg_all, avg_filtered = "N/A", "N/A"
            logger.error(f"Card {i}: Error scraping stats for {player_name}: {e}")
            logger.debug(traceback.format_exc())

        current_date = datetime.now().strftime("%m-%d")
        
        # Queue the data to be written to CSV
        writer_queue.put({
            'Player Name': player_name,
            'Number': number,
            'Odds': odds,
            'Projection': projection,
            'Avg': avg_all,
            'Home/Away Avg': avg_filtered,
            'Home/Away': home_or_away,
            'Date': f'"{current_date}"',  
            'Stat Category': stat_category, 
            'Team': first_team  
        })

        logger.info(f"Successfully processed card {i} for player: {player_name}")
        logger.info("-" * 40)

    except Exception as e:
        logger.error(f"Error processing card {i}: {e}")
        logger.debug(traceback.format_exc())

def csv_writer_thread(writer_queue: queue.Queue, stat_filename: str, fieldnames: List[str]) -> None:
    """
    Thread function to handle writing data to the CSV file.
    
    Runs as a separate thread that continuously pulls data from the queue
    and writes it to the CSV file. Continues until receiving a "DONE" signal.
    Uses a lock to ensure thread-safe file access.
    
    Args:
        writer_queue: Queue containing data dictionaries to be written
        stat_filename: Name of the CSV file to write to
        fieldnames: List of column headers for the CSV
    """
    while True:
        data = writer_queue.get()
        if data == "DONE":
            writer_queue.task_done()
            break
        
        try:
            with csv_lock:
                file_exists = os.path.exists(stat_filename)
                with open(stat_filename, 'a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(data)
            writer_queue.task_done()
        except Exception as e:
            logger.error(f"Error writing to CSV: {e}")
            writer_queue.task_done()

def scrape_page_data(driver: webdriver.Chrome, num_games: int, stat_category: str, writer_queue: queue.Queue) -> None:
    """
    Scrape the current page's data and queue it for saving to CSV.
    
    Extracts all player cards from the current page, processes each one to extract
    their statistics, and sends the data to the CSV writer queue.
    
    Args:
        driver: The Selenium WebDriver instance
        num_games: Number of recent games to analyze
        stat_category: The type of statistic being collected
        writer_queue: Queue for sending data to the CSV writer thread
    """
    try:
        # Handle any popups before proceeding
        handle_popups(driver)
        
        # Wait until the player prop cards container is present
        logger.info("Waiting for player prop cards to load...")
        
        # More robust approach to find player cards with retries
        max_retries = 3
        for attempt in range(max_retries):
            try:
                target_element = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div/div[1]/div/main/div/div[2]/section/div[2]'))
                )
                break
            except TimeoutException:
                if attempt < max_retries - 1:
                    logger.warning(f"Retry {attempt+1}/{max_retries} loading player prop cards container")
                    driver.refresh()
                    time.sleep(2)
                    handle_popups(driver)
                else:
                    logger.error("Failed to load player prop cards container after retries")
                    return

        # Extract all player prop cards with retry mechanism
        for attempt in range(max_retries):
            try:
                player_prop_cards = WebDriverWait(target_element, 15).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "player-prop-cards-container__card"))
                )
                break
            except (TimeoutException, StaleElementReferenceException):
                if attempt < max_retries - 1:
                    logger.warning(f"Retry {attempt+1}/{max_retries} finding player prop cards")
                    driver.refresh()
                    time.sleep(2)
                    handle_popups(driver)
                    target_element = WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div/div[1]/div/main/div/div[2]/section/div[2]'))
                    )
                else:
                    logger.error("Failed to find player prop cards after retries")
                    return
        
        logger.info(f"Found {len(player_prop_cards)} player cards on this page.")

        # Process each player card sequentially
        for i, card in enumerate(player_prop_cards, start=1):
            process_player_card(driver, card, num_games, stat_category, writer_queue, i)

    except Exception as e:
        logger.error(f"Error in scraping page data: {e}")
        logger.debug(traceback.format_exc())

def setup_webdriver() -> webdriver.Chrome:
    """
    Set up and return a configured WebDriver instance.
    
    Configures a Chrome WebDriver with appropriate options for web scraping,
    including disabling notifications, popups, and other browser features
    that might interfere with automated scraping.
    
    Returns:
        A configured Chrome WebDriver instance
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")  # Disable browser notifications
    options.add_argument("--disable-popup-blocking")  # Allow popups but we'll handle them
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    
    # Headless option can be enabled for faster scraping without UI
    # options.add_argument("--headless")
    
    # Create and return the driver
    return webdriver.Chrome(options=options)

def scrape_stat_category(stat_option: str, num_games: int) -> None:
    """
    Scrape a single stat category with its own WebDriver instance.
    
    Creates a dedicated WebDriver instance to scrape a specific stat category.
    Handles navigation through pages, clicking on the appropriate stat filter,
    and processes all player cards across multiple pages.
    
    Args:
        stat_option: The key of the stat option in STAT_OPTIONS dictionary
        num_games: Number of recent games to analyze
    """
    stat_name = STAT_OPTIONS[stat_option]["name"]
    stat_xpath = STAT_OPTIONS[stat_option]["xpath"]
    stat_filename = f"{stat_name.lower().replace(' ', '_')}_player_props.csv"
    
    logger.info(f"Starting scrape for {stat_name}...")
    
    driver = setup_webdriver()
    writer_queue: queue.Queue = queue.Queue()
    
    try:
        # Set up CSV writer thread
        fieldnames = ['Player Name', 'Number', 'Odds', 'Projection', 'Avg', 'Home/Away Avg', 'Home/Away', 'Date', 'Stat Category', 'Team']
        csv_thread = threading.Thread(target=csv_writer_thread, args=(writer_queue, stat_filename, fieldnames))
        csv_thread.start()
        
        # Navigate to the website
        url = "https://www.bettingpros.com/mlb/props/"
        driver.get(url)
        
        # Wait for the page to load and handle popups
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        handle_popups(driver)
        
        # Click the button corresponding to the current stat with retry mechanism
        max_retries = 3
        for attempt in range(max_retries):
            try:
                button = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.XPATH, stat_xpath))
                )
                # Try to click using different methods
                try:
                    button.click()
                except ElementClickInterceptedException:
                    # If click is intercepted, try JavaScript click
                    driver.execute_script("arguments[0].click();", button)
                
                logger.info(f"Clicked on {stat_name}")
                time.sleep(3)  # Allow time for the page to update
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Retry {attempt+1}/{max_retries} clicking on {stat_name}: {e}")
                    driver.refresh()
                    time.sleep(2)
                    handle_popups(driver)
                else:
                    logger.error(f"Failed to click on {stat_name} after retries: {e}")
                    writer_queue.put("DONE")
                    csv_thread.join()
                    return
        
        # Track pages to ensure we don't miss any
        page_number = 1
        pages_with_no_content = 0
        max_empty_pages = 3  # Stop after this many consecutive empty pages
        
        # Scrape the first page
        logger.info(f"Scraping page {page_number} for {stat_name}")
        scrape_page_data(driver, num_games, stat_name, writer_queue)
        
        # Attempt to click "Next Page" button repeatedly
        next_button_xpath = '/html/body/div[1]/div/div/div[1]/div/main/div/div[2]/section/div[3]/button[2]'
        
        while True:
            try:
                # Ensure we're at the bottom of the page
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                
                # Handle any popups that might have appeared
                handle_popups(driver)
                
                # Check if next button exists and is clickable
                try:
                    next_button = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, next_button_xpath))
                    )
                    
                    # Check if button is disabled
                    if "disabled" in next_button.get_attribute("outerHTML"):
                        logger.info("Next page button is disabled, no more pages to scrape.")
                        break
                    
                    # Try clicking with different methods
                    try:
                        next_button.click()
                    except ElementClickInterceptedException:
                        # If click is intercepted, use JavaScript
                        driver.execute_script("arguments[0].click();", next_button)
                    
                    page_number += 1
                    logger.info(f"Clicked 'Next Page' button. Now on page {page_number}")
                    
                    # Give time for the page to load
                    time.sleep(3)
                    
                    # Handle popups after page load
                    handle_popups(driver)
                    
                    # Check if page has content
                    cards = driver.find_elements(By.CLASS_NAME, "player-prop-cards-container__card")
                    if len(cards) == 0:
                        pages_with_no_content += 1
                        logger.warning(f"Page {page_number} has no player cards. Empty pages count: {pages_with_no_content}")
                        if pages_with_no_content >= max_empty_pages:
                            logger.info(f"Stopping after {max_empty_pages} consecutive empty pages")
                            break
                    else:
                        pages_with_no_content = 0  # Reset counter if page has content
                        scrape_page_data(driver, num_games, stat_name, writer_queue)
                    
                except TimeoutException:
                    logger.info("Next page button not found or not clickable. Assuming end of pages.")
                    break
                
            except Exception as e:
                logger.error(f"Error while handling pagination: {e}")
                logger.debug(traceback.format_exc())
                break
        
        # Signal to the CSV writer thread that we're done
        writer_queue.put("DONE")
        csv_thread.join()
        
        logger.info(f"Completed scraping for {stat_name}")
        
    except Exception as e:
        logger.error(f"An error occurred during scraping of {stat_name}: {e}")
        logger.debug(traceback.format_exc())
        # Signal to the CSV writer thread that we're done even in case of error
        writer_queue.put("DONE")
        csv_thread.join()
    finally:
        # Quit the driver
        driver.quit()

def scrape_selected_stats() -> None:
    """
    Prompt user to select stats and start the scraping process.
    
    Main function that handles the user interface, prompts for input,
    and coordinates the scraping process across multiple stat categories.
    Uses parallel processing with thread pool to improve performance.
    """
    # Prompt user to select one or more stats
    print("Select stats to scrape (enter numbers separated by commas):")
    for key, value in STAT_OPTIONS.items():
        print(f"{key}: {value['name']}")
    
    user_input = input("Your choice: ").split(",")
    selected_options = [option.strip() for option in user_input if option.strip() in STAT_OPTIONS]

    if not selected_options:
        logger.error("No valid options selected. Exiting.")
        return

    logger.info(f"Selected options: {[STAT_OPTIONS[opt]['name'] for opt in selected_options]}")

    # Prompt user for the number of games to scrape
    try:
        num_games = int(input("Enter the number of recent games to scrape (e.g., 5): "))
        if num_games <= 0:
            logger.error("Please enter a positive number of games.")
            return
    except ValueError:
        logger.error("Invalid input. Please enter a valid integer for the number of games.")
        return
    
    # Prompt user for parallel scraping
    try:
        max_workers = int(input("Enter number of parallel scrapers (1-4, higher uses more CPU): ") or "2")
        max_workers = max(1, min(4, max_workers))  # Limit between 1 and 4
    except ValueError:
        max_workers = 2  # Default to 2 workers
    
    logger.info(f"Using {max_workers} parallel scrapers")
    
    # Use ThreadPoolExecutor for parallel scraping of different stat categories
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all stat categories for scraping
        future_to_stat = {
            executor.submit(scrape_stat_category, option, num_games): option
            for option in selected_options
        }
        
        # Wait for all to complete and handle any exceptions
        for future in concurrent.futures.as_completed(future_to_stat):
            stat_option = future_to_stat[future]
            stat_name = STAT_OPTIONS[stat_option]["name"]
            try:
                future.result()
                logger.info(f"Completed processing for {stat_name}")
            except Exception as exc:
                logger.error(f"{stat_name} generated an exception: {exc}")
    
    logger.info("All scraping tasks completed!")

if __name__ == "__main__":
    scrape_selected_stats()

