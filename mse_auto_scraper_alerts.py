"""
mse_auto_scraper_alerts.py
Automated Malawi Stock Exchange scraper with daily scheduling, logging, and email alerts.
"""

import time
import schedule
import pandas as pd
import sqlite3
import smtplib
from email.mime.text import MIMEText
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import logging
import os
from io import StringIO
from dotenv import load_dotenv

# =============================
# CONFIGURATION
# =============================

RUN_TIME = "16:00"  # Time to run each day (24-hour format)
DB_FILE = "mse_data.db"
EXCEL_FILE = "mse_daily.xlsx"
TABLE_NAME = "daily_prices"
LOG_FILE = "mse_scraper.log"

load_dotenv()  # Load environment variables from .env file

# Load email config from environment variables (set via .env or GitHub Secrets)
EMAIL_USER = os.getenv("EMAIL_USER", "your_email@gmail.com")
EMAIL_PASS = os.getenv("EMAIL_PASS", "your_app_password")
EMAIL_TO = os.getenv("EMAIL_TO", "your_email@gmail.com")

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# =============================
# LOGGING SETUP
# =============================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

# =============================
# SCRAPER FUNCTIONS
# =============================

def get_driver():
    """Initialize a headless Chrome browser session for Selenium."""
    options = Options()
    options.headless = True
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)

def fetch_table():
    """Visit the MSE mainboard page, extract the stock table, and return it as a DataFrame."""
    url = "https://mse.co.mw/market/mainboard"
    logger.info("Fetching MSE data...")
    driver = get_driver()
    driver.get(url)
    time.sleep(5)  # Wait for page to fully render

    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")

    if not table:
        raise ValueError("No table found on the page. The site may have changed.")

    df = pd.read_html(StringIO(str(table)))[0]
    df.columns = [c.strip() for c in df.columns]
    df["scrape_date"] = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Fetched {len(df)} rows from MSE.")
    return df

def save_to_sqlite(df):
    """Save the scraped data into a local SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    df.to_sql(TABLE_NAME, conn, if_exists="append", index=False)

    # Remove duplicates (same Ticker + scrape_date)
    if "Ticker" in df.columns:
        conn.execute(f"""
            DELETE FROM {TABLE_NAME}
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM {TABLE_NAME}
                GROUP BY "Ticker", "scrape_date"
            );
        """)
    conn.commit()
    conn.close()
    logger.info(f"Saved {len(df)} records to SQLite database '{DB_FILE}'.")

def save_to_excel(df):
    """Save the scraped data into an Excel file."""
    try:
        old_df = pd.read_excel(EXCEL_FILE)
        combined = pd.concat([old_df, df], ignore_index=True)
        if "Ticker" in combined.columns:
            combined = combined.drop_duplicates(subset=["Ticker", "scrape_date"])
    except FileNotFoundError:
        combined = df

    combined.to_excel(EXCEL_FILE, index=False)
    logger.info(f"Saved data to Excel file '{EXCEL_FILE}'.")

# =============================
# EMAIL ALERTS
# =============================

def send_email(subject, body):
    """Send an email alert for success or failure."""
    msg = MIMEText(body)
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        logger.info(f"Email sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")

# =============================
# MAIN JOB FUNCTION
# =============================

def daily_job():
    """Main scheduled job: scrape, save, log, and send alerts."""
    logger.info("----- Daily scrape started -----")
    try:
        df = fetch_table()
        save_to_sqlite(df)
        save_to_excel(df)
        success_message = f"✅ MSE daily update successful.\nRows fetched: {len(df)}\nTime: {datetime.now()}"
        send_email("✅ MSE Data Update Successful", success_message)
        logger.info("Daily scrape completed successfully.")
    except Exception as e:
        error_message = f"❌ MSE scraper failed: {e}"
        logger.error(error_message)
        send_email("❌ MSE Scraper Failed", error_message)

# =============================
# SCHEDULER LOOP
# =============================

def run_daily():
    """Run the job once per day and keep the scheduler alive."""
    logger.info(f"Scheduler started. Scraper will run daily at {RUN_TIME}.")
    schedule.every().day.at(RUN_TIME).do(daily_job)

    # Optional: run once immediately when script starts
    daily_job()

    while True:
        schedule.run_pending()
        time.sleep(60)

# =============================
# MAIN
# =============================

if __name__ == "__main__":
    run_daily()
