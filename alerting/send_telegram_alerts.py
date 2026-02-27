import os
import requests
import logging
import time
from include.eczachly.snowflake_queries import execute_snowflake_query

# Configure Logger
logger = logging.getLogger("TelegramAlerts")
logging.basicConfig(level=logging.INFO)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message: str):
    """Sends a text message to the configured Telegram Chat."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram Alerting Skipped: Missing Token or Chat ID.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info("Alert sent successfully!")
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")
        raise e

def check_and_alert(execution_date: str):
    """Checks Gold Layer for anomalies and alerts if found."""
    logger.info(f"Checking for anomalies on {execution_date}...")
    
    query = f"""
    SELECT 
        vessel_name, 
        mmsi, 
        max_speed_kts, 
        speed_anomaly, 
        has_blackouts_greater_than_2hrs,
        is_sanctioned
    FROM DATAEXPERT_STUDENT.NICOLASSTEEL.FCT_DAILY_VESSEL_ACTIVITY
    WHERE activity_date = '{execution_date}'
      AND (speed_anomaly = TRUE OR has_blackouts_greater_than_2hrs = TRUE OR is_sanctioned = TRUE)
    """
    
    try:
        results = execute_snowflake_query(query)
        
        if not results:
            logger.info("No anomalies detected today. Sleep well.")
            return
            
        logger.info(f"Found {len(results)} anomalies. Sending Alert...")

        # Format Message
        # Batching: Group by 10 vessels to avoid 429 Too Many Requests
        BATCH_SIZE = 10
        
        for i in range(0, len(results), BATCH_SIZE):
            batch = results[i:i + BATCH_SIZE]
            msg = f"🚨 <b>TANKER ALERT REPORT ({i+1}-{min(i+BATCH_SIZE, len(results))}/{len(results)})</b> 🚨\nDate: {execution_date}\n\n"
            
            for row in batch:
                name, mmsi, speed, is_speed, is_blackout, is_sanctioned = row
                
                flags = []
                if is_speed: flags.append(f"Speed {speed}kts")
                if is_blackout: flags.append("Blackout > 2h")
                if is_sanctioned: flags.append("SANCTIONED VESSEL")
                
                # Escape HTML characters in name just in case
                safe_name = str(name).replace("<", "&lt;").replace(">", "&gt;").replace("&", "&amp;")
                
                msg += f"🚢 <b>{safe_name}</b> ({mmsi})\n   ⚠️ {' + '.join(flags)}\n\n"
            
            # Send the batch
            send_telegram_message(msg)
            time.sleep(3) # Wait 3s to respect Telegram limit (max 20 msgs/min)
        
    except Exception as e:
        logger.error(f"Error querying Snowflake for alerts: {e}")
        raise e

if __name__ == "__main__":
    # Test Manual Run via CLI
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2024-09-10"
    check_and_alert(date)
