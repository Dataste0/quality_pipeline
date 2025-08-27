import requests
import pipeline_lib.config as cfg

# --- Logger
import logging
logger = logging.getLogger(__name__)


PBI_URL = cfg.PBI_REFRESH_WEBHOOK
PAYLOAD = {"note": "refresh after pipeline"}

def powerbi_refresh():
    logger.info(f"Power BI Refresh Phase Started")
    print("[INFO] Power BI Refresh Phase Started")
    
    try:
        r = requests.post(PBI_URL, json=PAYLOAD, timeout=30)
        r.raise_for_status()
        logger.info("Trigger sent to Power Automate (202 Accepted).")
        #print("Trigger sent to Power Automate (202 Accepted).")
    except requests.HTTPError as e:
        logger.warning(f"HTTP Error: {e.response.status_code} - {e.response.text[:300]}")
        #print(f"HTTP Error: {e.response.status_code} - {e.response.text[:300]}")
        return False
    except requests.RequestException as e:
        #print(f"Network Error: {e}")
        logger.warning(f"Network Error: {e}")
        return False

    logger.info(f"Power BI Refresh Phase Ended")
    print("[INFO] Power BI Refresh Phase Ended")
    return True
