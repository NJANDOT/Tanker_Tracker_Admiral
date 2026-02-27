import os
import logging
import requests
from pathlib import Path

import zipfile
import shutil

def setup_logger(name: str, log_file: str = None) -> logging.Logger:
    """Configures and returns a logger."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console Handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    # File Handler
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        
    return logger

def download_file(url: str, dest_path: Path, logger: logging.Logger) -> bool:
    """Downloads a file from a URL to a destination path."""
    try:
        logger.info(f"Starting download from {url} to {dest_path}")
        
        # Ensure directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        response = requests.get(url, stream=True)
        logger.info(f"Response Status: {response.status_code}")
        logger.info(f"Content-Type: {response.headers.get('Content-Type')}")
        response.raise_for_status()
        
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        logger.info("Download completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to download file: {e}")
        return False

def download_and_extract_zip(url: str, extract_to: Path, logger: logging.Logger) -> bool:
    """Downloads a zip file and extracts its contents."""
    zip_path = extract_to / "temp_download.zip"
    
    if download_file(url, zip_path, logger):
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            logger.info(f"Extracted zip contents to {extract_to}")
            
            # Cleanup zip file
            os.remove(zip_path)
            return True
        except zipfile.BadZipFile:
            logger.error("Downloaded file is not a valid zip file.")
            # Debug: Read first 500 bytes to see what it is (likely HTML)
            try:
                with open(zip_path, 'r', errors='ignore') as f:
                    head = f.read(500)
                    logger.error(f"File Header Preview:\n{head}")
            except Exception as read_err:
                logger.error(f"Could not read file preview: {read_err}")

            if os.path.exists(zip_path):
                os.remove(zip_path)
            return False
            if os.path.exists(zip_path):
                os.remove(zip_path)
            return False
        except Exception as e:
            logger.error(f"Failed to extract zip file: {e}")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            return False
    return False
