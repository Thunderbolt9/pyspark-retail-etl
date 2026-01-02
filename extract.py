import kaggle
import logging
import os

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    dataset_slug = "carrie1/ecommerce-data"
    download_path = "./data"

    logger = logging.getLogger(__name__)

    try:
        logger.info("Perform Full Load From Kaggle")
        # Ensure directory exists
        if not os.path.exists(download_path):
            os.makedirs(download_path)
            
        kaggle.api.dataset_download_files(dataset_slug, download_path, unzip=True)
        logger.info("Download Successful!")
        
    except Exception as e:
        logger.error(f"Full Load Failed Due To: {e}")