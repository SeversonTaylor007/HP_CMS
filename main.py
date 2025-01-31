import requests
import pandas as pd
import json
import sqlite3
from datetime import datetime
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from io import StringIO

class CMSDataProcessor:
    """
    Processes CMS hospital datasets by downloading and transforming them.
    
    # Key design choices:
    - Using SQLite for tracking - lightweight and no extra infrastructure needed
    - Processing files in chunks to handle large datasets
    - Parallel processing with memory constraints in mind
    """

    def __init__(self):
        # Using a local SQLite db for simplicity
        self.metadata_db = "data/metadata.db"
        self._init_db()

    def _init_db(self):
        # Set up SQLite tracking database
        # Note: Using TEXT for dates to avoid timezone headaches
        os.makedirs("data/processed", exist_ok=True)
        with sqlite3.connect(self.metadata_db) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    dataset_id TEXT PRIMARY KEY,
                    last_modified TEXT,
                    last_processed TEXT
                )
            """)

    def _to_snake_case(self, col_name):
        # Convert CMS's messy column names to clean snake_case
        # Examples we need to handle:
        # "Patient Rating Score" -> "patient_rating_score"
        # "Total ($) Cost" -> "total_cost"
        return ''.join(['_' + c.lower() if c.isupper() else c 
                      for c in col_name]).lstrip('_').replace(' ', '_')

    def _process_dataset(self, dataset):
        try:
            dataset_id = dataset['identifier']
            modified_date = dataset.get('modified', '')
            
            # Skip if we already have the latest version
            if not self._needs_update(dataset_id, modified_date):
                print(f"Skipping {dataset_id} - already up to date")
                return

            download_url = dataset['distribution'][0]['downloadURL']
            output_path = f"data/processed/{dataset_id}.csv"

            # Stream the download to handle large files
            # Had memory issues before adding streaming
            with requests.get(download_url, stream=True) as response:
                if response.status_code == 200:
                    # Use temp file to avoid corrupted data if process fails
                    temp_file = f"data/processed/temp_{dataset_id}.csv"
                    with open(temp_file, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    
                    # Process in chunks - file is 500MB+
                    # Using smaller chunks (5000) after memory testing
                    first_chunk = True
                    for chunk in pd.read_csv(
                        temp_file,
                        chunksize=5000,
                        low_memory=False,
                        dtype=str  # Prevent dtype inference issues, don't want to infer type
                    ):
                        if first_chunk:
                            # Only convert headers once for efficiency
                            chunk.columns = [self._to_snake_case(col) for col in chunk.columns]
                            chunk.to_csv(output_path, mode='w', index=False)
                            first_chunk = False
                        else:
                            chunk.to_csv(output_path, mode='a', header=False, index=False)
                    
                    # Clean up temp files to manage disk space
                    os.remove(temp_file)
                    
                    self._update_metadata(dataset_id, modified_date)
                    print(f"Successfully processed {dataset_id}")

        except Exception as e:
            print(f"Error processing {dataset_id}: {str(e)}")

    def _needs_update(self, dataset_id, modified_date):
        # Check if we need to process this file
        # Simple string comparison works fine for dates here
        with sqlite3.connect(self.metadata_db) as conn:
            result = conn.execute(
                "SELECT last_modified FROM processed_files WHERE dataset_id = ?", 
                (dataset_id,)
            ).fetchone()
            return not result or result[0] != modified_date

    def _update_metadata(self, dataset_id, modified_date):
        # Track successful processing
        # Using REPLACE to handle updates simply
        with sqlite3.connect(self.metadata_db) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO processed_files VALUES (?, ?, ?)",
                (dataset_id, modified_date, datetime.now().isoformat())
            )

    def process_data(self):
        # Load and process the CMS data catalog
        with open('cms_data.json', 'r') as f:
            data = json.load(f)

        # Only want hospital datasets for now
        # Could make theme configurable later if needed
        hospital_datasets = [
            d for d in data 
            if 'Hospitals' in d.get('theme', [])
        ]

        print(f"Found {len(hospital_datasets)} hospital datasets to process")

        # Process in parallel but limit workers
        # Found 3 workers is sweet spot between speed and memory usage
        with ProcessPoolExecutor(max_workers=3) as executor:
            executor.map(self._process_dataset, hospital_datasets)

if __name__ == "__main__":
    processor = CMSDataProcessor()
    processor.process_data()