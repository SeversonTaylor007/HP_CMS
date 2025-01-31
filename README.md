# CMS Hospital Data Processor

This Python application downloads and processes hospital-related datasets from the CMS data portal. 

## Pandas not Pyspark (Pyspark requires too many external installations for this use-case)

## Features - covering questions 
- Downloads hospital-related datasets from CMS
- Converts column names to snake_case format
- Tracks processed files using SQLite
- Parallel processing for faster execution
- Error handling and logging

## Installation


1. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

1. Prepare your CMS data JSON file:
   - Name it `cms_data.json`
   - Place it in the root directory of the project

2. Run the processor:
```bash
python main.py
```

## Output
- Processed CSV files are saved in `data/processed/`
- Each file is named with its dataset identifier
- Processing metadata is stored in `data/metadata.db`

## Project Structure
```
.
├── main.py              # Main processing script
├── requirements.txt     # Python dependencies
├── README.md           # This file
├── cms_data.json       # Input data catalog
└── data/
    ├── metadata.db     # Processing history
    └── processed/      # Output directory
```

## Error Handling
The script handles common issues like:
- Large file processing
- Mixed data types
- Memory constraints

## Author
Taylor Severson
