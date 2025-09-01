import os
from pathlib import Path
from typing import List
import logging 

logging.basicConfig(level= logging.INFO, format = "[%(asctime)s]: %(levelname)s: %(message)s")

nyc_pipeline = 'NYC_taxi_data_pipeline'

nyc_pipeline_list = [
    'config/config.yaml',
    'notebooks/research',
    f"src/{nyc_pipeline}/extract/__init__.py",
    f"src/{nyc_pipeline}/extract/api_extractor.py",
    f"src/{nyc_pipeline}/extract/scraper.py",
    f"src/{nyc_pipeline}/transform/__init__.py",
    f"src/{nyc_pipeline}/transform/cleaning.py",
    f"src/{nyc_pipeline}/transform/transformation.py",
    f"src/{nyc_pipeline}/transform/validation.py",
    f"src/{nyc_pipeline}/load/__init__.py",
    f"src/{nyc_pipeline}/load/to_parquet.py",
    f"src/{nyc_pipeline}/pipeline/__init__.py",
    f"src/{nyc_pipeline}/pipeline/etl_pipeline.py",
    f"src/{nyc_pipeline}/logger/__init__.py",
    f"src/{nyc_pipeline}/logger/logger.py",
    f"src/{nyc_pipeline}/exception/__init__.py",
    f"src/{nyc_pipeline}/exception/custom_exception.py"
    'main.py'
]


for list_file in nyc_pipeline_list:
    file = Path(list_file)
    file_dir, filename = os.path.split(file)

    if file_dir!='':
        os.makedirs(file_dir, exist_ok=True)
        logging.info(f"directory created successfully {file_dir}")

    if not (os.path.exists(file)) or (os.path.getsize(file)==0):
        with open(file, 'w') as fil:
            pass
    
    else:
        logging.info('file exists already') 

