import os,sys
from src.NYC_taxi_data_pipeline.exception.custom_exception import NycTaxiException
from src.NYC_taxi_data_pipeline.logger.logger import logger
from typing import List, Any
from pathlib import Path
import yaml
from box import ConfigBox
from ensure import ensure_annotations
from box.exceptions import BoxValueError


@ensure_annotations
def read_yaml(path_to_yaml: str)->ConfigBox:
        
    """ reads yaml file and returns
        
        Args:
            path_to_yaml (str): path like input
            
        Raises:
            ValueError: if yaml file is empty
            e: empty file
            
        Returns:
            ConfigBox: ConfigBox type
            
        """

    try:
        with open(path_to_yaml, 'r') as yaml_file:
            file = yaml.safe_load(yaml_file)
            logger.info(f"logger file loaded successfully from: {path_to_yaml}")

        return ConfigBox(file)
        
    except BoxValueError:
        raise ValueError('yaml file is empty')
        
    except Exception as e:
        logger.error('yaml file is empty')
        raise NycTaxiException(e, sys)
    

@ensure_annotations
def create_dir(dir_path:List, verbose=True)-> None:
    """ function to create list of directories
    
    Args:
        dir_path (List): list of path of directories to be created
        ignore_log (bool, optional): ignore if multiple dirs is to be created. Default to True
        
    Return:
        None
    
    """
    try:
        
        for dir in dir_path:
            os.makedirs(dir, exist_ok=True)
            
            if verbose:
                logger.info(f"created directory at: {dir}")
    
    except Exception as e:
        logger.error(f'{e}')
        raise NycTaxiException(e, sys)

