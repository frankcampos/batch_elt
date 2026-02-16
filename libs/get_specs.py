import os 
import yaml
from dagster import get_dagster_logger

logger = get_dagster_logger()

def get_specs(spec_path:str):
    specs = []
    path_to_file = None
    for root, _, files in os.walk(spec_path):
        for file in files:
            if file == "spec.yaml":
                path_to_file = os.path.join(root,file)
                with open(path_to_file, "r") as stream:
                    try:
                        spec = yaml.safe_load(stream)
                        specs.append(spec)
                    except yaml.YAMLError as exc:
                        logger.error(exc)
    return specs