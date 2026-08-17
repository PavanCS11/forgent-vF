import yaml
import os
from typing import Dict, List, Any

class ConfigLoader:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Loads the YAML configuration file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found at: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            try:
                return yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise ValueError(f"Error parsing YAML configuration: {e}")

    def get_sources(self) -> List[Dict[str, Any]]:
        """Returns the list of source configurations."""
        return self.config.get('sources', [])

    def get_source_by_name(self, name: str) -> Dict[str, Any]:
        """Returns a specific source configuration by name."""
        for source in self.get_sources():
            if source['name'] == name:
                return source
        return None

if __name__ == "__main__":
    # Test the loader
    loader = ConfigLoader("step_01_ingestion/b.config/source_mappings.yaml")
    print(f"Loaded {len(loader.get_sources())} sources.")
