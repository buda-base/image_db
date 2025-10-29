"""
Configuration management for the image database tool.

Handles loading database credentials and tool settings from config files
or environment variables with security in mind.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class Config:
    """
    Configuration manager for database credentials and tool settings.
    
    Loads configuration from config.yaml or environment variables.
    Environment variables take precedence over config file.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to config.yaml file. If None, looks in current directory.
        """
        self.config_path = config_path or os.getenv('IMAGE_DB_CONFIG', 'config.yaml')
        self._config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from file and environment variables."""
        # Load from file if exists
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    self._config = yaml.safe_load(f) or {}
                logger.info(f"Loaded configuration from {self.config_path}")
            except Exception as e:
                logger.warning(f"Failed to load config file: {e}")
                self._config = {}
        else:
            logger.info("No config file found, using environment variables")
            self._config = {}
        
        # Override with environment variables (more secure for credentials)
        self._load_from_env()
    
    def _load_from_env(self) -> None:
        """Load sensitive credentials from environment variables."""
        env_mappings = {
            'DB_HOST': ('database', 'host'),
            'DB_PORT': ('database', 'port'),
            'DB_USER': ('database', 'user'),
            'DB_PASSWORD': ('database', 'password'),
            'DB_NAME': ('database', 'name'),
            'ARCHIVE_MOUNT_POINT': ('archive', 'mount_point'),
            'WORKERS': ('processing', 'workers'),
            'BATCH_SIZE': ('processing', 'batch_size'),
        }
        
        for env_var, (section, key) in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                if section not in self._config:
                    self._config[section] = {}
                # Convert numeric values
                if key in ['port', 'workers', 'batch_size']:
                    try:
                        value = int(value)
                    except ValueError:
                        pass
                self._config[section][key] = value
    
    def get(self, section: str, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        return self._config.get(section, {}).get(key, default)
    
    def get_db_config(self) -> Dict[str, Any]:
        """
        Get database configuration.
        
        Returns:
            Dictionary with database connection parameters
            
        Raises:
            ValueError: If required database credentials are missing
        """
        db_config = {
            'host': self.get('database', 'host', 'localhost'),
            'port': self.get('database', 'port', 3306),
            'user': self.get('database', 'user'),
            'password': self.get('database', 'password'),
            'database': self.get('database', 'name', 'storage'),
        }
        
        # Validate required fields
        if not db_config['user']:
            raise ValueError("Database user not configured (set DB_USER env var or config file)")
        if not db_config['password']:
            raise ValueError("Database password not configured (set DB_PASSWORD env var or config file)")
        
        return db_config
    
    def get_archive_config(self) -> Dict[str, Any]:
        """
        Get archive processing configuration.
        
        Returns:
            Dictionary with archive settings
        """
        return {
            'mount_point': self.get('archive', 'mount_point', '/mnt'),
            'roots': self.get('archive', 'roots', ['Archive0', 'Archive1', 'Archive2', 'Archive3']),
        }
    
    def get_processing_config(self) -> Dict[str, Any]:
        """
        Get processing configuration.
        
        Returns:
            Dictionary with processing settings
        """
        return {
            'workers': self.get('processing', 'workers', 4),
            'batch_size': self.get('processing', 'batch_size', 1000),
            'parallel': self.get('processing', 'parallel', False),
        }
