#!/usr/bin/env python3
"""
Example usage of the BDRC Image Database Tool.

This script demonstrates how to use the tool programmatically
instead of through the CLI.
"""

import sys
import os
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from image_db_tool.config import Config
from image_db_tool.orchestrator import ImageDatabaseOrchestrator


def example_basic_usage():
    """Example: Basic usage with environment variables."""
    print("Example 1: Basic Usage with Environment Variables")
    print("="*60)
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create config (will use environment variables)
    config = Config()
    
    # Create orchestrator
    orchestrator = ImageDatabaseOrchestrator(config)
    
    # Process all archives (sequential)
    # NOTE: This would actually process the archive if database and files exist
    # stats = orchestrator.process_all(parallel=False)
    
    print("This example shows how to use environment variables:")
    print("  export DB_USER=your_user")
    print("  export DB_PASSWORD=your_password")
    print("  python examples/example_usage.py")
    print()


def example_config_file():
    """Example: Using a configuration file."""
    print("Example 2: Using Configuration File")
    print("="*60)
    
    # Load config from file
    config = Config(config_path='config.yaml')
    
    print("This example loads configuration from config.yaml")
    print(f"  Database host: {config.get('database', 'host', 'not set')}")
    print(f"  Mount point: {config.get('archive', 'mount_point', 'not set')}")
    print()


def example_parallel_processing():
    """Example: Parallel processing with custom settings."""
    print("Example 3: Parallel Processing")
    print("="*60)
    
    # Create config
    config = Config()
    
    # Override settings programmatically
    config._config['processing'] = {
        'workers': 8,
        'batch_size': 5000,
        'parallel': True,
    }
    
    # Create orchestrator
    orchestrator = ImageDatabaseOrchestrator(config)
    
    print("This example shows parallel processing:")
    print(f"  Workers: {config.get('processing', 'workers')}")
    print(f"  Batch size: {config.get('processing', 'batch_size')}")
    print()
    
    # Process with parallel execution
    # stats = orchestrator.process_all(parallel=True)


def example_single_root():
    """Example: Process a single archive root."""
    print("Example 4: Process Single Root")
    print("="*60)
    
    config = Config()
    orchestrator = ImageDatabaseOrchestrator(config)
    
    print("This example processes only Archive0:")
    print("  stats = orchestrator.process_all(root_name='Archive0')")
    print()
    
    # Process only Archive0
    # stats = orchestrator.process_all(root_name='Archive0')


def main():
    """Run all examples."""
    print("\n" + "="*60)
    print("BDRC Image Database Tool - Usage Examples")
    print("="*60 + "\n")
    
    examples = [
        example_basic_usage,
        example_config_file,
        example_parallel_processing,
        example_single_root,
    ]
    
    for example in examples:
        try:
            example()
        except Exception as e:
            print(f"Error in {example.__name__}: {e}")
            import traceback
            traceback.print_exc()
    
    print("="*60)
    print("Note: These are examples only.")
    print("To actually process the archive, you need:")
    print("  1. A running MySQL database with the schema loaded")
    print("  2. Access to the BDRC archive storage")
    print("  3. Proper database credentials configured")
    print("="*60)


if __name__ == '__main__':
    main()
