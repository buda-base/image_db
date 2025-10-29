"""
Command-line interface for the BDRC Image Database Tool.

Provides a simple CLI for running the archive processing.
"""

import argparse
import logging
import sys
from pathlib import Path

from .config import Config
from .orchestrator import ImageDatabaseOrchestrator


def setup_logging(verbose: bool = False) -> None:
    """
    Setup logging configuration.
    
    Args:
        verbose: Enable verbose (DEBUG) logging
    """
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('image_db_tool.log')
        ]
    )


def main() -> int:
    """
    Main CLI entry point.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    parser = argparse.ArgumentParser(
        description='BDRC Image Database Tool - Index archive files into SQL database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all archives
  python -m image_db_tool.cli --config config.yaml

  # Process specific root with parallel processing
  python -m image_db_tool.cli --root Archive0 --parallel --workers 8

  # Verbose logging
  python -m image_db_tool.cli --verbose

Environment variables:
  DB_HOST          Database host (default: localhost)
  DB_PORT          Database port (default: 3306)
  DB_USER          Database username (required)
  DB_PASSWORD      Database password (required)
  DB_NAME          Database name (default: storage)
  ARCHIVE_MOUNT_POINT  Archive mount point (default: /mnt)
  WORKERS          Number of parallel workers (default: 4)
  BATCH_SIZE       Database batch size (default: 1000)
        """
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '--root',
        type=str,
        help='Process specific root only (e.g., Archive0)'
    )
    
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Enable parallel processing'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        help='Number of parallel workers (overrides config)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        help='Database batch size (overrides config)'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-scan of all files (default: skip files with same path and size)'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = Config(args.config)
        
        # Override config with CLI arguments
        if args.workers:
            config.set('processing', 'workers', args.workers)
        if args.batch_size:
            config.set('processing', 'batch_size', args.batch_size)
        if args.parallel:
            config.set('processing', 'parallel', True)
        
        logger.info("Starting BDRC Image Database Tool")
        logger.info(f"Configuration: {args.config}")
        
        # Create orchestrator
        orchestrator = ImageDatabaseOrchestrator(config)
        
        # Process archive
        parallel = config.get_processing_config().get('parallel', False) or args.parallel
        stats = orchestrator.process_all(
            root_name=args.root,
            parallel=parallel,
            force=args.force
        )
        
        # Print summary
        print("\n" + "="*60)
        print("Processing Complete")
        print("="*60)
        print(f"Objects processed: {stats.objects_processed}")
        print(f"Files processed: {stats.files_processed}")
        print(f"Files skipped: {stats.files_skipped}")
        print(f"Images processed: {stats.images_processed}")
        print(f"PDFs processed: {stats.pdfs_processed}")
        print(f"Errors: {stats.errors}")
        print(f"Elapsed time: {stats.elapsed_time():.2f} seconds")
        print("="*60)
        
        return 0 if stats.errors == 0 else 1
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
