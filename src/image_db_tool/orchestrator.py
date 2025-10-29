"""
Main orchestrator for the image database tool.

Coordinates scanning, processing, and database operations with
optional parallel processing for high-performance operation.
"""

import logging
import multiprocessing as mp
from typing import Optional, List
from datetime import datetime
from dataclasses import dataclass

from .config import Config
from .database import DatabaseManager
from .scanner import ArchiveScanner, ArchiveFile
from .processor import ImageProcessor

logger = logging.getLogger(__name__)


@dataclass
class ProcessingStats:
    """Statistics for processing run."""
    
    objects_processed: int = 0
    files_processed: int = 0
    images_processed: int = 0
    errors: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    def elapsed_time(self) -> float:
        """Get elapsed time in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


class ImageDatabaseOrchestrator:
    """
    Main orchestrator for processing archive into database.
    
    Handles the complete workflow:
    1. Scan archive structure
    2. Process files (hash, extract metadata)
    3. Store in database
    """
    
    def __init__(self, config: Config):
        """
        Initialize orchestrator.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.db_config = config.get_db_config()
        self.archive_config = config.get_archive_config()
        self.processing_config = config.get_processing_config()
        
        # Initialize components
        self.db_manager = DatabaseManager(
            self.db_config,
            pool_size=self.processing_config.get('workers', 4) + 2
        )
        self.scanner = ArchiveScanner(
            self.archive_config['mount_point'],
            self.archive_config['roots']
        )
        self.processor = ImageProcessor()
        
        self.stats = ProcessingStats()
    
    def process_object(
        self,
        object_id: str,
        root_name: str,
        object_path
    ) -> None:
        """
        Process a single storage object.
        
        Args:
            object_id: BDRC object ID
            root_name: Storage root name
            object_path: Path to object directory
        """
        try:
            # Get or create root
            root_id = self.db_manager.get_or_create_root(root_name)
            
            # Get object metadata from filesystem
            object_stat = object_path.stat()
            created_at = datetime.fromtimestamp(object_stat.st_ctime)
            modified_at = datetime.fromtimestamp(object_stat.st_mtime)
            
            # Get or create object
            object_db_id = self.db_manager.get_or_create_object(
                bdrc_id=object_id,
                root_id=root_id,
                created_at=created_at,
                last_modified_at=modified_at
            )
            
            # Process all files in object
            files = list(self.scanner.iter_object_files(object_id, object_path))
            batch_paths = []
            
            for archive_file in files:
                try:
                    # Process file
                    file_info, image_metadata = self.processor.process_file(
                        archive_file.absolute_path
                    )
                    
                    # Store file in database
                    file_id, persistent_id = self.db_manager.get_or_create_file(
                        sha256_hash=file_info['sha256'],
                        size=file_info['size'],
                        created_at=file_info['created_at'],
                        earliest_mdate=file_info['modified_at']
                    )
                    
                    # Add path (batch for performance)
                    batch_paths.append((
                        file_id,
                        object_db_id,
                        archive_file.relative_path
                    ))
                    
                    # Store image metadata if available
                    if image_metadata:
                        try:
                            self.db_manager.add_image_info(
                                storage_file_id=file_id,
                                **image_metadata
                            )
                            self.stats.images_processed += 1
                        except Exception as e:
                            logger.error(
                                f"Error storing image metadata for {archive_file.absolute_path}: {e}"
                            )
                    
                    self.stats.files_processed += 1
                    
                    # Batch insert paths periodically
                    if len(batch_paths) >= self.processing_config.get('batch_size', 1000):
                        self.db_manager.batch_insert_paths(batch_paths)
                        batch_paths = []
                    
                except Exception as e:
                    logger.error(f"Error processing file {archive_file.absolute_path}: {e}")
                    self.stats.errors += 1
            
            # Insert remaining paths
            if batch_paths:
                self.db_manager.batch_insert_paths(batch_paths)
            
            self.stats.objects_processed += 1
            
            if self.stats.objects_processed % 10 == 0:
                logger.info(
                    f"Progress: {self.stats.objects_processed} objects, "
                    f"{self.stats.files_processed} files, "
                    f"{self.stats.images_processed} images, "
                    f"{self.stats.errors} errors"
                )
        
        except Exception as e:
            logger.error(f"Error processing object {object_id}: {e}")
            self.stats.errors += 1
    
    def process_all(
        self,
        root_name: Optional[str] = None,
        parallel: bool = False
    ) -> ProcessingStats:
        """
        Process all objects in the archive.
        
        Args:
            root_name: Specific root to process, or None for all roots
            parallel: Whether to use parallel processing
            
        Returns:
            Processing statistics
        """
        self.stats = ProcessingStats()
        self.stats.start_time = datetime.now()
        
        logger.info("Starting archive processing")
        logger.info(f"Parallel processing: {parallel}")
        logger.info(f"Root filter: {root_name or 'all'}")
        
        if parallel:
            self._process_parallel(root_name)
        else:
            self._process_sequential(root_name)
        
        self.stats.end_time = datetime.now()
        
        logger.info("Processing complete")
        logger.info(f"Objects processed: {self.stats.objects_processed}")
        logger.info(f"Files processed: {self.stats.files_processed}")
        logger.info(f"Images processed: {self.stats.images_processed}")
        logger.info(f"Errors: {self.stats.errors}")
        logger.info(f"Elapsed time: {self.stats.elapsed_time():.2f} seconds")
        
        return self.stats
    
    def _process_sequential(self, root_name: Optional[str] = None) -> None:
        """
        Process objects sequentially.
        
        Args:
            root_name: Root to process
        """
        for object_id, root, object_path in self.scanner.iter_objects(root_name):
            self.process_object(object_id, root, object_path)
    
    def _process_parallel(self, root_name: Optional[str] = None) -> None:
        """
        Process objects in parallel using multiprocessing.
        
        Args:
            root_name: Root to process
        """
        workers = self.processing_config.get('workers', 4)
        logger.info(f"Starting parallel processing with {workers} workers")
        
        # Collect all objects first (to enable progress tracking)
        objects = list(self.scanner.iter_objects(root_name))
        total_objects = len(objects)
        logger.info(f"Found {total_objects} objects to process")
        
        # Process in parallel using process pool
        with mp.Pool(processes=workers) as pool:
            # Create processing function that includes config
            worker_func = _ParallelWorker(
                self.db_config,
                self.archive_config,
                self.processing_config
            )
            
            # Map objects to workers
            results = pool.starmap(
                worker_func.process_object_wrapper,
                objects
            )
            
            # Aggregate statistics
            for stats_dict in results:
                if stats_dict:
                    self.stats.objects_processed += stats_dict.get('objects', 0)
                    self.stats.files_processed += stats_dict.get('files', 0)
                    self.stats.images_processed += stats_dict.get('images', 0)
                    self.stats.errors += stats_dict.get('errors', 0)


class _ParallelWorker:
    """
    Worker class for parallel processing.
    
    Must be picklable for multiprocessing.
    """
    
    def __init__(
        self,
        db_config: dict,
        archive_config: dict,
        processing_config: dict
    ):
        """Initialize worker with configuration."""
        self.db_config = db_config
        self.archive_config = archive_config
        self.processing_config = processing_config
    
    def process_object_wrapper(
        self,
        object_id: str,
        root_name: str,
        object_path
    ) -> dict:
        """
        Wrapper for processing object in worker process.
        
        Args:
            object_id: BDRC object ID
            root_name: Storage root name
            object_path: Path to object
            
        Returns:
            Statistics dictionary
        """
        # Initialize components in worker process
        db_manager = DatabaseManager(self.db_config, pool_size=2)
        scanner = ArchiveScanner(
            self.archive_config['mount_point'],
            self.archive_config['roots']
        )
        processor = ImageProcessor()
        
        stats = {
            'objects': 0,
            'files': 0,
            'images': 0,
            'errors': 0,
        }
        
        try:
            # Get or create root
            root_id = db_manager.get_or_create_root(root_name)
            
            # Get object metadata
            object_stat = object_path.stat()
            created_at = datetime.fromtimestamp(object_stat.st_ctime)
            modified_at = datetime.fromtimestamp(object_stat.st_mtime)
            
            # Get or create object
            object_db_id = db_manager.get_or_create_object(
                bdrc_id=object_id,
                root_id=root_id,
                created_at=created_at,
                last_modified_at=modified_at
            )
            
            # Process files
            files = list(scanner.iter_object_files(object_id, object_path))
            batch_paths = []
            
            for archive_file in files:
                try:
                    file_info, image_metadata = processor.process_file(
                        archive_file.absolute_path
                    )
                    
                    file_id, _ = db_manager.get_or_create_file(
                        sha256_hash=file_info['sha256'],
                        size=file_info['size'],
                        created_at=file_info['created_at'],
                        earliest_mdate=file_info['modified_at']
                    )
                    
                    batch_paths.append((
                        file_id,
                        object_db_id,
                        archive_file.relative_path
                    ))
                    
                    if image_metadata:
                        try:
                            db_manager.add_image_info(
                                storage_file_id=file_id,
                                **image_metadata
                            )
                            stats['images'] += 1
                        except Exception:
                            pass
                    
                    stats['files'] += 1
                    
                    if len(batch_paths) >= self.processing_config.get('batch_size', 1000):
                        db_manager.batch_insert_paths(batch_paths)
                        batch_paths = []
                
                except Exception:
                    stats['errors'] += 1
            
            if batch_paths:
                db_manager.batch_insert_paths(batch_paths)
            
            stats['objects'] = 1
        
        except Exception:
            stats['errors'] += 1
        
        return stats
