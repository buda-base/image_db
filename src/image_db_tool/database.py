"""
Database operations module.

Handles all database interactions with connection pooling, parameterized queries
for security, and batch operations for performance.
"""

import logging
import hashlib
import secrets
from typing import Dict, List, Any, Optional, Tuple
from contextlib import contextmanager
import mysql.connector
from mysql.connector import pooling
from mysql.connector.pooling import PooledMySQLConnection

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages database connections and operations with security and performance in mind.
    
    Uses connection pooling for better performance and parameterized queries
    to prevent SQL injection.
    """
    
    def __init__(self, db_config: Dict[str, Any], pool_size: int = 10):
        """
        Initialize database manager with connection pooling.
        
        Args:
            db_config: Database configuration dictionary
            pool_size: Size of connection pool
        """
        self.db_config = db_config
        try:
            self.pool = pooling.MySQLConnectionPool(
                pool_name="image_db_pool",
                pool_size=pool_size,
                pool_reset_session=True,
                **db_config
            )
            logger.info(f"Database connection pool created with size {pool_size}")
        except mysql.connector.Error as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self) -> PooledMySQLConnection:
        """
        Context manager for database connections.
        
        Yields:
            Database connection from pool
        """
        conn = None
        try:
            conn = self.pool.get_connection()
            yield conn
        except mysql.connector.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    def get_or_create_root(self, root_name: str, layout: str = "bdrc_legacy") -> int:
        """
        Get or create a storage root entry.
        
        Args:
            root_name: Name of the storage root (e.g., "Archive0")
            layout: Storage layout type
            
        Returns:
            Root ID
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Check if root exists
                cursor.execute(
                    "SELECT id FROM storage.roots WHERE name = %s",
                    (root_name,)
                )
                result = cursor.fetchone()
                
                if result:
                    return result[0]
                
                # Create new root
                cursor.execute(
                    "INSERT INTO storage.roots (name, layout) VALUES (%s, %s)",
                    (root_name, layout)
                )
                conn.commit()
                return cursor.lastrowid
            finally:
                cursor.close()
    
    def get_or_create_object(
        self,
        bdrc_id: str,
        root_id: int,
        created_at: Optional[str] = None,
        last_modified_at: Optional[str] = None
    ) -> int:
        """
        Get or create a storage object entry.
        
        Args:
            bdrc_id: BDRC identifier (e.g., "W22084")
            root_id: Storage root ID
            created_at: Creation timestamp
            last_modified_at: Last modification timestamp
            
        Returns:
            Object ID
            
        Raises:
            ValueError: If bdrc_id exceeds 32 characters (security check)
        """
        # Security: validate BDRC ID length and format
        if len(bdrc_id) > 32:
            raise ValueError(f"BDRC ID too long: {bdrc_id}")
        if not bdrc_id.replace('-', '').replace('_', '').isalnum():
            raise ValueError(f"Invalid BDRC ID format: {bdrc_id}")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Check if object exists
                cursor.execute(
                    "SELECT id FROM storage.objects WHERE bdrc_id = %s AND root = %s",
                    (bdrc_id, root_id)
                )
                result = cursor.fetchone()
                
                if result:
                    return result[0]
                
                # Create new object
                cursor.execute(
                    """INSERT INTO storage.objects 
                       (bdrc_id, root, created_at, last_modified_at)
                       VALUES (%s, %s, %s, %s)""",
                    (bdrc_id, root_id, created_at, last_modified_at)
                )
                conn.commit()
                return cursor.lastrowid
            finally:
                cursor.close()
    
    def get_or_create_file(
        self,
        sha256_hash: bytes,
        size: int,
        created_at: Optional[str] = None,
        earliest_mdate: Optional[str] = None
    ) -> Tuple[int, bytes]:
        """
        Get or create a file entry (deduplication based on sha256+size).
        
        Args:
            sha256_hash: SHA256 hash of file content (32 bytes)
            size: File size in bytes
            created_at: Creation timestamp
            earliest_mdate: Earliest modification date
            
        Returns:
            Tuple of (file_id, persistent_id)
            
        Raises:
            ValueError: If sha256_hash is not 32 bytes
        """
        if len(sha256_hash) != 32:
            raise ValueError(f"SHA256 hash must be 32 bytes, got {len(sha256_hash)}")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Check if file exists (deduplication)
                cursor.execute(
                    "SELECT id, persistent_id FROM storage.files WHERE sha256 = %s AND size = %s",
                    (sha256_hash, size)
                )
                result = cursor.fetchone()
                
                if result:
                    return result[0], result[1]
                
                # Generate persistent ID (sha256 or random if collision)
                persistent_id = sha256_hash
                
                # Check for collision
                cursor.execute(
                    "SELECT id FROM storage.files WHERE persistent_id = %s",
                    (persistent_id,)
                )
                if cursor.fetchone():
                    # Collision: generate random ID
                    persistent_id = secrets.token_bytes(32)
                    logger.warning(f"Persistent ID collision detected, using random ID")
                
                # Create new file
                cursor.execute(
                    """INSERT INTO storage.files 
                       (sha256, size, persistent_id, created_at, earliest_mdate)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (sha256_hash, size, persistent_id, created_at, earliest_mdate)
                )
                conn.commit()
                return cursor.lastrowid, persistent_id
            finally:
                cursor.close()
    
    def add_file_path(
        self,
        file_id: int,
        storage_object_id: int,
        path: str
    ) -> None:
        """
        Add a file path entry.
        
        Args:
            file_id: File ID
            storage_object_id: Storage object ID
            path: File path within object
            
        Raises:
            ValueError: If path exceeds 1024 characters
        """
        if len(path) > 1024:
            raise ValueError(f"Path too long ({len(path)} > 1024): {path}")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """INSERT INTO storage.paths (file, storage_object, path)
                       VALUES (%s, %s, %s)""",
                    (file_id, storage_object_id, path)
                )
                conn.commit()
            finally:
                cursor.close()
    
    def add_image_info(
        self,
        storage_file_id: int,
        image_type: str,
        image_mode: str,
        width: int,
        height: int,
        bps: int,
        tiff_compression: Optional[str] = None,
        quality: Optional[int] = None,
        recorded_date: Optional[str] = None
    ) -> None:
        """
        Add image metadata to content.image_file_infos table.
        
        Args:
            storage_file_id: Storage file ID
            image_type: Type of image (jpg, png, single_image_tiff, jp2, raw)
            image_mode: PIL image mode (1, L, RGB, RGBA, CMYK, P, OTHER)
            width: Image width in pixels
            height: Image height in pixels
            bps: Bits per sample
            tiff_compression: TIFF compression type (if applicable)
            quality: Image quality (0-100 for JPEG, 0-9 for PNG)
            recorded_date: Recorded date from EXIF
        """
        # Validate enum values
        valid_image_types = {'jpg', 'png', 'single_image_tiff', 'jp2', 'raw'}
        valid_image_modes = {'1', 'L', 'RGB', 'RGBA', 'CMYK', 'P', 'OTHER'}
        
        if image_type not in valid_image_types:
            raise ValueError(f"Invalid image_type: {image_type}")
        if image_mode not in valid_image_modes:
            raise ValueError(f"Invalid image_mode: {image_mode}")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """INSERT INTO content.image_file_infos 
                       (storage_file, image_type, image_mode, width, height, bps,
                        tiff_compression, quality, recorded_date)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                       image_type = VALUES(image_type),
                       image_mode = VALUES(image_mode),
                       width = VALUES(width),
                       height = VALUES(height),
                       bps = VALUES(bps),
                       tiff_compression = VALUES(tiff_compression),
                       quality = VALUES(quality),
                       recorded_date = VALUES(recorded_date)""",
                    (storage_file_id, image_type, image_mode, width, height, bps,
                     tiff_compression, quality, recorded_date)
                )
                conn.commit()
            finally:
                cursor.close()
    
    def batch_insert_paths(self, paths: List[Tuple[int, int, str]]) -> None:
        """
        Batch insert file paths for better performance.
        
        Args:
            paths: List of tuples (file_id, storage_object_id, path)
        """
        if not paths:
            return
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(
                    """INSERT INTO storage.paths (file, storage_object, path)
                       VALUES (%s, %s, %s)""",
                    paths
                )
                conn.commit()
                logger.info(f"Batch inserted {len(paths)} paths")
            finally:
                cursor.close()
