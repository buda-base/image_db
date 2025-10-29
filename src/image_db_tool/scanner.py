"""
File scanner module for traversing the BDRC archive structure.

Efficiently scans the archive following the documented structure:
{mount_point}/{root}/{object_id_end}/{object_id}/
"""

import os
import logging
from pathlib import Path
from typing import Iterator, Tuple, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ArchiveFile:
    """Represents a file in the archive with metadata."""
    
    absolute_path: str
    relative_path: str  # Path within the object
    object_id: str
    root_name: str
    file_type: str  # 'image', 'archive', 'source', or 'other'
    volume_folder_id: Optional[str] = None
    
    def __post_init__(self):
        """Validate file paths."""
        if not os.path.exists(self.absolute_path):
            raise ValueError(f"File does not exist: {self.absolute_path}")


class ArchiveScanner:
    """
    Scans the BDRC archive structure efficiently.
    
    The archive structure is:
    {mount_point}/{root}/{object_id_end}/{object_id}/
    
    where root is Archive0, Archive1, Archive2, or Archive3
    and object_id_end is last 2 chars of object_id or '00' if not digits
    """
    
    def __init__(self, mount_point: str, roots: Optional[List[str]] = None):
        """
        Initialize archive scanner.
        
        Args:
            mount_point: Root mount point (e.g., "/mnt")
            roots: List of root names (default: Archive0-3)
        """
        self.mount_point = Path(mount_point)
        self.roots = roots or ['Archive0', 'Archive1', 'Archive2', 'Archive3']
        
        if not self.mount_point.exists():
            raise ValueError(f"Mount point does not exist: {mount_point}")
    
    @staticmethod
    def calculate_object_id_end(object_id: str) -> str:
        """
        Calculate the object_id_end from object_id.
        
        Args:
            object_id: BDRC object ID (e.g., "W22084")
            
        Returns:
            Last 2 characters if they are digits, otherwise "00"
        """
        last_two = object_id[-2:] if len(object_id) >= 2 else "00"
        return last_two if last_two.isdigit() else "00"
    
    def iter_objects(self, root_name: Optional[str] = None) -> Iterator[Tuple[str, str, Path]]:
        """
        Iterate over all storage objects in the archive.
        
        Args:
            root_name: Specific root to scan, or None for all roots
            
        Yields:
            Tuple of (object_id, root_name, object_path)
        """
        roots_to_scan = [root_name] if root_name else self.roots
        
        for root in roots_to_scan:
            root_path = self.mount_point / root
            
            if not root_path.exists():
                logger.warning(f"Root path does not exist: {root_path}")
                continue
            
            logger.info(f"Scanning root: {root_path}")
            
            # Iterate through object_id_end directories
            try:
                for object_id_end_dir in root_path.iterdir():
                    if not object_id_end_dir.is_dir():
                        continue
                    
                    # Iterate through object directories
                    try:
                        for object_dir in object_id_end_dir.iterdir():
                            if not object_dir.is_dir():
                                continue
                            
                            object_id = object_dir.name
                            
                            # Validate object_id format (should start with W)
                            if not object_id.startswith('W'):
                                logger.debug(f"Skipping non-W object: {object_id}")
                                continue
                            
                            # Verify object_id_end matches
                            expected_end = self.calculate_object_id_end(object_id)
                            if object_id_end_dir.name != expected_end:
                                logger.warning(
                                    f"Object {object_id} in wrong directory "
                                    f"(expected {expected_end}, found {object_id_end_dir.name})"
                                )
                            
                            yield object_id, root, object_dir
                    except PermissionError as e:
                        logger.error(f"Permission denied accessing {object_id_end_dir}: {e}")
                    except Exception as e:
                        logger.error(f"Error scanning {object_id_end_dir}: {e}")
            except PermissionError as e:
                logger.error(f"Permission denied accessing {root_path}: {e}")
            except Exception as e:
                logger.error(f"Error scanning {root_path}: {e}")
    
    def iter_object_files(
        self,
        object_id: str,
        object_path: Path
    ) -> Iterator[ArchiveFile]:
        """
        Iterate over all files in a storage object.
        
        Args:
            object_id: BDRC object ID
            object_path: Path to object directory
            
        Yields:
            ArchiveFile objects
        """
        root_name = object_path.parent.parent.name
        
        # Scan known directories
        for dir_type in ['images', 'archive', 'sources']:
            dir_path = object_path / dir_type
            if not dir_path.exists():
                continue
            
            try:
                for file_path in self._walk_directory(dir_path):
                    # Calculate relative path within object
                    try:
                        relative_path = file_path.relative_to(object_path)
                    except ValueError:
                        logger.error(f"Cannot calculate relative path for {file_path}")
                        continue
                    
                    # Extract volume_folder_id for images/archive directories
                    volume_folder_id = None
                    if dir_type in ['images', 'archive'] and len(relative_path.parts) >= 2:
                        volume_folder = relative_path.parts[1]
                        # Extract volume_folder_id from folder name
                        # Format: {object_id}-{volume_folder_id}
                        if '-' in volume_folder:
                            volume_folder_id = volume_folder.split('-', 1)[1]
                    
                    yield ArchiveFile(
                        absolute_path=str(file_path),
                        relative_path=str(relative_path),
                        object_id=object_id,
                        root_name=root_name,
                        file_type=dir_type if dir_type != 'sources' else 'source',
                        volume_folder_id=volume_folder_id
                    )
            except Exception as e:
                logger.error(f"Error scanning {dir_path}: {e}")
        
        # Scan for other directories (not standard but may contain important data)
        try:
            for item in object_path.iterdir():
                if item.is_dir() and item.name not in ['images', 'archive', 'sources']:
                    logger.debug(f"Found non-standard directory: {item.name} in {object_id}")
                    try:
                        for file_path in self._walk_directory(item):
                            try:
                                relative_path = file_path.relative_to(object_path)
                            except ValueError:
                                continue
                            
                            yield ArchiveFile(
                                absolute_path=str(file_path),
                                relative_path=str(relative_path),
                                object_id=object_id,
                                root_name=root_name,
                                file_type='other'
                            )
                    except Exception as e:
                        logger.error(f"Error scanning non-standard directory {item}: {e}")
        except Exception as e:
            logger.error(f"Error listing object directory {object_path}: {e}")
    
    @staticmethod
    def _walk_directory(directory: Path) -> Iterator[Path]:
        """
        Recursively walk directory and yield file paths.
        
        Args:
            directory: Directory to walk
            
        Yields:
            File paths
        """
        try:
            for root, dirs, files in os.walk(directory):
                root_path = Path(root)
                for file_name in files:
                    # Skip hidden files and system files
                    if file_name.startswith('.'):
                        continue
                    yield root_path / file_name
        except PermissionError as e:
            logger.error(f"Permission denied walking {directory}: {e}")
        except Exception as e:
            logger.error(f"Error walking {directory}: {e}")
    
    def scan_object(self, object_id: str, root_name: str) -> List[ArchiveFile]:
        """
        Scan a specific object and return all its files.
        
        Args:
            object_id: BDRC object ID
            root_name: Root name (e.g., "Archive0")
            
        Returns:
            List of ArchiveFile objects
        """
        object_id_end = self.calculate_object_id_end(object_id)
        object_path = self.mount_point / root_name / object_id_end / object_id
        
        if not object_path.exists():
            raise ValueError(f"Object path does not exist: {object_path}")
        
        return list(self.iter_object_files(object_id, object_path))
