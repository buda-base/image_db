"""
Image processing module for extracting metadata from image files.

Uses PIL/Pillow for image analysis with comprehensive error handling.
Designed to work with potentially corrupted or non-standard files.
"""

import logging
import hashlib
import os
from typing import Dict, Optional, Tuple, Any
from pathlib import Path
from datetime import datetime

try:
    from PIL import Image, ExifTags
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    logging.warning("PIL/Pillow not available, image processing will be limited")

logger = logging.getLogger(__name__)


class ImageProcessor:
    """
    Processes image files to extract metadata and hash.
    
    Handles potentially corrupted files and unknown formats gracefully.
    """
    
    # Mapping of PIL image modes to database enum values
    MODE_MAPPING = {
        '1': '1',
        'L': 'L',
        'RGB': 'RGB',
        'RGBA': 'RGBA',
        'CMYK': 'CMYK',
        'P': 'P',
    }
    
    # Mapping of PIL compression values to database enum
    COMPRESSION_MAPPING = {
        'raw': 'raw',
        'tiff_ccitt': 'tiff_ccitt',
        'group3': 'group3',
        'group4': 'group4',
        'tiff_lzw': 'tiff_lzw',
        'tiff_jpeg': 'tiff_jpeg',
        'jpeg': 'jpeg',
        'tiff_adobe_deflate': 'tiff_adobe_deflate',
        'lzma': 'lzma',
    }
    
    def __init__(self, chunk_size: int = 8192):
        """
        Initialize image processor.
        
        Args:
            chunk_size: Chunk size for streaming file reads (bytes)
        """
        self.chunk_size = chunk_size
        if not PIL_AVAILABLE:
            logger.warning("PIL not available, image metadata extraction disabled")
    
    def calculate_sha256(self, file_path: str) -> bytes:
        """
        Calculate SHA256 hash of a file using streaming for memory efficiency.
        
        Args:
            file_path: Path to file
            
        Returns:
            32-byte SHA256 hash
            
        Raises:
            OSError: If file cannot be read
        """
        sha256_hash = hashlib.sha256()
        
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        break
                    sha256_hash.update(chunk)
            return sha256_hash.digest()
        except OSError as e:
            logger.error(f"Error reading file {file_path}: {e}")
            raise
    
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """
        Get basic file information (size, timestamps, hash).
        
        Args:
            file_path: Path to file
            
        Returns:
            Dictionary with file metadata
            
        Raises:
            OSError: If file cannot be accessed
        """
        try:
            stat_info = os.stat(file_path)
            sha256_hash = self.calculate_sha256(file_path)
            
            return {
                'sha256': sha256_hash,
                'size': stat_info.st_size,
                'created_at': datetime.fromtimestamp(stat_info.st_ctime),
                'modified_at': datetime.fromtimestamp(stat_info.st_mtime),
            }
        except OSError as e:
            logger.error(f"Error getting file info for {file_path}: {e}")
            raise
    
    def is_image_file(self, file_path: str) -> bool:
        """
        Check if file is a supported image file.
        
        Args:
            file_path: Path to file
            
        Returns:
            True if file is an image
        """
        if not PIL_AVAILABLE:
            # Fallback to extension check
            ext = Path(file_path).suffix.lower()
            return ext in ['.jpg', '.jpeg', '.tif', '.tiff', '.png', '.jp2']
        
        try:
            with Image.open(file_path) as img:
                img.verify()
            return True
        except Exception:
            return False
    
    def extract_image_metadata(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Extract metadata from image file.
        
        This is the critical function for image processing. Handles:
        - Potentially corrupted files
        - Unknown/unsupported formats
        - Missing EXIF data
        
        Args:
            file_path: Path to image file
            
        Returns:
            Dictionary with image metadata or None if not an image
        """
        if not PIL_AVAILABLE:
            logger.warning(f"Cannot extract image metadata (PIL not available): {file_path}")
            return None
        
        try:
            with Image.open(file_path) as img:
                # Determine image type
                image_type = self._determine_image_type(img, file_path)
                
                # Get image mode
                image_mode = self.MODE_MAPPING.get(img.mode, 'OTHER')
                
                # Get dimensions
                width, height = img.size
                
                # Get bits per sample
                bps = self._get_bits_per_sample(img)
                
                # Get compression info (for TIFF)
                tiff_compression = None
                if image_type == 'single_image_tiff':
                    tiff_compression = self._get_tiff_compression(img)
                
                # Get quality (for JPEG and PNG)
                quality = self._estimate_quality(img, image_type)
                
                # Get recorded date from EXIF
                recorded_date = self._get_exif_date(img)
                
                metadata = {
                    'image_type': image_type,
                    'image_mode': image_mode,
                    'width': width,
                    'height': height,
                    'bps': bps,
                    'tiff_compression': tiff_compression,
                    'quality': quality,
                    'recorded_date': recorded_date,
                }
                
                return metadata
                
        except Exception as e:
            logger.error(f"Error extracting image metadata from {file_path}: {e}")
            return None
    
    def _determine_image_type(self, img: 'Image.Image', file_path: str) -> str:
        """
        Determine database image type from PIL image.
        
        Args:
            img: PIL Image object
            file_path: Path to file (for extension fallback)
            
        Returns:
            Image type string for database enum
        """
        format_mapping = {
            'JPEG': 'jpg',
            'PNG': 'png',
            'TIFF': 'single_image_tiff',
            'JPEG2000': 'jp2',
        }
        
        img_format = img.format
        if img_format in format_mapping:
            return format_mapping[img_format]
        
        # Fallback to extension
        ext = Path(file_path).suffix.lower()
        if ext in ['.jpg', '.jpeg']:
            return 'jpg'
        elif ext in ['.tif', '.tiff']:
            return 'single_image_tiff'
        elif ext == '.png':
            return 'png'
        elif ext == '.jp2':
            return 'jp2'
        else:
            return 'raw'
    
    def _get_bits_per_sample(self, img: 'Image.Image') -> int:
        """
        Get bits per sample from image.
        
        Args:
            img: PIL Image object
            
        Returns:
            Bits per sample
        """
        # Mapping of PIL modes to typical bits per sample
        mode_bps = {
            '1': 1,
            'L': 8,
            'P': 8,
            'RGB': 8,
            'RGBA': 8,
            'CMYK': 8,
            'I': 32,
            'F': 32,
        }
        
        bps = mode_bps.get(img.mode, 8)
        
        # Check TIFF tag for actual value
        if hasattr(img, 'tag_v2') and 258 in img.tag_v2:  # BitsPerSample tag
            tag_bps = img.tag_v2[258]
            if isinstance(tag_bps, (list, tuple)):
                bps = tag_bps[0]
            else:
                bps = tag_bps
        
        return bps
    
    def _get_tiff_compression(self, img: 'Image.Image') -> Optional[str]:
        """
        Get TIFF compression type.
        
        Args:
            img: PIL Image object
            
        Returns:
            Compression type or None
        """
        if not hasattr(img, 'tag_v2'):
            return None
        
        # TIFF Compression tag (259)
        if 259 not in img.tag_v2:
            return 'raw'
        
        compression_code = img.tag_v2[259]
        
        # TIFF compression codes
        compression_codes = {
            1: 'raw',
            2: 'tiff_ccitt',
            3: 'group3',
            4: 'group4',
            5: 'tiff_lzw',
            6: 'tiff_jpeg',
            7: 'jpeg',
            8: 'tiff_adobe_deflate',
            32946: 'lzma',
        }
        
        return compression_codes.get(compression_code, 'other')
    
    def _estimate_quality(
        self,
        img: 'Image.Image',
        image_type: str
    ) -> Optional[int]:
        """
        Estimate image quality.
        
        For JPEG: quality from 0-100
        For PNG: compression level 0-9
        
        Args:
            img: PIL Image object
            image_type: Image type
            
        Returns:
            Quality value or None
        """
        # For JPEG, try to get quality from quantization tables
        if image_type == 'jpg':
            # This is an approximation - exact quality is hard to determine
            # We return None unless we can determine it reliably
            if hasattr(img, 'quantization'):
                # Simplified quality estimation
                # This is a placeholder - actual implementation would be more complex
                return None
            return None
        
        # For PNG, compression is not stored in the file
        elif image_type == 'png':
            return None
        
        return None
    
    def _get_exif_date(self, img: 'Image.Image') -> Optional[datetime]:
        """
        Extract date from EXIF metadata.
        
        Args:
            img: PIL Image object
            
        Returns:
            Datetime object or None
        """
        try:
            exif = img._getexif()
            if not exif:
                return None
            
            # Look for DateTime tags
            for tag_id, tag_name in ExifTags.TAGS.items():
                if tag_name in ['DateTime', 'DateTimeOriginal', 'DateTimeDigitized']:
                    if tag_id in exif:
                        date_str = exif[tag_id]
                        # Parse EXIF datetime format: "YYYY:MM:DD HH:MM:SS"
                        try:
                            return datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")
                        except ValueError:
                            logger.debug(f"Could not parse EXIF date: {date_str}")
            
            return None
        except Exception:
            return None
    
    def process_file(self, file_path: str) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Process a file and extract all available metadata.
        
        Args:
            file_path: Path to file
            
        Returns:
            Tuple of (file_info, image_metadata)
            - file_info: Always present (hash, size, etc.)
            - image_metadata: Present only for image files
        """
        # Always get basic file info
        file_info = self.get_file_info(file_path)
        
        # Try to extract image metadata
        image_metadata = None
        if self.is_image_file(file_path):
            image_metadata = self.extract_image_metadata(file_path)
        
        return file_info, image_metadata
