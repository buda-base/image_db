"""
Image and file processing module for extracting metadata from files.

Supports:
- Standard images (JPEG, PNG, TIFF, JPEG2000) via PIL/Pillow and OpenCV
- RAW images via rawpy
- PDF files via PyMuPDF

Uses comprehensive error handling and never assumes file format from extension.
"""

import logging
import hashlib
import os
from typing import Dict, Optional, Tuple, Any
from pathlib import Path
from datetime import datetime

# Optional dependencies with graceful fallback
try:
    from PIL import Image, ExifTags
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    logging.warning("PIL/Pillow not available, image processing will be limited")

try:
    import cv2
    import numpy as np
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    logging.warning("OpenCV not available, bits-per-sample detection will be limited")

try:
    import rawpy
    RAWPY_AVAILABLE = True
except ImportError:
    RAWPY_AVAILABLE = False
    logging.warning("rawpy not available, RAW image processing will be limited")

try:
    import pymupdf
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False
    logging.warning("PyMuPDF not available, PDF processing will be limited")

logger = logging.getLogger(__name__)


# IJG / libjpeg "standard" quantization tables (Annex K.1) for quality=50 (scale S=100)
STD_LUMA = [
    16, 11, 10, 16, 24, 40, 51, 61,
    12, 12, 14, 19, 26, 58, 60, 55,
    14, 13, 16, 24, 40, 57, 69, 56,
    14, 17, 22, 29, 51, 87, 80, 62,
    18, 22, 37, 56, 68, 109, 103, 77,
    24, 35, 55, 64, 81, 104, 113, 92,
    49, 64, 78, 87, 103, 121, 120, 101,
    72, 92, 95, 98, 112, 100, 103, 99,
]

STD_CHROMA = [
    17, 18, 24, 47, 99, 99, 99, 99,
    18, 21, 26, 66, 99, 99, 99, 99,
    24, 26, 56, 99, 99, 99, 99, 99,
    47, 66, 99, 99, 99, 99, 99, 99,
    99, 99, 99, 99, 99, 99, 99, 99,
    99, 99, 99, 99, 99, 99, 99, 99,
    99, 99, 99, 99, 99, 99, 99, 99,
    99, 99, 99, 99, 99, 99, 99, 99,
]


class ImageProcessor:
    """
    Processes image and PDF files to extract metadata and hash.
    
    Handles potentially corrupted files and unknown formats gracefully.
    Never assumes file format from extension - always reads bytes.
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
        Initialize file processor.
        
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
    
    def detect_file_type(self, file_path: str) -> str:
        """
        Detect file type by reading magic bytes (not extension).
        
        Args:
            file_path: Path to file
            
        Returns:
            File type: 'image', 'pdf', 'raw', or 'unknown'
        """
        try:
            with open(file_path, 'rb') as f:
                header = f.read(12)
            
            # Check magic bytes
            if header.startswith(b'\xff\xd8\xff'):  # JPEG
                return 'image'
            elif header.startswith(b'\x89PNG\r\n\x1a\n'):  # PNG
                return 'image'
            elif header.startswith(b'II*\x00') or header.startswith(b'MM\x00*'):  # TIFF
                return 'image'
            elif header.startswith(b'\x00\x00\x00\x0cjP'):  # JPEG2000
                return 'image'
            elif header.startswith(b'%PDF'):  # PDF
                return 'pdf'
            # Check for common RAW formats by magic bytes
            elif self._is_raw_file(file_path, header):
                return 'raw'
            
            return 'unknown'
        except Exception as e:
            logger.error(f"Error detecting file type for {file_path}: {e}")
            return 'unknown'
    
    def _is_raw_file(self, file_path: str, header: bytes) -> bool:
        """
        Check if file is a RAW image format.
        
        Args:
            file_path: Path to file
            header: First bytes of file
            
        Returns:
            True if file appears to be RAW format
        """
        # Common RAW format magic bytes
        raw_signatures = [
            b'II\x2a\x00',  # CR2 (Canon)
            b'MM\x00\x2a',  # Some RAW TIFFs
        ]
        
        for sig in raw_signatures:
            if header.startswith(sig):
                return True
        
        # Try to open with rawpy if available
        if RAWPY_AVAILABLE:
            try:
                with rawpy.imread(file_path) as raw:
                    return True
            except Exception:
                pass
        
        return False
    
    def is_image_file(self, file_path: str) -> bool:
        """
        Check if file is a supported image file by reading bytes.
        
        Args:
            file_path: Path to file
            
        Returns:
            True if file is an image
        """
        file_type = self.detect_file_type(file_path)
        return file_type in ['image', 'raw']
    
    def is_pdf_file(self, file_path: str) -> bool:
        """
        Check if file is a PDF by reading bytes.
        
        Args:
            file_path: Path to file
            
        Returns:
            True if file is a PDF
        """
        return self.detect_file_type(file_path) == 'pdf'
    
    def extract_image_metadata(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Extract metadata from image file.
        
        This is the critical function for image processing. Handles:
        - Potentially corrupted files
        - Unknown/unsupported formats
        - Missing EXIF data
        - RAW images
        
        Args:
            file_path: Path to image file
            
        Returns:
            Dictionary with image metadata or None if not an image
        """
        # Try RAW first if it's detected as RAW
        if self.detect_file_type(file_path) == 'raw':
            return self._extract_raw_metadata(file_path)
        
        # Try standard image processing
        if not PIL_AVAILABLE:
            logger.warning(f"Cannot extract image metadata (PIL not available): {file_path}")
            return None
        
        try:
            with Image.open(file_path) as img:
                # Determine image type by actual format, not extension
                image_type = self._determine_image_type(img)
                
                # Get image mode
                image_mode = self.MODE_MAPPING.get(img.mode, 'OTHER')
                
                # Get dimensions
                width, height = img.size
                
                # Get bits per sample using OpenCV if available
                bps = self._get_bits_per_sample(file_path, img)
                
                # Get compression info (for TIFF)
                tiff_compression = None
                if image_type == 'single_image_tiff':
                    tiff_compression = self._get_tiff_compression(img)
                
                # Get quality (for JPEG)
                quality = None
                if image_type == 'jpg':
                    quality = self._estimate_jpeg_quality(file_path)
                
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
    
    def _extract_raw_metadata(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Extract metadata from RAW image file.
        
        Args:
            file_path: Path to RAW image
            
        Returns:
            Dictionary with image metadata or None
        """
        if not RAWPY_AVAILABLE:
            logger.warning(f"Cannot extract RAW metadata (rawpy not available): {file_path}")
            return None
        
        try:
            with rawpy.imread(file_path) as raw:
                # Get RAW image dimensions
                sizes = raw.sizes
                width = sizes.width
                height = sizes.height
                
                # RAW images are typically 12-16 bits per sample
                # This varies by camera, but we can get from raw properties
                bps = 14  # Common default for most RAW formats
                
                metadata = {
                    'image_type': 'raw',
                    'image_mode': 'OTHER',
                    'width': width,
                    'height': height,
                    'bps': bps,
                    'tiff_compression': None,
                    'quality': None,
                    'recorded_date': None,
                }
                
                return metadata
        except Exception as e:
            logger.error(f"Error extracting RAW metadata from {file_path}: {e}")
            return None
    
    def extract_pdf_metadata(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Extract metadata from PDF file.
        
        Args:
            file_path: Path to PDF file
            
        Returns:
            Dictionary with PDF metadata or None
        """
        if not PYMUPDF_AVAILABLE:
            logger.warning(f"Cannot extract PDF metadata (PyMuPDF not available): {file_path}")
            return None
        
        try:
            doc = pymupdf.open(file_path)
            total_pages = len(doc)
            
            if total_pages == 0:
                doc.close()
                return {
                    'number_of_pages': 0,
                    'median_nb_chr_per_page': 0,
                    'median_nb_images_per_page': 0,
                    'recorded_date': None,
                }
            
            # Determine which pages to analyze
            if total_pages > 20:
                # Skip first 10 pages, analyze next 20
                start_page = 10
                end_page = min(30, total_pages)
            else:
                # Analyze all pages if <= 20 pages
                start_page = 0
                end_page = total_pages
            
            pages_analyzed = end_page - start_page
            total_text_chars = 0
            total_images = 0
            
            for page_num in range(start_page, end_page):
                page = doc[page_num]
                
                # Extract text and count characters
                text = page.get_text()
                # Count only non-whitespace characters
                text_chars = len(''.join(text.split()))
                total_text_chars += text_chars
                
                # Count images on the page
                image_list = page.get_images()
                total_images += len(image_list)
            
            doc.close()
            
            # Calculate medians (using averages as approximation)
            median_chars = total_text_chars // pages_analyzed if pages_analyzed > 0 else 0
            median_images = total_images // pages_analyzed if pages_analyzed > 0 else 0
            
            metadata = {
                'number_of_pages': total_pages,
                'median_nb_chr_per_page': median_chars,
                'median_nb_images_per_page': median_images,
                'recorded_date': None,  # Could extract from PDF metadata if needed
            }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting PDF metadata from {file_path}: {e}")
            return None
    
    def _determine_image_type(self, img: 'Image.Image') -> str:
        """
        Determine database image type from PIL image format (not extension).
        
        Args:
            img: PIL Image object
            
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
        return format_mapping.get(img_format, 'raw')
    
    def _get_bits_per_sample(self, file_path: str, img: 'Image.Image') -> int:
        """
        Get bits per sample from image using OpenCV (preferred) or PIL.
        
        OpenCV reads actual bit depth from file, whereas PIL may not accurately
        represent it for 16-bit images.
        
        Args:
            file_path: Path to image file
            img: PIL Image object
            
        Returns:
            Bits per sample
        """
        # Try OpenCV first for accurate bit depth
        if CV2_AVAILABLE:
            try:
                cv2_img = cv2.imread(file_path, cv2.IMREAD_UNCHANGED)
                if cv2_img is not None:
                    dtype = cv2_img.dtype
                    # Map numpy dtypes to bits per sample
                    if dtype == np.uint8:
                        return 8
                    elif dtype == np.uint16:
                        return 16
                    elif dtype == np.float32 or dtype == np.int32:
                        return 32
                    elif dtype == np.float64 or dtype == np.int64:
                        return 64
            except Exception as e:
                logger.debug(f"OpenCV failed to read {file_path}: {e}")
        
        # Fallback to PIL/TIFF tags
        # Check TIFF tag for actual value
        if hasattr(img, 'tag_v2') and 258 in img.tag_v2:  # BitsPerSample tag
            tag_bps = img.tag_v2[258]
            if isinstance(tag_bps, (list, tuple)):
                return tag_bps[0]
            else:
                return tag_bps
        
        # Final fallback to PIL mode mapping
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
        
        return mode_bps.get(img.mode, 8)
    
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
    
    def _estimate_jpeg_quality(self, file_path: str) -> Optional[int]:
        """
        Estimate JPEG quality from quantization tables.
        
        Returns an integer (1..100) estimating the IJG/libjpeg quality.
        If the JPEG uses nonstandard tables, this may be None or approximate.
        
        Args:
            file_path: Path to JPEG file
            
        Returns:
            Quality value (1-100) or None
        """
        if not PIL_AVAILABLE:
            return None
        
        try:
            img = Image.open(file_path)
            if not hasattr(img, "quantization") or not img.quantization:
                return None

            # Pillow gives a dict: {table_id: [64 coeffs]}. 0: luma, 1: chroma
            qtabs = img.quantization

            # Estimate S from luma and chroma (when present), then combine robustly
            S_candidates = []
            if 0 in qtabs and len(qtabs[0]) == 64:
                S_candidates.append(self._scale_from_tables(qtabs[0], STD_LUMA))
            if 1 in qtabs and len(qtabs[1]) == 64:
                S_candidates.append(self._scale_from_tables(qtabs[1], STD_CHROMA))

            # If we only got one, use it. If both, take the median
            S_candidates = [S for S in S_candidates if S is not None]
            if not S_candidates:
                # Fallback: try all present tables against luma std
                for _, t in qtabs.items():
                    if len(t) == 64:
                        S = self._scale_from_tables(t, STD_LUMA)
                        if S is not None:
                            S_candidates.append(S)
            if not S_candidates:
                return None

            S_candidates.sort()
            S_final = S_candidates[len(S_candidates)//2]
            return self._quality_from_scale(S_final)
        except Exception as e:
            logger.debug(f"Could not estimate JPEG quality for {file_path}: {e}")
            return None
    
    def _scale_from_tables(self, qtable: list, std: list) -> Optional[float]:
        """
        Estimate the libjpeg scale S (1..500) from quantization table.
        
        Args:
            qtable: Quantization table from image
            std: Standard quantization table
            
        Returns:
            Scale value or None
        """
        S_vals = []
        for q, s in zip(qtable, std):
            if s == 0:
                continue
            # Undo rounding: q ≈ floor((s*S + 50)/100)
            low = (100 * q - 50) / s
            high = (100 * q + 49) / s
            # Use the center of the admissible interval
            S_est = (low + high) / 2.0
            # Keep only sane entries
            if 1 <= S_est <= 500:
                S_vals.append(S_est)
        if not S_vals:
            return None
        # Robust aggregation: median
        S_vals.sort()
        return S_vals[len(S_vals)//2]
    
    def _quality_from_scale(self, S: Optional[float]) -> Optional[int]:
        """
        Convert libjpeg scale S to quality Q.
        
        Args:
            S: Scale value
            
        Returns:
            Quality (1-100) or None
        """
        if S is None:
            return None
        if S <= 100:  # corresponds to Q >= 50
            Q = (200 - S) / 2.0
        else:  # corresponds to Q < 50
            Q = 5000.0 / S
        # Round and clamp to [1,100]
        Q = int(round(Q))
        return max(1, min(100, Q))
    
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
    
    def process_file(self, file_path: str) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Process a file and extract all available metadata.
        
        Detects file type by reading bytes (not extension) and processes accordingly.
        
        Args:
            file_path: Path to file
            
        Returns:
            Tuple of (file_info, image_metadata, pdf_metadata)
            - file_info: Always present (hash, size, etc.)
            - image_metadata: Present only for image files
            - pdf_metadata: Present only for PDF files
        """
        # Always get basic file info
        file_info = self.get_file_info(file_path)
        
        # Detect file type by reading bytes
        file_type = self.detect_file_type(file_path)
        
        image_metadata = None
        pdf_metadata = None
        
        if file_type in ['image', 'raw']:
            image_metadata = self.extract_image_metadata(file_path)
        elif file_type == 'pdf':
            pdf_metadata = self.extract_pdf_metadata(file_path)
        
        return file_info, image_metadata, pdf_metadata
