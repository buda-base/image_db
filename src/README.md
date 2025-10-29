# BDRC Image Database Tool

A high-performance, secure Python tool for indexing the BDRC image archive into a SQL database. Designed to handle 350TB+ of image data with parallel processing capabilities.

## Features

- **High Performance**: Optimized for I/O operations on massive datasets (350TB+)
- **Parallel Processing**: Multi-process support for faster indexing
- **Security-Focused**: 
  - Parameterized SQL queries to prevent injection
  - Input validation for all data
  - Secure credential management via environment variables
  - Comprehensive error handling for file operations
- **Robust Error Handling**: Never assumes files are in correct format, handles corrupted/missing files gracefully
- **Modular Design**: Clean separation of concerns with well-documented functions
- **Batch Operations**: Database operations are batched for optimal performance
- **Connection Pooling**: Efficient database connection management

## Architecture

### Code Structure

```
src/image_db_tool/
├── __init__.py          # Package initialization
├── cli.py               # Command-line interface
├── config.py            # Configuration management (env vars + config file)
├── database.py          # Database operations (connection pooling, batch inserts)
├── scanner.py           # Archive structure scanner (file traversal)
├── processor.py         # Image processing (metadata extraction, hashing)
└── orchestrator.py      # Main workflow orchestrator (sequential/parallel)
```

### Workflow

1. **Scanner** (`scanner.py`): Traverses the archive structure following BDRC conventions
2. **Processor** (`processor.py`): Computes SHA256 hashes and extracts image metadata
3. **Database Manager** (`database.py`): Stores data with deduplication and batch operations
4. **Orchestrator** (`orchestrator.py`): Coordinates the workflow, supports parallel execution

## Installation

### Prerequisites

- Python 3.8+
- MySQL 5.7+ or MariaDB 10.3+
- Access to BDRC archive storage

### Install Dependencies

```bash
pip install -r requirements.txt
```

Required packages:
- `mysql-connector-python` - MySQL database driver
- `PyYAML` - Configuration file parsing
- `Pillow` - Image processing and metadata extraction

### Database Setup

Run the SQL schema to create the database structure:

```bash
mysql -u root -p < schemas/archive_storage_base_step1.sql
```

## Configuration

### Option 1: Environment Variables (Recommended for credentials)

```bash
export DB_HOST=localhost
export DB_PORT=3306
export DB_USER=bdrc_user
export DB_PASSWORD=your_secure_password
export DB_NAME=storage
export ARCHIVE_MOUNT_POINT=/mnt
export WORKERS=8
export BATCH_SIZE=1000
```

### Option 2: Configuration File

Copy the example config and customize:

```bash
cp config.yaml.example config.yaml
# Edit config.yaml with your settings
```

**Note**: Environment variables take precedence over config file values.

## Usage

### Basic Usage

Process all archives:

```bash
python -m image_db_tool.cli --config config.yaml
```

### Parallel Processing

Process with 8 parallel workers:

```bash
python -m image_db_tool.cli --parallel --workers 8
```

### Process Specific Root

Process only Archive0:

```bash
python -m image_db_tool.cli --root Archive0
```

### Verbose Logging

Enable debug logging:

```bash
python -m image_db_tool.cli --verbose
```

### Complete Example

```bash
# Using environment variables for credentials
export DB_USER=bdrc_user
export DB_PASSWORD=secure_password

# Process Archive0 with 8 parallel workers, batch size 2000
python -m image_db_tool.cli \
    --root Archive0 \
    --parallel \
    --workers 8 \
    --batch-size 2000 \
    --verbose
```

## Performance Optimization

### For Large-Scale Processing (350TB)

1. **Parallel Processing**: Use `--parallel` with `--workers` set to number of CPU cores
2. **Batch Size**: Increase `--batch-size` to 5000-10000 for faster database operations
3. **Database Tuning**: 
   - Increase `innodb_buffer_pool_size`
   - Adjust `max_connections` to support connection pool
   - Consider using SSD storage for database
4. **Network**: Ensure high-bandwidth connection between processing server and database

### Recommended Settings for Production

```bash
export WORKERS=16          # Adjust based on CPU cores
export BATCH_SIZE=5000     # Larger batches for better throughput
python -m image_db_tool.cli --parallel --verbose
```

## Security Features

### SQL Injection Prevention
- All database queries use parameterized statements
- No string concatenation for SQL queries

### Input Validation
- BDRC ID length and format validation
- File path length checks
- SHA256 hash verification (must be exactly 32 bytes)
- Enum value validation for image types

### Error Handling
- All file operations wrapped in try-except blocks
- Permission errors logged but don't stop processing
- Corrupted files skipped with error logging
- Database connection errors handled gracefully

### Credential Security
- Support for environment variables
- No hardcoded credentials
- Config file can be kept outside version control

## Technical Details

### File Deduplication

Files are deduplicated based on SHA256 hash + size. If the same file exists in multiple locations, it's stored once in `storage.files` with multiple entries in `storage.paths`.

### Hash Collision Handling

In the rare case of a SHA256 collision for the persistent ID, the system generates a random 32-byte ID.

### Image Metadata Extraction

The tool extracts:
- Image dimensions (width, height)
- Image type (JPEG, PNG, TIFF, JPEG2000)
- Color mode (RGB, RGBA, CMYK, etc.)
- Bits per sample
- TIFF compression type
- EXIF recorded date

### Memory Efficiency

- Files are read in chunks (8KB) for hashing
- No loading of entire files into memory
- Streaming operations for large files

### Archive Structure

The tool follows the BDRC archive structure:
```
{mount_point}/{root}/{object_id_end}/{object_id}/
  ├── images/{object_id}-{volume_folder_id}/
  ├── archive/{object_id}-{volume_folder_id}/
  └── sources/
```

## Logging

Logs are written to:
- **Console**: INFO level and above
- **File**: `image_db_tool.log` (all levels with `--verbose`)

Log format includes timestamp, module name, level, and message.

## Error Recovery

The tool is designed to be restartable:
- Database operations use `INSERT ... ON DUPLICATE KEY UPDATE`
- Existing files/objects are detected and skipped
- Processing can be interrupted and resumed

## Troubleshooting

### Database Connection Issues

```bash
# Test database connection
mysql -h $DB_HOST -u $DB_USER -p$DB_PASSWORD $DB_NAME -e "SHOW TABLES;"
```

### Permission Errors

Ensure the user running the tool has:
- Read access to archive directories
- Write access to log file location
- Network access to database server

### Performance Issues

1. Check database server load
2. Monitor disk I/O on archive storage
3. Reduce `--workers` if CPU-bound
4. Increase `--batch-size` for better database throughput

## Development

### Running Tests

```bash
# Create test database
mysql -u root -p -e "CREATE DATABASE storage_test;"
mysql -u root -p storage_test < schemas/archive_storage_base_step1.sql

# Run tests (if test suite exists)
python -m pytest tests/
```

### Code Style

The code follows:
- PEP 8 style guidelines
- Type hints for function parameters
- Comprehensive docstrings
- Security-first design

## License

See LICENSE file for details.

## Support

For issues or questions, please contact the BDRC development team.
