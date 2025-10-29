# Quick Reference Guide

## Installation

```bash
# Clone repository
git clone https://github.com/buda-base/image_db.git
cd image_db

# Install dependencies
pip install -r requirements.txt

# Or install as package
pip install -e .
```

## Database Setup

```bash
# Create database and load schema
mysql -u root -p -e "CREATE DATABASE storage;"
mysql -u root -p storage < schemas/archive_storage_base_step1.sql
```

## Configuration

### Using Environment Variables (Recommended)

```bash
export DB_HOST=localhost
export DB_PORT=3306
export DB_USER=bdrc_user
export DB_PASSWORD=your_password
export DB_NAME=storage
export ARCHIVE_MOUNT_POINT=/mnt
export WORKERS=8
export BATCH_SIZE=1000
```

### Using Config File

```bash
cp config.yaml.example config.yaml
# Edit config.yaml with your settings
```

## Usage Examples

### Basic Usage

```bash
# Process all archives
python -m image_db_tool.cli

# Or with explicit config
python -m image_db_tool.cli --config config.yaml
```

### Parallel Processing

```bash
# Use 8 workers
python -m image_db_tool.cli --parallel --workers 8
```

### Process Specific Root

```bash
# Process only Archive0
python -m image_db_tool.cli --root Archive0
```

### Production Settings

```bash
# High-performance settings for 350TB archive
export WORKERS=16
export BATCH_SIZE=5000

python -m image_db_tool.cli \
    --parallel \
    --workers 16 \
    --batch-size 5000 \
    --verbose
```

## Module API

### Programmatic Usage

```python
from image_db_tool.config import Config
from image_db_tool.orchestrator import ImageDatabaseOrchestrator

# Create config
config = Config('config.yaml')

# Create orchestrator
orchestrator = ImageDatabaseOrchestrator(config)

# Process archives
stats = orchestrator.process_all(parallel=True)

print(f"Processed {stats.files_processed} files")
print(f"Processed {stats.images_processed} images")
```

### Process Single Object

```python
from image_db_tool.scanner import ArchiveScanner
from pathlib import Path

scanner = ArchiveScanner('/mnt')
orchestrator.process_object('W22084', 'Archive0', Path('/mnt/Archive0/84/W22084'))
```

## Performance Tuning

### CPU-Bound

- Increase `--workers` (up to number of CPU cores)
- Enable `--parallel`

### I/O-Bound

- Increase `--batch-size` (1000-10000)
- Use SSD storage for database
- Ensure high-bandwidth network

### Database Tuning

```sql
-- MySQL configuration
SET GLOBAL innodb_buffer_pool_size = 4G;
SET GLOBAL max_connections = 200;
SET GLOBAL innodb_flush_log_at_trx_commit = 2;
```

## Troubleshooting

### Database Connection Failed

```bash
# Test connection
mysql -h $DB_HOST -u $DB_USER -p$DB_PASSWORD $DB_NAME -e "SHOW TABLES;"
```

### Permission Denied

```bash
# Check archive access
ls -la $ARCHIVE_MOUNT_POINT/Archive0/
```

### Out of Memory

- Reduce `--workers`
- Check `--batch-size` (lower if needed)
- Monitor with `top` or `htop`

## Logs

Logs are written to:
- **Console**: INFO and above
- **File**: `image_db_tool.log`

Use `--verbose` for DEBUG level logging.

## Security Checklist

- ✓ Use environment variables for credentials
- ✓ Never commit config.yaml with real passwords
- ✓ Use least-privilege database user
- ✓ Enable MySQL SSL if over network
- ✓ Keep log files secure (may contain file paths)

## Common Commands

```bash
# Dry run (requires implementing dry-run mode in code)
# python -m image_db_tool.cli --dry-run

# Process with logging
python -m image_db_tool.cli --verbose 2>&1 | tee processing.log

# Process specific root in background
nohup python -m image_db_tool.cli --root Archive0 --parallel &

# Monitor progress
tail -f image_db_tool.log
```
