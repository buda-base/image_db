# Image database of BDRC

This repository contains resources (documentation and scripts) for the BDRC image database.

## Contents

- **`docs/`** - Documentation on the archive structure and database design
- **`schemas/`** - SQL schemas for the storage and content databases
- **`src/image_db_tool/`** - Python tool for indexing the archive into the database

## Quick Start

See [src/README.md](src/README.md) for detailed documentation on the image database tool.

### Installation

```bash
pip install -r requirements.txt
```

### Basic Usage

```bash
# Set database credentials
export DB_USER=your_user
export DB_PASSWORD=your_password

# Run the tool
python -m image_db_tool.cli --help
```

For more information, see the [tool documentation](src/README.md).
