# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

tap-oracle is a Singer tap for Oracle databases, built with the Nekt Singer SDK. It extracts data from Oracle databases and outputs it in Singer format for use in ETL pipelines. The tap supports multiple replication methods including full table sync, incremental sync, and log-based replication using Oracle Log Miner.

**Current Status**: The codebase is being refactored to use SQLAlchemy 2.x with improved stream architecture. The `sync_strategies` module has been removed in favor of a consolidated streams approach.

## Development Commands

### Environment Setup
```bash
# Install dependencies
uv sync

# Install development dependencies  
uv sync --group dev
```

### Running the Tap
```bash
# Run tap directly
uv run tap-oracle --help
uv run tap-oracle --version
uv run tap-oracle --config CONFIG --discover > ./catalog.json

# Alternative entry point
uv run python -m tap_oracle
```

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_discovery.py

# Run tests with verbose output
uv run pytest -v

# Run tests with durations (configured in pyproject.toml)
uv run pytest --durations=10
```

### Code Quality
```bash
# Run pre-commit hooks manually
pre-commit run --all-files

# Format code with ruff
ruff format

# Check code with ruff
ruff check

# Type checking (configured in pyproject.toml)
mypy tap_oracle/
```

## Architecture Overview

### Core Components

1. **TapOracle** (`tap_oracle/tap.py`): Main tap class that orchestrates the data extraction
   - Inherits from SQLTap (Nekt Singer SDK)
   - Handles configuration validation and SSH tunneling
   - Manages stream discovery and synchronization

2. **OracleConnector** (`tap_oracle/connector.py`): Database connection handler
   - Extends SQLConnector with Oracle-specific functionality
   - Handles SQL Alchemy URL generation and connection pooling
   - Includes custom type conformance for date/datetime handling

3. **Stream Classes** (`tap_oracle/streams/`):
   - **OracleStream**: Base stream class for full table and incremental replication (in `common.py`)
   - **OracleLogBasedStream**: Individual log-based streams using Oracle Log Miner (in `log_based.py`)
   - **OracleSingleLogBasedStream**: Coordinator for multiple log-based streams (in `single_log_based.py`)

4. **SSH Tunnel Support** (`tap_oracle/ssh_tunnel.py`): SSH tunneling for secure connections

### Replication Methods

- **FULL_TABLE**: Complete table extraction
- **INCREMENTAL**: Incremental updates based on replication key
- **LOG_BASED**: Change data capture using Oracle Log Miner

### Key Features

- SSH tunnel support for secure database connections
- SSL/TLS encryption support
- Schema filtering capabilities
- Chunked data extraction for large tables
- Custom date/datetime handling to prevent type conversion issues

## Configuration

The tap accepts either individual connection parameters or a complete SQLAlchemy URL:

### Individual Parameters
- `host`, `port`, `user`, `password`, `database`

### SQLAlchemy URL
- `sqlalchemy_url`: Complete Oracle connection string

### Optional Features
- `ssh_tunnel`: SSH tunnel configuration object
- `filter_schemas`: Array of schema names to process
- `chunk_size`: Number of rows to fetch at once (0 = no chunking)
- `date_format`: Custom date format configuration to handle date/datetime strings

## Testing Strategy

Tests are organized by functionality:
- Discovery tests (`test_discovery.py`)
- Full table replication (`test_full_table.py`, `test_full_table_interruption.py`)
- Log Miner tests for different data types (`test_log_miner_*.py`)
- Currently syncing behavior (`test_currently_syncing.py`)
- Unsupported primary key handling (`test_unsupported_pk.py`)

## Dependencies

- **nekt-singer-sdk**: Custom fork of Meltano Singer SDK at v0.2.9
- **oracledb**: Oracle database driver (3.3.0+)
- **sqlalchemy**: SQL toolkit and ORM (2.0.43+)
- **paramiko**: SSH client library for tunneling
- **sshtunnel**: SSH tunnel implementation

## Pre-commit Configuration

The project uses pre-commit hooks for code quality (configured in `.pre-commit-config.yaml`):
- JSON, TOML, YAML validation (excluding VS Code launch.json)
- Ruff formatting and linting
- UV lock file management and sync
- GitHub workflow validation
- Meltano configuration validation
- Dependabot configuration validation