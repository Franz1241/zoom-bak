# Zoom Recordings Backup Tool

## Overview

This Python application provides a comprehensive backup solution for Zoom recordings, including meeting recordings, phone call recordings, and webinar recordings. It uses Zoom's Server-to-Server OAuth API to authenticate and download all available recordings from your Zoom account to local storage while maintaining metadata in a PostgreSQL database.

## Features

- **Complete Backup**: Download all meeting recordings, phone recordings, and webinar recordings from your Zoom account
- **Historical Data**: Retrieve recordings dating back to November 2020 (or your specified start date)
- **Metadata Preservation**: Store comprehensive metadata in PostgreSQL for easy searching and organization
- **Progress Tracking**: Two-phase process that first discovers all recordings, then downloads them with status tracking
- **Robust Error Handling**: Automatic token refresh, retry mechanisms, and comprehensive logging
- **Modular Architecture**: Clean separation of concerns with dedicated modules for different functionalities
- **YAML Configuration**: Centralized configuration management for easy customization
- **Advanced Logging**: Multi-level logging with file rotation and real-time monitoring

## Architecture

The application has been refactored into a modular structure for better maintainability and extensibility:

### Module Structure

```
zoom_bak_v2/
├── main.py                    # Main entry point
├── config.yaml                # Configuration file
├── logging_config.py          # Logging configuration
├── database/                  # Database operations
│   ├── __init__.py
│   ├── inventory.py          # Recording inventory management
│   ├── metadata.py           # Metadata storage operations
│   └── setup.py              # Database schema setup
├── zoom_api/                  # Zoom API interactions
│   ├── __init__.py
│   ├── auth.py               # OAuth authentication
│   ├── discovery.py          # Recording discovery
│   ├── download.py           # File download operations
│   └── user.py               # User management
└── utils/                     # Utility functions
    ├── __init__.py
    ├── api.py                # API request helpers
    ├── file.py               # File operations
    └── misc.py               # Configuration and retry utilities
```

### Key Components

#### Authentication Module (`zoom_api/auth.py`)
- Handles OAuth token management with automatic refresh
- Built-in retry logic for authentication failures
- Token expiration tracking and proactive refresh

#### Discovery Module (`zoom_api/discovery.py`)
- Discovers recordings across all users and date ranges
- Handles pagination and rate limiting
- Stores findings in the inventory database

#### Download Module (`zoom_api/download.py`)
- Processes the recording inventory for downloads
- Manages file organization and metadata storage
- Implements download retry logic with exponential backoff

#### Database Modules (`database/`)
- **setup.py**: Manages database schema and versioning
- **inventory.py**: Handles recording inventory operations
- **metadata.py**: Stores and retrieves recording metadata

#### Utility Modules (`utils/`)
- **misc.py**: Configuration management and retry decorators
- **api.py**: API request helpers with error handling
- **file.py**: File operations and path management

## Configuration

The application uses a centralized YAML configuration system that replaces hardcoded values:

### Configuration File (`config.yaml`)

```yaml
version: "v4"

database:
  url: "postgresql://postgres:postgres@localhost:5432/zoom_backups"

directories:
  base_dir: "./zoom_backups"
  log_dir: "./logs"

dates:
  start_date: "2020-11-01"
  
api:
  rate_limit_delay: 0.5  
  request_timeout: 60    
  retries: 3
  token_refresh_buffer: 300  
  
  page_sizes:
    recordings: 30
    users: 300
    phone_recordings: 30
  
  sleep_durations:
    rate_limit: 60      
    retry: 30           
    token_refresh: 5    
    download_retry: 60  

processing:
  months_per_range: 6   
  
logging:
  levels:
    console: "INFO"
    file_debug: "DEBUG" 
    file_info: "INFO"
    file_warning: "WARNING"
  
  files:
    debug: "zoom_backup_debug.log"
    info: "zoom_backup_info.log"
    warnings: "zoom_backup_warnings.log"
```

## Installation

### Prerequisites

- Python 3.10+
- PostgreSQL database
- Sufficient disk space for recordings storage

### Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd zoom_bak_v2
   ```

2. **Install dependencies using uv:**
   ```bash
   uv sync
   ```

3. **Set up environment variables:**
   Create a `.env` file in the project root:
   ```env
   ZOOM_ACCOUNT_ID=your_zoom_account_id
   ZOOM_CLIENT_ID=your_zoom_client_id
   ZOOM_CLIENT_SECRET=your_zoom_client_secret
   ```

4. **Configure the application:**
   Edit `config.yaml` to match your environment:
   - Update the PostgreSQL connection URL
   - Adjust directory paths as needed
   - Modify date ranges and API settings

## Required Zoom Permissions

Your Zoom Server-to-Server OAuth App **MUST** have the following scopes enabled:

### Essential Scopes

- **`recording:read:admin`** ✅ - Read all account recordings
- **`user:read:admin`** ✅ - List all users in the account
- **`account:read:admin`** ✅ - Access account-level information
- **`phone:read:admin`** ✅ - Access phone recording data

### Permission Level

- **Account-level permissions** (NOT user-level) - Your app must have admin access to the entire Zoom account

### Setting Up Zoom App Permissions

1. Go to [Zoom Marketplace](https://marketplace.zoom.us/)
2. Navigate to "Develop" → "Build App"
3. Select your Server-to-Server OAuth app (or create one)
4. Go to "Scopes" section
5. Enable all required scopes listed above
6. Ensure "Account-level" permissions are selected
7. Get app approval from your Zoom account admin if required

## Usage

### Basic Execution

```bash
# Activate the virtual environment
source .venv/bin/activate

# Run the backup process
python main.py
```

### Monitoring Progress

The application provides comprehensive logging across multiple levels:

```bash
# Watch real-time progress
tail -f logs/zoom_backup_info.log

# Monitor debug information
tail -f logs/zoom_backup_debug.log

# Check for errors and warnings
tail -f logs/zoom_backup_warnings.log
```

## How It Works

### Two-Phase Process

#### Phase 1: Discovery

1. **Database Setup**: Creates or updates database schema with version support
2. **User Enumeration**: Retrieves all active users from your Zoom account
3. **Recording Discovery**: For each user, searches through date ranges to find all available recordings
4. **Inventory Creation**: Stores all found recordings in the `zoom_recording_inventory_v4` table with status "found"
5. **Validation**: Reports what was discovered, including specific checks for historical data

#### Phase 2: Download

1. **Queue Processing**: Processes all recordings marked as "found" in the inventory
2. **File Download**: Downloads each recording file to organized local directories
3. **Metadata Storage**: Saves detailed metadata to type-specific tables
4. **Status Updates**: Updates inventory status to "downloaded", "failed", or "skipped"

### Directory Structure

```
zoom_backups_v4/
├── meetings/
│   └── user@email.com/
│       ├── meeting_id_file_id.mp4
│       ├── meeting_id_file_id.m4a
│       └── meeting_id_file_id.vtt
├── phone/
│   └── user@email.com/
│       └── call_id_timestamp.mp3
└── webinars/
    └── user@email.com/
        └── webinar_files...
```

## Error Handling and Reliability

### Retry Mechanisms

The application implements sophisticated retry logic:

- **API Retry**: Automatic retry for API requests with exponential backoff
- **File Retry**: Robust file download retry with customizable delays
- **Database Retry**: Database operation retry for transient connection issues
- **Token Refresh**: Proactive token refresh to prevent authentication failures

### Logging System

Multi-level logging system with:
- **Console Output**: Real-time progress information
- **Debug Logs**: Detailed technical information for troubleshooting
- **Info Logs**: General process information and progress
- **Warning Logs**: Warnings and errors only

## Database Schema

### Core Tables

#### `zoom_recording_inventory_v4`
Master inventory of all discovered recordings with download status tracking.

#### `zoom_recordings_v4`
Meeting recording metadata and file information.

#### `zoom_phone_recordings_v4`
Phone call recording metadata and file information.

#### `zoom_webinar_recordings_v4`
Webinar recording metadata and file information.

## Expected Output

### Discovery Phase

```
2025-01-18 10:00:00 - INFO - Starting Zoom backup process...
2025-01-18 10:00:01 - INFO - Setting up database with version: v4
2025-01-18 10:00:02 - INFO - Refreshing access token...
2025-01-18 10:00:03 - INFO - Access token refreshed successfully
2025-01-18 10:00:05 - INFO - Found 50 users to process...
2025-01-18 10:00:10 - INFO - [1/50] Discovering recordings for: user1@company.com
2025-01-18 10:00:15 - INFO - Discovered 25 meeting recordings for user1@company.com
```

### Download Phase

```
2025-01-18 10:30:00 - INFO - Starting download phase...
2025-01-18 10:30:00 - INFO - Found 1295 recordings to download
2025-01-18 10:30:05 - INFO - [1/1295] Downloading meeting recording: abc123 (MP4)
2025-01-18 10:30:08 - INFO - Downloaded meeting recording: abc123_def456.mp4 (125648 bytes)
```

### Final Summary

```
2025-01-18 12:45:00 - INFO - Backup process completed!
2025-01-18 12:45:00 - INFO - Summary:
2025-01-18 12:45:00 - INFO -   Meeting recordings downloaded: 1250
2025-01-18 12:45:00 - INFO -   Phone recordings downloaded: 45
2025-01-18 12:45:00 - INFO -   Inventory status:
2025-01-18 12:45:00 - INFO -     downloaded: 1290
2025-01-18 12:45:00 - INFO -     failed: 5
2025-01-18 12:45:00 - INFO - Inventory by year and type:
2025-01-18 12:45:00 - INFO -   2020 (meeting): 45
2025-01-18 12:45:00 - INFO -   2021 (meeting): 234
2025-01-18 12:45:00 - INFO -   2022 (meeting): 456
```

## Troubleshooting

### Common Issues

1. **Authentication Failures**: Check your Zoom app credentials and scopes
2. **Database Connection**: Verify PostgreSQL connection string in `config.yaml`
3. **Disk Space**: Ensure sufficient disk space for recordings storage
4. **Rate Limiting**: Application handles rate limiting automatically with configurable delays

### Debug Mode

Enable debug logging by setting the console log level to "DEBUG" in `config.yaml`:

```yaml
logging:
  levels:
    console: "DEBUG"
```

## Version History

- **v4**: Current version with modular architecture
- **v3**: Previous version (legacy)
- **v2**: Initial version

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions:
1. Check the debug logs in `logs/zoom_backup_debug.log`
2. Review the configuration in `config.yaml`
3. Ensure all Zoom API permissions are properly configured
4. Check database connectivity and schema version
