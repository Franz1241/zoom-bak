# Zoom Recordings Backup Script

## Overview

This Python script provides a comprehensive backup solution for Zoom recordings, including meeting recordings, phone call recordings, and webinar recordings. It uses Zoom's Server-to-Server OAuth API to authenticate and download all available recordings from your Zoom account to local storage while maintaining metadata in a PostgreSQL database.

## Purpose

- **Complete Backup**: Download all meeting recordings, phone recordings, and webinar recordings from your Zoom account
- **Historical Data**: Retrieve recordings dating back to November 2020 (or your specified start date)
- **Metadata Preservation**: Store comprehensive metadata in PostgreSQL for easy searching and organization
- **Progress Tracking**: Two-phase process that first discovers all recordings, then downloads them with status tracking
- **Error Resilience**: Robust error handling with automatic token refresh and retry mechanisms

## How It Works

### Two-Phase Process

#### Phase 1: Discovery

1. **User Enumeration**: Retrieves all active users from your Zoom account
2. **Recording Discovery**: For each user, searches through date ranges to find all available recordings
3. **Inventory Creation**: Stores all found recordings in the `zoom_recording_inventory_{version}` table with status "found"
4. **Validation**: Reports what was discovered, including specific checks for historical data (2020 recordings)

#### Phase 2: Download

1. **Queue Processing**: Processes all recordings marked as "found" in the inventory
2. **File Download**: Downloads each recording file to organized local directories
3. **Metadata Storage**: Saves detailed metadata to type-specific tables
4. **Status Updates**: Updates inventory status to "downloaded", "failed", or "skipped"

### Directory Structure

```
zoom_backups_{version}/
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

## Prerequisites

### System Requirements

- Python 3.10+
- PostgreSQL database
- Sufficient disk space for recordings storage

### Python Dependencies

```bash
uv add requests psycopg2-binary python-dotenv python-dateutil
```

## Configuration

### Environment Variables

Create a `.env` file in the script directory:

```env
ZOOM_ACCOUNT_ID=your_zoom_account_id
ZOOM_CLIENT_ID=your_zoom_client_id
ZOOM_CLIENT_SECRET=your_zoom_client_secret
```

### Database Configuration

Update the PostgreSQL connection string in the script:

```python
POSTGRES_URL = "postgresql://username:password@localhost:5432/zoom_backups"
```

### Script Configuration

Adjust these variables in the script as needed:

```python
BASE_DIR = "./zoom_backups_{version}"          # Local storage directory
START_DATE = "2020-11-01"               # Earliest date to search
END_DATE = datetime.now().strftime("%Y-%m-%d")  # Latest date
RATE_LIMIT_DELAY = 0.5                  # API rate limiting (seconds)
```

## Database Schema

### Core Tables

#### `zoom_recording_inventory_{version}`

Master inventory of all discovered recordings with download status tracking.

#### `zoom_recordings_{version}`

Meeting recording metadata and file information.

#### `zoom_phone_recordings_{version}`

Phone call recording metadata and file information.

#### `zoom_webinar_recordings_{version}`

Webinar recording metadata and file information.

## Logging System

The script creates comprehensive logs in the `./logs/` directory:

- **`zoom_backup_debug.log`** - Detailed debug information for troubleshooting
- **`zoom_backup_info.log`** - General process information and progress
- **`zoom_backup_warnings.log`** - Warnings and errors only
- **Console output** - Real-time progress information

## Usage

### Basic Execution

```bash
python zoom_backup_script.py
```

### Monitoring Progress

Monitor the console output and log files to track progress:

```bash
# Watch real-time progress
tail -f logs/zoom_backup_info.log

# Check for errors
tail -f logs/zoom_backup_warnings.log
```

## Expected Output

### Discovery Phase

```
2025-06-17 01:00:00 - INFO - Starting recording discovery phase...
2025-06-17 01:00:05 - INFO - [1/50] Discovering recordings for: user1@company.com
2025-06-17 01:00:10 - INFO - Discovered 25 meeting recordings for user1@company.com
2025-06-17 01:00:15 - INFO - Discovery Results:
2025-06-17 01:00:15 - INFO -   meeting: 1250 recordings (2020-11-01 to 2025-06-16)
2025-06-17 01:00:15 - INFO -   phone: 45 recordings (2021-03-15 to 2025-06-16)
```

### Download Phase

```
2025-06-17 01:30:00 - INFO - Starting download phase...
2025-06-17 01:30:00 - INFO - Found 1295 recordings to download
2025-06-17 01:30:05 - INFO - [1/1295] Downloading meeting recording: abc123 (MP4)
2025-06-17 01:30:08 - INFO - Downloaded meeting recording: abc123_def456.mp4 (125648 bytes)
```

### Final Summary

```
2025-06-17 03:45:00 - INFO - Summary:
2025-06-17 03:45:00 - INFO -   Meeting recordings downloaded: 1250
2025-06-17 03:45:00 - INFO -   Phone recordings downloaded: 45
2025-06-17 03:45:00 - INFO -   Inventory status:
2025-06-17 03:45:00 - INFO -     downloaded: 1290
2025-06-17 03:45:00 - INFO -     failed: 5
```

## Troubleshooting

### Common Issues

#### 401 Unauthorized Errors

- **Cause**: Insufficient app permissions or scope issues
- **Solution**:
  1. Verify all required scopes are enabled
  2. Ensure account-level permissions (not user-level)
  3. Check if app needs admin approval

#### Missing Historical Data

- **Cause**: Recordings may have been automatically deleted by Zoom's retention policy
- **Check**: Review the inventory table to see what was actually found vs expected

#### Database Constraint Errors

- **Cause**: Field size limitations or data type mismatches
- **Solution**: The {version} tables have increased field sizes to handle longer IDs

#### Rate Limiting

- **Cause**: Too many API requests too quickly
- **Solution**: Script includes automatic rate limiting and retry logic

### Debugging Steps

1. **Check Discovery Results**: Look at the inventory table to see what recordings were found

   ```sql
   SELECT recording_type, COUNT(*), MIN(start_time), MAX(start_time)
   FROM zoom_recording_inventory_{version}
   GROUP BY recording_type;
   ```

2. **Check 2020 Data**: Verify if expected historical recordings were discovered

   ```sql
   SELECT user_email, COUNT(*)
   FROM zoom_recording_inventory_{version}
   WHERE start_time >= '2020-11-01' AND start_time < '2021-01-01'
   GROUP BY user_email;
   ```

3. **Review Failed Downloads**: Check what failed and why
   ```sql
   SELECT recording_type, status, COUNT(*)
   FROM zoom_recording_inventory_{version}
   GROUP BY recording_type, status;
   ```

## Data Retention Notes

- **Zoom's Policy**: Recordings may be automatically deleted based on your Zoom account's retention settings
- **Cloud Storage**: Only recordings stored in Zoom cloud will be accessible via API
- **Local Recordings**: Recordings saved locally by users are not accessible through this API

## Security Considerations

- Store Zoom credentials securely using environment variables
- Limit database access to necessary users only
- Consider encrypting sensitive recording data at rest
- Regularly rotate Zoom app credentials
- Monitor access logs for unauthorized usage

## Performance Optimization

- **Batch Processing**: Script processes recordings in batches to handle large datasets
- **Rate Limiting**: Built-in delays prevent API throttling
- **Resume Capability**: Two-phase approach allows resuming interrupted downloads
- **Parallel Processing**: Consider running multiple instances for different user groups (advanced)

## Support

For issues related to:

- **Zoom API**: Check [Zoom API Documentation](https://developers.zoom.us/docs/api/)
- **Script Functionality**: Review debug logs and ensure all prerequisites are met
- **Database Issues**: Verify PostgreSQL connection and permissions
