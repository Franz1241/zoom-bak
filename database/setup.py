"""
Database setup module for Zoom Backup application.
Handles table creation, indexes, and database schema setup.
"""

from logging_config import get_logger

logger = get_logger()


def setup_database(cursor, conn, version: str):
    """
    Create tables and indexes if they don't exist.

    Args:
        cursor: Database cursor
        conn: Database connection
        version: Version string for table naming
    """
    logger.info("Setting up database tables...")

    # Recording inventory table - tracks all recordings found before download
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS zoom_recording_inventory_{version} (
            id SERIAL PRIMARY KEY,
            recording_type VARCHAR(20) NOT NULL, -- 'meeting', 'phone', 'webinar'
            recording_id VARCHAR(128) NOT NULL,
            meeting_id VARCHAR(128),
            user_email VARCHAR(320),
            topic TEXT,
            start_time TIMESTAMPTZ,
            duration INTEGER,
            file_type VARCHAR(32),
            file_size BIGINT,
            download_url TEXT,
            status VARCHAR(20) DEFAULT 'found', -- 'found', 'downloaded', 'failed', 'skipped'
            found_at TIMESTAMPTZ DEFAULT NOW(),
            downloaded_at TIMESTAMPTZ,
            error_message TEXT,
            raw_data JSON,
            UNIQUE(recording_type, recording_id, file_type)
        );
    """)

    # Meeting recordings table (updated with version and longer varchar fields)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS zoom_recordings_{version} (
            id SERIAL PRIMARY KEY,
            meeting_id VARCHAR(128),
            recording_id VARCHAR(128),
            topic TEXT,
            host_id VARCHAR(128),
            host_email VARCHAR(320),
            start_time TIMESTAMPTZ,
            duration INTEGER,
            file_type VARCHAR(32),
            file_size BIGINT,
            recording_type VARCHAR(64),
            download_url TEXT,
            transcript_url TEXT,
            path TEXT,
            transcript_path TEXT,
            downloaded_at TIMESTAMPTZ DEFAULT NOW(),
            data_type VARCHAR(20) DEFAULT 'meeting',
            unprocessed JSON DEFAULT '{{}}'
        );
    """)

    # Phone recordings table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS zoom_phone_recordings_{version} (
            id SERIAL PRIMARY KEY,
            recording_id VARCHAR(128) UNIQUE,
            call_id VARCHAR(128),
            caller_number VARCHAR(32),
            callee_number VARCHAR(32),
            caller_name VARCHAR(255),
            callee_name VARCHAR(255),
            direction VARCHAR(16),
            start_time TIMESTAMPTZ,
            end_time TIMESTAMPTZ,
            duration INTEGER,
            file_type VARCHAR(32),
            file_size BIGINT,
            download_url TEXT,
            path TEXT,
            owner_id VARCHAR(128),
            owner_email VARCHAR(320),
            downloaded_at TIMESTAMPTZ DEFAULT NOW(),
            unprocessed JSON DEFAULT '{{}}'
        );
    """)

    # Webinars table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS zoom_webinar_recordings_{version} (
            id SERIAL PRIMARY KEY,
            webinar_id VARCHAR(128),
            recording_id VARCHAR(128),
            topic TEXT,
            host_id VARCHAR(128),
            host_email VARCHAR(320),
            start_time TIMESTAMPTZ,
            duration INTEGER,
            file_type VARCHAR(32),
            file_size BIGINT,
            recording_type VARCHAR(64),
            download_url TEXT,
            transcript_url TEXT,
            path TEXT,
            transcript_path TEXT,
            downloaded_at TIMESTAMPTZ DEFAULT NOW(),
            unprocessed JSON DEFAULT '{{}}'
        );
    """)

    # Create indexes for better performance
    _create_indexes(cursor, version)

    conn.commit()
    logger.info("Database tables created successfully")


def _create_indexes(cursor, version: str):
    """
    Create database indexes for better performance.

    Args:
        cursor: Database cursor
        version: Version string for table naming
    """
    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_inventory_user_email ON zoom_recording_inventory_{version}(user_email);",
        f"CREATE INDEX IF NOT EXISTS idx_inventory_start_time ON zoom_recording_inventory_{version}(start_time);",
        f"CREATE INDEX IF NOT EXISTS idx_inventory_status ON zoom_recording_inventory_{version}(status);",
    ]

    for index_sql in indexes:
        cursor.execute(index_sql)

    logger.debug(f"Created {len(indexes)} indexes for version {version}")

