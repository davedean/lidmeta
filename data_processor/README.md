# MusicBrainz Data Processing Pipeline

This directory contains the scripts for the multi-stage, memory-safe pipeline responsible for processing raw MusicBrainz dumps into a structured, queryable format for the Lidarr Metadata Server.

## Architecture

The pipeline is designed to be robust, idempotent, and memory-efficient. It operates in three distinct stages, orchestrated by the main `docker-compose.flat-file.yml` file.

### Stage 1: Extraction (`extract.py`)

-   **Purpose:** Decompresses the raw `artist.tar.xz` and `release-group.tar.xz` archives.
-   **Idempotency:** This script checks if the uncompressed files already exist and will skip the extraction if they do, making subsequent runs much faster.

### Stage 2: Indexing (`build_indexes.py`)

-   **Purpose:** To build several small, fast lookup files (indexes) from the uncompressed data. This is the key to the pipeline's performance and low memory usage.
-   **Method:** It creates indexes that map entity MBIDs (e.g., an artist's ID) to their exact **byte offset** within the large data files.
-   **Idempotency:** This script checks if the final index files already exist and will skip the entire building process if they do.
-   **Output:** The indexes are saved to `/data/processed/indexes/`.

### Stage 3: Processing (`main.py`)

-   **Purpose:** To process each artist one by one, creating the final JSON files.
-   **Method:** This script uses a **per-artist streaming model**.
    1.  It loads all the pre-built indexes into memory.
    2.  It iterates through each artist ID.
    3.  For each artist, it uses the byte-offset indexes to perform near-instantaneous `seek()` operations to read only the specific lines of data it needs for that one artist and their associated release groups.
    4.  It normalizes the data and **immediately writes the final artist and album JSON files to disk.**
    5.  It then discards the artist's data from memory and moves to the next, ensuring a low and constant memory footprint.
-   **Idempotency:** This script uses a `processing_progress.json` file to keep track of which artists have been successfully completed. If the script is stopped and restarted, it will automatically skip artists it has already processed.

## How to Run

The entire pipeline is orchestrated via Docker Compose.

```bash
docker-compose -f docker-compose.flat-file.yml up data-processor --build
```

This command will automatically execute the three stages in the correct order.

## Configuration

Key configuration options are located at the top of `data_processor/main.py` in the `PROCESSING_CONFIG` dictionary.

-   `max_artists_to_process`: Set this to a small integer (e.g., `100`) to perform a test run on a limited number of artists. Set to `None` to process all artists.
-   `include_release_types`: Allows you to filter which types of albums to include (e.g., only `["Album"]`).
-   `exclude_secondary_types`: Allows you to exclude certain album types (e.g., `["Live", "Compilation"]`).

## Output

The final processed data is written to the `/data/processed` directory, which includes:
-   `/artist`: Contains the final, structured artist JSON files.
-   `/album`: Contains the final, structured album JSON files.
-   `artist.db` / `release-group.db`: SQLite databases for search functionality.
-   `/indexes`: The persistent byte-offset indexes.
-   `processing_progress.json`: The resume-state file.
