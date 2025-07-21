#!/usr/bin/env python3
"""
Monitor the streaming release processor progress.
"""

import os
import time
import psutil
from pathlib import Path

def monitor_process(pid):
    """Monitor a process by PID."""
    try:
        process = psutil.Process(pid)

        # Get process info
        cpu_percent = process.cpu_percent()
        memory_info = process.memory_info()
        create_time = process.create_time()
        elapsed_time = time.time() - create_time

        # Get I/O counters if available
        try:
            io_counters = process.io_counters()
            read_bytes = io_counters.read_bytes
            write_bytes = io_counters.write_bytes
        except:
            read_bytes = write_bytes = 0

        print(f"Process {pid} Status:")
        print(f"  CPU: {cpu_percent:.1f}%")
        print(f"  Memory: {memory_info.rss / 1024 / 1024:.1f} MB")
        print(f"  Elapsed: {elapsed_time / 60:.1f} minutes")
        print(f"  Read: {read_bytes / 1024 / 1024 / 1024:.1f} GB")
        print(f"  Write: {write_bytes / 1024 / 1024:.1f} MB")

        # Check if output files exist
        output_dir = Path("deploy/data/mbjson/dump-20250716-001001/streaming_releases")
        if output_dir.exists():
            files = list(output_dir.glob("*.json"))
            print(f"  Output files: {len(files)}")
            for f in files:
                size = f.stat().st_size / 1024 / 1024
                print(f"    {f.name}: {size:.1f} MB")
        else:
            print("  Output directory: not created yet")

    except psutil.NoSuchProcess:
        print(f"Process {pid} not found")
    except Exception as e:
        print(f"Error monitoring process: {e}")

def main():
    """Main monitoring function."""
    pid = 37604  # The streaming processor PID

    print("🎵 Monitoring Streaming Release Processor")
    print("=" * 50)

    while True:
        monitor_process(pid)
        print("-" * 30)
        time.sleep(30)  # Check every 30 seconds

if __name__ == "__main__":
    main()
