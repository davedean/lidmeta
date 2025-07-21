#!/usr/bin/env python3
"""
Quick test to check if we can read the release.tar.xz file.
"""

import tarfile
import lzma
import json
import time

def test_release_file():
    """Test reading the release file."""
    print("Testing release.tar.xz file...")

    start_time = time.time()

    try:
        with lzma.open("deploy/data/mbjson/dump-20250716-001001/release.tar.xz", 'rb') as xz_file:
            with tarfile.open(fileobj=xz_file, mode='r') as tar:
                # Find the release file
                release_member = None
                for member in tar.getmembers():
                    if member.name.endswith('/release'):
                        release_member = member
                        break

                if not release_member:
                    print("❌ No release file found in tar archive")
                    return

                print(f"✅ Found release file: {release_member.name}")
                print(f"   Size: {release_member.size:,} bytes")

                # Try to read first few lines
                release_file = tar.extractfile(release_member)
                if not release_file:
                    print("❌ Could not extract release file")
                    return

                print("📖 Reading first 10 releases...")
                count = 0
                for line_num, line in enumerate(release_file, 1):
                    try:
                        release = json.loads(line.decode('utf-8').strip())
                        count += 1

                        if count <= 3:
                            artist_name = release.get('artist-credit', [{}])[0].get('name', 'Unknown')
                            print(f"   Release {count}: {artist_name}")

                        if count >= 10:
                            break

                    except json.JSONDecodeError as e:
                        print(f"⚠️  Invalid JSON at line {line_num}: {e}")
                        continue
                    except Exception as e:
                        print(f"❌ Error at line {line_num}: {e}")
                        break

                elapsed = time.time() - start_time
                print(f"✅ Successfully read {count} releases in {elapsed:.1f}s")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_release_file()
