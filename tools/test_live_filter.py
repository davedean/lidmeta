#!/usr/bin/env python3
"""
Test script to verify the improved live filtering catches live recordings by title.
"""

import json
import sys
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from filter_release_groups import is_lidarr_compatible

# The problematic release that slipped through
test_release = {
    "primary-type-id": "6d0c5bf6-7a33-3420-a519-44fc63eedebf",
    "title": "Live at the Astoria",
    "rating": {"value": None, "votes-count": 0},
    "annotation": None,
    "secondary-type-ids": [],
    "id": "27f41de5-d887-3873-8ae8-eee94401e671",
    "relations": [],
    "first-release-date": "1995",
    "tags": [{"name": "alternative rock", "count": 1}, {"name": "rock", "count": 1}],
    "disambiguation": "",
    "genres": [
        {"count": 1, "name": "alternative rock", "disambiguation": "", "id": "ceeaa283-5d7b-4202-8d1d-e25d116b2a18"},
        {"id": "0e3fc579-2d24-4f20-9dae-736e1ec78798", "disambiguation": "", "count": 1, "name": "rock"}
    ],
    "aliases": [],
    "primary-type": "EP",
    "secondary-types": [],
    "artist-credit": [{"artist": {"name": "Radiohead"}, "name": "Radiohead", "joinphrase": ""}]
}

# Test some other live recordings that should be filtered
test_cases = [
    ("Live at the Astoria", "EP", [], "Should be filtered - live recording"),
    ("Live from the Basement", "Album", [], "Should be filtered - live recording"),
    ("Live in Concert", "Single", [], "Should be filtered - live recording"),
    ("OK Computer", "Album", [], "Should be kept - studio album"),
    ("The Bends", "Album", [], "Should be kept - studio album"),
    ("Creep", "Single", [], "Should be kept - studio single"),
    ("My Iron Lung", "EP", [], "Should be kept - studio EP"),
    ("Live at the Astoria", "Album", ["Live"], "Should be filtered - has Live secondary type"),
    ("Greatest Hits", "Album", ["Compilation"], "Should be filtered - compilation"),
]

logger.info("Testing improved live filtering:")
logger.info("=" * 60)

for title, primary_type, secondary_types, description in test_cases:
    test_release_copy = test_release.copy()
    test_release_copy["title"] = title
    test_release_copy["primary-type"] = primary_type
    test_release_copy["secondary-types"] = secondary_types

    result = is_lidarr_compatible(test_release_copy)
    status = "FILTERED" if not result else "KEPT"

    logger.info(f"{status:8} | {primary_type:8} | {title:25} | {description}")

    if title == "Live at the Astoria" and result:
        logger.error("  ❌ ERROR: Live at the Astoria should be filtered!")
    elif title == "Live at the Astoria" and not result:
        logger.info("  ✅ SUCCESS: Live at the Astoria correctly filtered!")

logger.info("\n" + "=" * 60)
