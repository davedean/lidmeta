#!/usr/bin/env python3
"""
Get MBIDs for popular artists to use in the remote extraction.
"""

# Popular artists with their known MBIDs
POPULAR_ARTISTS = {
    "Radiohead": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
    "The Beatles": "b7ffd2af-418f-4be2-bdd1-22f8a48614da",
    "Pink Floyd": "c0b2500e-0cef-4130-869d-732b23ed9df5",
    "Queen": "5c6acb91-4b9b-4dd6-978d-73e3e9a72e0b",
    "Led Zeppelin": "678d88b2-87bf-4757-9607-2b86bde9f212",
    "The Rolling Stones": "b071f9fa-14b0-4217-8e97-eb41da73f598",
    "David Bowie": "5441c29d-3602-4898-b1a1-b77fa23b8e50",
    "Bob Dylan": "72c536dc-7137-4477-a521-567eeb840fa8",
    "The Who": "9fda7ca5-e7e3-4a24-8f5c-0008ba4974ef",
    "Nirvana": "5b11f4ce-a62d-471e-81fc-a69a9e53c4de",
    "U2": "5925ea08-f33f-43a2-a721-4e306d3dc0b6",
    "Coldplay": "cc197bad-dc9c-440d-a8b7-bfeb160b5b93",
    "Arctic Monkeys": "65f4f0c5-ef9e-490c-aee3-909e7ae6b2ab",
    "The Strokes": "6b335658-09c3-4f8e-b6b1-5a1d2e7e9c8f",
    "Arcade Fire": "52074bad-d7e3-4fb7-b8c3-3ac0c6e2879a"
}

def main():
    print("Popular Artists and their MBIDs:")
    print("=" * 50)

    for artist, mbid in POPULAR_ARTISTS.items():
        print(f"{artist:<20} {mbid}")

    print("\nTo use these in the remote extraction script:")
    print("1. Copy the script to your remote server")
    print("2. Update the ARTISTS_TO_EXTRACT dictionary with the MBIDs you want")
    print("3. Run: python extract_artists_remote.py")
    print("4. Copy the extracted files back to your local machine")

if __name__ == "__main__":
    main()
