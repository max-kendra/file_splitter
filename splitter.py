import os
import json
import hashlib
import time
from tqdm import tqdm

DEFAULT_CHUNK_SIZE = 1024 * 1024 * 1024  # 1 GB

def md5sum(path, block_size=8192):
    """Compute MD5 checksum for a file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()

def parse_size(size_str):
    """Parse human-readable size strings like '500M', '2G', or bytes."""
    s = size_str.strip().upper()
    if s.endswith("G"):
        return int(float(s[:-1]) * 1024**3)
    if s.endswith("M"):
        return int(float(s[:-1]) * 1024**2)
    if s.endswith("K"):
        return int(float(s[:-1]) * 1024)
    return int(s)

def safe_folder_name(name):
    """Replace characters not safe for folder names."""
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in name)

def split_file(file_path, chunk_size):
    base_name = os.path.basename(file_path)
    file_dir = os.path.dirname(file_path)

    # Keep extension and sanitize: e.g. "report.xlsx_split"
    folder_name = safe_folder_name(f"{base_name}_split")
    output_dir = os.path.join(file_dir, folder_name)
    os.makedirs(output_dir, exist_ok=True)

    total_size = os.path.getsize(file_path)
    parts = []

    print(f"\nSplitting '{base_name}' ({total_size / (1024**3):.2f} GB)...\n")

    with open(file_path, "rb") as infile, tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        desc="Overall progress",
        ncols=80,
        leave=True,
    ) as pbar:
        idx = 1
        while True:
            chunk = infile.read(chunk_size)
            if not chunk:
                break

            part_name = f"{base_name}.part{idx:03d}"
            part_path = os.path.join(output_dir, part_name)

            with open(part_path, "wb") as outfile:
                outfile.write(chunk)

            md5 = md5sum(part_path)
            parts.append({"filename": part_name, "size": len(chunk), "md5": md5})
            pbar.update(len(chunk))
            tqdm.write(f"Wrote {part_name} ({len(chunk) / (1024**2):.1f} MB)")
            idx += 1

    manifest = {
        "original_filename": base_name,
        "total_parts": len(parts),
        "chunk_size_bytes": chunk_size,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "parts": parts,
    }

    manifest_path = os.path.join(output_dir, f"{base_name}.manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print("\nSplit complete!")
    print(f"Parts and manifest saved in:\n{output_dir}\n")
    return manifest_path

def main():
    print("────────────────────────────────────────────")
    print("          FILE SPLITTER UTILITY")
    print("────────────────────────────────────────────\n")

    while True:
        file_path = input("Enter the full path to the file you want to split:\n> ").strip('"')
        if os.path.exists(file_path) and os.path.isfile(file_path):
            break
        print("File not found. Please try again.\n")

    chunk_str = input("\nEnter desired chunk size (default 1G, e.g. 500M, 2G):\n> ").strip() or "1G"
    try:
        chunk_size = parse_size(chunk_str)
    except ValueError:
        print("Invalid size format. Using default 1 GB.")
        chunk_size = DEFAULT_CHUNK_SIZE

    try:
        split_file(file_path, chunk_size)
    except Exception as e:
        print(f"\nAn error occurred: {e}")

    input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
