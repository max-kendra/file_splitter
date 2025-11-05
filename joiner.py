import os
import json
import hashlib
from tqdm import tqdm

def md5sum(path, block_size=8192):
    """Compute MD5 checksum for a file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()

def join_from_manifest(manifest_path):
    """Reassemble split parts into a single file, saving output next to the manifest."""
    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    base_dir = os.path.dirname(manifest_path)
    base_name = manifest["original_filename"]
    out_path = os.path.join(base_dir, base_name)
    total_size = sum(p["size"] for p in manifest["parts"])

    print(f"\nReassembling '{base_name}' from {manifest['total_parts']} parts...\n")

    with open(out_path, "wb") as outfile, tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        desc="Merging",
        ncols=80,
        leave=True,
    ) as pbar:
        for part in manifest["parts"]:
            part_path = os.path.join(base_dir, part["filename"])
            if not os.path.exists(part_path):
                raise FileNotFoundError(f"Missing part: {part['filename']}")

            # verify checksum before appending
            md5_actual = md5sum(part_path)
            if md5_actual != part["md5"]:
                raise ValueError(f"Checksum mismatch in {part['filename']}")

            with open(part_path, "rb") as p:
                for chunk in iter(lambda: p.read(1024 * 1024), b""):
                    outfile.write(chunk)
                    pbar.update(len(chunk))

            tqdm.write(f"Merged {part['filename']}")

    print(f"\nMerge complete! File saved to:\n{out_path}")
    return out_path

def main():
    print("────────────────────────────────────────────")
    print("           FILE JOINER UTILITY")
    print("────────────────────────────────────────────\n")

    while True:
        manifest_path = input("Enter the full path to the manifest (.json) file:\n> ").strip('"')
        if os.path.exists(manifest_path) and manifest_path.lower().endswith(".json"):
            break
        print("Manifest file not found. Please try again.\n")
    try:
        join_from_manifest(manifest_path)
    except Exception as e:
        print(f"\nAn error occurred: {e}")

    input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
