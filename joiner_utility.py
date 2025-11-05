import os
import json
import hashlib
import argparse
from tqdm import tqdm

def md5sum(path, block_size=8192):
    """Compute MD5 checksum for a file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()

def join_from_manifest(manifest_path, output_dir=None):
    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    base_dir = os.path.dirname(manifest_path)
    if output_dir is None:
        output_dir = base_dir

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, manifest["original_filename"])
    total_size = sum(p["size"] for p in manifest["parts"])

    print(f"\nReassembling '{manifest['original_filename']}' from {manifest['total_parts']} parts...\n")

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
            md5_actual = md5sum(part_path)
            if md5_actual != part["md5"]:
                raise ValueError(
                    f"Checksum mismatch in {part['filename']} "
                    f"(expected {part['md5']}, got {md5_actual})"
                )
            with open(part_path, "rb") as p:
                for chunk in iter(lambda: p.read(1024 * 1024), b""):
                    outfile.write(chunk)
                    pbar.update(len(chunk))
            tqdm.write(f"Merged {part['filename']}")

    print(f"\n🎉 Merge complete! File saved to:\n{out_path}")
    return out_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reassemble split files from manifest")
    parser.add_argument("manifest", help="Path to manifest JSON file")
    parser.add_argument(
        "-o", "--output", help="Output directory (default: same folder as manifest)", default=None
    )
    args = parser.parse_args()

    join_from_manifest(args.manifest, args.output)