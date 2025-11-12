import os
import json
import hashlib
import argparse
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
    """Parse human-readable sizes like '500M' or '2G'."""
    size_str = size_str.strip().upper()
    if size_str.endswith("G"):
        return int(float(size_str[:-1]) * 1024**3)
    elif size_str.endswith("M"):
        return int(float(size_str[:-1]) * 1024**2)
    elif size_str.endswith("K"):
        return int(float(size_str[:-1]) * 1024)
    else:
        return int(size_str)  # assume bytes

def split_file(file_path, output_dir=None, chunk_size=DEFAULT_CHUNK_SIZE):
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be > 0, got {chunk_size}")

    base_name = os.path.basename(file_path)
    total_size = os.path.getsize(file_path)

    if output_dir is None or output_dir.strip() == "":
        parent_dir = os.path.abspath(os.path.dirname(file_path) or ".")
        output_dir = os.path.join(parent_dir, f"{base_name}_split")

    os.makedirs(output_dir, exist_ok=True)
    print(f"[info] Writing parts to: {output_dir}")

    parts = []
    print(f"\nSplitting '{base_name}' ({total_size / (1024**3):.2f} GB)...\n")

    WRITE_BLOCK = 8 * 1024 * 1024
    with open(file_path, "rb") as infile, tqdm(
        total=total_size, unit="B", unit_scale=True, desc="Overall progress", ncols=80, leave=True
    ) as pbar:
        idx = 1
        bytes_left = total_size
        while bytes_left > 0:
            to_write = min(chunk_size, bytes_left)
            part_name = f"{base_name}.part{idx:03d}"
            part_path = os.path.join(output_dir, part_name)

            md5 = hashlib.md5()
            written = 0
            with open(part_path, "wb") as outfile:
                while written < to_write:
                    block = min(WRITE_BLOCK, to_write - written)
                    buf = infile.read(block)
                    if not buf:
                        break
                    outfile.write(buf)
                    md5.update(buf)
                    written += len(buf)
                    pbar.update(len(buf))

            parts.append({"filename": part_name, "size": written, "md5": md5.hexdigest()})
            tqdm.write(f"Wrote {part_name} ({written / (1024**2):.1f} MB)")
            idx += 1
            bytes_left -= written

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split large files into chunks with manifest")
    parser.add_argument("file", help="Path to the file to split")
    parser.add_argument(
        "-o", 
        "--output",
        help="Output directory (default: {filename}_split next to the file)",
        default=None
    )

    parser.add_argument(
        "-s",
        "--chunk-size",
        help="Chunk size (e.g. 500M, 2G, 1048576000 bytes)",
        default="1G",
    )
    args = parser.parse_args()

    split_file(args.file, args.output, parse_size(args.chunk_size))
