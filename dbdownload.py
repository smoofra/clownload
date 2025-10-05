#!/usr/bin/env python3
import os
import sys
import hashlib
import argparse
from pathlib import Path

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import dropbox
from dropbox.files import FileMetadata, ListFolderResult
from dropbox.exceptions import ApiError, AuthError, HttpError

def load_hashes(path: Path | None) -> set[str]:
    if path is None:
        return set()
    with path.open("r") as f:
        def i():
            for line in f:
                line = line.strip()
                if line:
                    yield line
    return set(i())

def dropbox_content_hash(path: str | Path) -> str:
    """
    Compute the Dropbox content_hash of a local file.
    """
    block_size = 0x400000
    block_hashes = []
    with open(path, "rb") as f:
        while True:
            chunk = f.read(block_size)
            if not chunk:
                break
            block_hashes.append(hashlib.sha256(chunk).digest())

    # Concatenate all block hashes, then hash again
    full_hash = hashlib.sha256(b"".join(block_hashes)).hexdigest()
    return full_hash

def ensure_local_path(dest_root: Path, dropbox_path_lower: str) -> Path:
    """
    Map a Dropbox path (lowercase) to a local filesystem path rooted at dest_root.
    """
    rel = dropbox_path_lower.lstrip("/")
    local_path = dest_root / rel
    local_path.parent.mkdir(parents=True, exist_ok=True)
    return local_path

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type((ApiError, HttpError, ConnectionError))
)
def download_file(dbx: dropbox.Dropbox, dropbox_path: str, local_path: Path) -> None:
    """
    Download a single file with basic retry/backoff.
    """
    # Use files_download_to_file to stream efficiently to disk
    dbx.files_download_to_file(str(local_path), dropbox_path)

def list_all_files(dbx: dropbox.Dropbox, root_folder: str) -> list[FileMetadata]:
    """
    Recursively list all files from root_folder (Dropbox path), returning a list of FileMetadata.
    """
    files: list[FileMetadata] = []

    def handle_result(res: ListFolderResult):
        for entry in res.entries:
            if isinstance(entry, FileMetadata):
                files.append(entry)
        return res.has_more, res.cursor

    try:
        res = dbx.files_list_folder(
            root_folder,
            recursive=True,
            include_non_downloadable_files=False
        )
    except ApiError as e:
        print(f"Error listing folder '{root_folder}': {e}", file=sys.stderr)
        raise

    has_more, cursor = handle_result(res)
    while has_more:
        res = dbx.files_list_folder_continue(cursor)
        has_more, cursor = handle_result(res)

    return files

def main():
    parser = argparse.ArgumentParser(
        description="Download all Dropbox files, skipping those with known content hashes."
    )
    parser.add_argument(
        "--root",
        default="",
        help="Dropbox folder path to start from (e.g. '' for root, or '/Photos')."
    )
    parser.add_argument(
        "--hashes",
        type=Path,
        help="Path to a text file containing known Dropbox content_hash values (one per line)."
    )
    parser.add_argument(
        "--dest",
        default="downloaded",
        type=Path,
        help="Local destination directory (default: ./downloaded)."
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="If set, skip downloading when a local file already exists (no hash check)."
    )
    args = parser.parse_args()

    token = os.environ.get("DROPBOX_TOKEN")
    if not token:
        print("ERROR: Please set DROPBOX_TOKEN environment variable with your access token.", file=sys.stderr)
        sys.exit(1)


    try:
        known_hashes = load_hashes(args.hashes)
    except FileNotFoundError:
        print(f"ERROR: Hash file not found: {args.hashes}", file=sys.stderr)
        sys.exit(1)

    dbx = dropbox.Dropbox(token, timeout=120)
    try:
        dbx.users_get_current_account()
    except AuthError:
        print("ERROR: Invalid Dropbox token (authentication failed).", file=sys.stderr)
        sys.exit(1)

    print(f"Listing files under Dropbox path: '{args.root or '/'}' ...")
    files = list_all_files(dbx, args.root)
    print(f"Discovered {len(files)} files.")

    args.dest.mkdir(parents=True, exist_ok=True)

    skipped_known = 0
    skipped_existing = 0
    downloaded = 0
    errors = 0

    for fm in files:
        # fm.content_hash is Dropbox's server-side hash; available without downloading the file
        ch = fm.content_hash
        if ch in known_hashes:
            skipped_known += 1
            print(f"[SKIP hash] {fm.path_display} (content_hash in known list)")
            continue

        local_path = ensure_local_path(args.dest, fm.path_lower)

        if args.skip_existing and local_path.exists():
            skipped_existing += 1
            print(f"[SKIP exists] {fm.path_display} -> {local_path}")
            continue

        try:
            download_file(dbx, fm.path_lower, local_path)
            downloaded += 1
            print(f"[DOWNLOADED] {fm.path_display} -> {local_path}")
        except Exception as e:
            errors += 1
            print(f"[ERROR] {fm.path_display}: {e}", file=sys.stderr)

    print("\nSummary")
    print("-------")
    print(f"Downloaded:      {downloaded}")
    print(f"Skipped (hash):  {skipped_known}")
    print(f"Skipped (exists):{skipped_existing}")
    print(f"Errors:          {errors}")

if __name__ == "__main__":
    main()