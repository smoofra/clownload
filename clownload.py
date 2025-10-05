#!/usr/bin/env python3

import os
import sys
import hashlib
import argparse
from pathlib import Path
import signal
from multiprocessing.pool import Pool, AsyncResult
from typing import NamedTuple, Dict, Tuple, Iterator, Generator
import csv
from datetime import datetime
from contextlib import contextmanager

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import dropbox # type: ignore
from dropbox.files import FileMetadata, ListFolderResult # type: ignore
from dropbox.exceptions import ApiError, AuthError, HttpError # type: ignore

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
        res:ListFolderResult= dbx.files_list_folder(
            root_folder,
            recursive=True,
            include_non_downloadable_files=False
        )  # type: ignore
    except ApiError as e:
        print(f"Error listing folder '{root_folder}': {e}", file=sys.stderr)
        raise

    has_more, cursor = handle_result(res)
    while has_more:
        res:ListFolderResult = dbx.files_list_folder_continue(cursor) # type: ignore
        has_more, cursor = handle_result(res)

    return files

def main():
    parser = argparse.ArgumentParser(
        description="download files from clown computing providers."
    )
    subs = parser.add_subparsers(dest="command")
    calcsums_parser = subs.add_parser("calcsums", help="Calculate checksums sums of local files.")
    CalcsumsMain.setup_args(calcsums_parser)

    dropbox_parser = subs.add_parser("dropbox", help="Download files from Dropbox.")
    dropbox_parser.add_argument(
        "--root",
        default="",
        help="Dropbox folder path to start from (e.g. '' for root, or '/Photos')."
    )
    dropbox_parser.add_argument(
        "--hashes",
        type=Path,
        help="Path to a text file containing known Dropbox content_hash values (one per line)."
    )
    dropbox_parser.add_argument(
        "--dest",
        default="downloaded",
        type=Path,
        help="Local destination directory (default: ./downloaded)."
    )
    dropbox_parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="If set, skip downloading when a local file already exists (no hash check)."
    )

    args = parser.parse_args()

    if args.command == "calcsums":
        CalcsumsMain().main(args)
    elif args.command == "dropbox":
        dropbox_main(args)


class KnownFile(NamedTuple):
    path: str
    dropbox_hash: str
    mtime: datetime # local mtime


class CalcsumsMain:

    absolute: bool
    mount_ok: bool
    known: Dict[str, KnownFile]
    pool: Pool
    sumsfile: str

    @staticmethod
    def _visit(path: str, mtime:datetime) -> KnownFile:
        hash = dropbox_content_hash(path)
        if mtime != datetime.fromtimestamp(os.stat(path).st_mtime):
            raise Exception(f"file changed during hash calculation: {path}")
        return KnownFile(path, hash, mtime)

    def visit(self, path: str) -> AsyncResult[KnownFile] | None:
        if os.path.samefile(path, self.sumsfile):
            return None
        if self.absolute:
            path = os.path.abspath(path)
        if os.path.islink(path) or not os.path.isfile(path):
            return None
        mtime = datetime.fromtimestamp(os.stat(path).st_mtime)
        if known := self.known.get(path):
            if mtime <= known.mtime:
                return None
        return self.pool.apply_async(self._visit, (path, mtime))

    @contextmanager
    def open_sums_file(self, path: str) -> Generator[None]:
        fieldnames = ["path", "dropbox_hash", "mtime"]
        if not os.path.exists(path):
            with open(path, 'w') as f:
                self.writer = csv.writer(f)
                self.writer.writerow(fieldnames)
                self.known = dict()
                yield
        else:
            with open(path, 'r+') as f:
                reader = csv.reader(f)
                rows: Iterator[Tuple[str,str,str]] = iter(reader) # type: ignore
                assert next(rows) == fieldnames
                self.known = {
                    path: KnownFile(
                        path,
                        dropbox_hash,
                        datetime.fromisoformat(mtime))
                    for path,dropbox_hash,mtime in rows}
                self.writer = csv.writer(f)
                yield

    def walk(self) -> Iterator[AsyncResult[KnownFile]|None]:
        for root, dirs, files in os.walk('.'):
            dirs[:] = [dir for dir in dirs
                    if not os.path.islink(os.path.join(root, dir))]
            if not self.mount_ok:
                dirs[:] = [dir for dir in dirs
                        if not os.path.ismount(os.path.join(root, dir))]
            for name in files:
                path = os.path.normpath(os.path.join(root, name))
                yield self.visit(path)

    @staticmethod
    def setup_args(parser: argparse.ArgumentParser):
        parser.add_argument("--mount-ok", action="store_true")
        parser.add_argument("--absolute", "--abs", action="store_true")
        parser.add_argument("--sums", default="sums")
        parser.add_argument("-C", "--chdir")

    def main(self, args) -> None:
        self.absolute = args.absolute
        self.mount_ok = args.mount_ok
        self.sumsfile = args.sums
        if args.chdir:
            os.chdir(args.chdir)

        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGINT, original_sigint_handler)

        with self.open_sums_file(self.sumsfile):
            with Pool() as self.pool:
                try:
                    for result in list(self.walk()):
                        if result:
                            self.writer.writerow(result.get())
                    self.pool.close()
                    self.pool.join()
                except KeyboardInterrupt:
                    signal.signal(signal.SIGINT, signal.SIG_IGN)
                    self.pool.terminate()
                    print()
                    print("interrupted.")
                    sys.exit(1)


def dropbox_main(args):

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