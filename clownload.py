#!/usr/bin/env python3

import os
import sys
import hashlib
import argparse
from pathlib import Path
import signal
from multiprocessing.pool import Pool, AsyncResult
from typing import NamedTuple, Dict, Tuple, Iterator, Generator, Set
import csv
from datetime import datetime
import logging
from contextlib import contextmanager
import shutil
import shlex
import itertools

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import dropbox  # type: ignore
from dropbox import Dropbox
from dropbox.files import FileMetadata, ListFolderResult  # type: ignore
from dropbox.exceptions import ApiError, HttpError  # type: ignore

log = logging.getLogger("clownload")


class UserError(Exception):
    pass


def q(s) -> str:
    return shlex.quote(str(s))


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


class File(NamedTuple):
    path: str
    dropbox_hash: str
    mtime: datetime  # local mtime


class SumsFile:

    def __init__(self, path: Path | str, mode: str):
        self.path = path
        self.mode = mode
        assert self.mode in ("r", "r+", "w")
        if self.mode == "r+" and not os.path.exists(self.path):
            self.mode = "w"

    def put(self, row: File) -> None:
        self.known[row.path] = row
        self.writer.writerow(row)

    def get(self, path: str) -> File | None:
        return self.known.get(path)

    def __enter__(self) -> "SumsFile":
        self.context = self.open()
        return self.context.__enter__()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.context.__exit__(exc_type, exc_value, traceback)

    def __contains__(self, path: str) -> bool:
        return path in self.known

    @contextmanager
    def open(self) -> Generator["SumsFile", None, None]:
        fieldnames = ["path", "dropbox_hash", "mtime"]
        if self.mode == "w":
            with open(self.path, "w", newline="") as f:
                self.writer = csv.writer(f)
                self.writer.writerow(fieldnames)
                self.known = dict()
                yield self
        else:
            with open(self.path, self.mode, newline="") as f:
                reader = csv.reader(f)
                rows: Iterator[Tuple[str, str, str]] = iter(reader)  # type: ignore
                assert next(rows) == fieldnames
                self.known = {
                    path: File(path, dropbox_hash, datetime.fromisoformat(mtime))
                    for path, dropbox_hash, mtime in rows
                }
                if self.mode == "r+":
                    self.writer = csv.writer(f)
                yield self


class CalcsumsMain:

    known: Dict[str, File]
    pool: Pool
    sumsfile: SumsFile

    @staticmethod
    def _visit(path: str, mtime: datetime) -> File:
        hash = dropbox_content_hash(path)
        if mtime != datetime.fromtimestamp(os.stat(path).st_mtime):
            raise Exception(f"file changed during hash calculation: {path}")
        return File(path, hash, mtime)

    def visit(self, path: str) -> AsyncResult[File] | None:
        if os.path.samefile(path, self.sumsfile.path):
            return None
        if os.path.islink(path) or not os.path.isfile(path):
            return None
        mtime = datetime.fromtimestamp(os.stat(path).st_mtime)
        if known := self.sumsfile.get(path):
            if mtime <= known.mtime:
                return None
        log.info(f"Hashing {path}")
        return self.pool.apply_async(self._visit, (path, mtime))

    def walk(self) -> Iterator[AsyncResult[File] | None]:
        for root, dirs, files in os.walk("."):
            dirs[:] = [dir for dir in dirs if not os.path.islink(os.path.join(root, dir))]
            for name in files:
                path = os.path.normpath(os.path.join(root, name))
                yield self.visit(path)

    @staticmethod
    def setup_args(parser: argparse.ArgumentParser):
        parser.add_argument("--sums", default="checksums.csv", help="filename for checksums")
        parser.add_argument("directory")

    def main(self, args) -> None:
        if args.directory:
            os.chdir(args.directory)

        original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGINT, original_sigint_handler)

        with SumsFile(args.sums, "r+") as self.sumsfile:
            with Pool() as self.pool:
                try:
                    for result in list(self.walk()):
                        if result:
                            self.sumsfile.put(result.get())
                    self.pool.close()
                    self.pool.join()
                except KeyboardInterrupt:
                    signal.signal(signal.SIGINT, signal.SIG_IGN)
                    self.pool.terminate()
                    print()
                    print("interrupted.")
                    sys.exit(1)


class DropboxMain:

    source: str
    dest: Path
    known: Set[str]
    sums: SumsFile
    dropbox: Dropbox

    @staticmethod
    def setup_args(parser: argparse.ArgumentParser):
        parser.add_argument(
            "--sums",
            default="checksums.csv",
            help="filename for checksums",
            metavar="CHECKSUMS.CSV",
        )
        parser.add_argument(
            "--source",
            default="",
            metavar="DROPBOX_PATH",
            help="dropbox folder to download",
        )
        parser.add_argument(
            "--known",
            "-k",
            type=Path,
            action="append",
            default=list(),
            metavar="KNOWN.CSV",
            help="additional csv file containing hashes of known files to skip",
        )
        parser.add_argument(
            "dest",
            default=".",
            type=Path,
            nargs="?",
            metavar="LOCAL_PATH",
            help="local destination directory",
        )

    def main(self, args) -> None:
        self.source = args.source
        self.dest = args.dest
        self.known = set()
        for path in args.known:
            with SumsFile(path, "r") as f:
                self.known.update(f.dropbox_hash for f in f.known.values())

        token = os.environ.get("DROPBOX_TOKEN")
        if not token:
            raise UserError(
                "ERROR: set $DROPBOX_TOKEN. see: https://www.dropbox.com/developers/apps"
            )

        args.dest.mkdir(parents=True, exist_ok=True)

        with SumsFile(self.dest / args.sums, "r+") as self.sumsfile:
            with dropbox.Dropbox(token, timeout=120) as self.dropbox:
                self.dropbox.users_get_current_account()
                files = list(self.list_files(self.source))
                for fm in files:
                    self.download_file(fm)

    def list_files(self, dropbox_path: str) -> Iterator[FileMetadata]:
        "Recursively list all files dropbox_path."

        def results() -> Iterator[ListFolderResult]:
            result: ListFolderResult
            result = self.dropbox.files_list_folder(
                dropbox_path, recursive=True, include_non_downloadable_files=False
            )  # type: ignore
            yield result
            while result.has_more:
                result = self.dropbox.files_list_folder_continue(result.cursor)  # type: ignore
                yield result

        for result in results():
            for entry in result.entries:
                if isinstance(entry, FileMetadata):
                    log.info(f"Found file: {q(entry.path_lower)}")
                    yield entry

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        retry=retry_if_exception_type((ApiError, HttpError, ConnectionError)),
    )
    def download_file(self, fm: FileMetadata) -> None:
        if fm.content_hash in self.known:
            log.info("Skipping known file: %s", fm.path_lower)
            return

        local_path = self.dest / fm.path_lower.lstrip("/")
        local_path.parent.mkdir(parents=True, exist_ok=True)

        row = self.sumsfile.get(str(local_path))

        if os.path.exists(local_path):
            mtime = datetime.fromtimestamp(os.stat(local_path).st_mtime)
            if not row or mtime > row.mtime:
                # hash from csv is out of date, update it
                row = File(str(local_path), dropbox_content_hash(local_path), mtime)
                if mtime != datetime.fromtimestamp(os.stat(local_path).st_mtime):
                    raise Exception(f"file changed during hash calculation: {local_path}")
                self.sumsfile.put(row)

        if row:
            if row.dropbox_hash == fm.content_hash:
                log.info(
                    f"Skipping already downloaded file: {q(fm.path_lower)} at {q(local_path)}"
                )
                return

            # file changed, move old file out of the way
            for n in itertools.count():
                old_path = local_path.parent / f"{local_path.name}.old.{n}"
                if not old_path.exists():
                    log.info(f"Moving changed file {q(local_path)} to {q(old_path)}")
                    shutil.move(local_path, old_path)
                    self.sumsfile.put(File(str(old_path), row.dropbox_hash, row.mtime))
                    break

        log.info(f"Downloading {q(fm.path_lower)} to {q(local_path)}")
        self.dropbox.files_download_to_file(local_path, fm.path_lower)
        row = File(
            str(local_path), fm.content_hash, datetime.fromtimestamp(os.stat(local_path).st_mtime)
        )
        self.sumsfile.put(row)


def main():
    parser = argparse.ArgumentParser(description="🤡 Download files from clown computers. 🤡")
    parser.add_argument("--verbose", "-v", action="count", default=0)
    subs = parser.add_subparsers(dest="command", required=True)
    calcsums_parser = subs.add_parser("calcsums", help="Calculate checksums of local files")
    dropbox_parser = subs.add_parser("dropbox", help="Download files from Dropbox")
    CalcsumsMain.setup_args(calcsums_parser)
    DropboxMain.setup_args(dropbox_parser)
    args = parser.parse_args()
    if args.verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose >= 1:
        log.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(handler)

    if args.command == "calcsums":
        CalcsumsMain().main(args)
    elif args.command == "dropbox":
        DropboxMain().main(args)


if __name__ == "__main__":
    try:
        main()
    except UserError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
