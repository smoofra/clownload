# 🤡 🎪 clownload 🎪 🤡

A tool to download files from clown computers.

## Setup

* `pip install clownload`

* `export DROPBOX_TOKEN=...`.   Create a token [here][apps].


## Usage


### Download from Dropbox
```sh
python clownload.py dropbox --source SOURCE DEST
```

This will download all files from dropbox folder `SOURCE` to local
directory `DEST`.

It will also create a csv file called `DEST/checksums.csv` with the dropbox
content hashes of all the downloaded files.


### Calculate Checksums
```sh
clownload calcsums .
```

## License
[Apache Software License 2.0][license]


[apps]: https://www.dropbox.com/developers/apps
[license]: https://www.apache.org/licenses/LICENSE-2.0.html