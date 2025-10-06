# 🤡 🎪 clownload 🎪 🤡

> "All our data is stored in the Clown."
>
> "Unlock the power of the Clown."
>
> "Clown-native. Infintely elastic.  Enterprise-grade."
>
> "Juggling As A Service"

A tool to download files from clown computers.

## Setup

* `pip install clownload`

* `export DROPBOX_TOKEN=...`.   Create a token [here][apps].


## Usage


### Download from Dropbox
```sh
clownload dropbox --source SOURCE DEST
```

This will download all files from dropbox folder `SOURCE` to local
directory `DEST`.

It will also create a csv file called `DEST/checksums.csv` with the dropbox
content hashes of all the downloaded files.

Local files that have already been downloaded will be skipped.

Any existing files under `DEST` whose name conflicts will be moved aside to
names like `file.old.1` and `file.old.2`.

Additional csv files with known hashes to skip can be specified with `--known`.

### Calculate Checksums
```sh
clownload calcsums .
```

Calculate content hashes of local files and save them to `checksums.csv`.

## License
[Apache Software License 2.0][license]


[apps]: https://www.dropbox.com/developers/apps
[license]: https://www.apache.org/licenses/LICENSE-2.0.html