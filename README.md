# Lassie

> Fetches from Filecoin, every time

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Overview

Lassie is a simple retrieval client for Filecoin. It finds and fetches your data over the best retrieval protocols available. Lassie makes Filecoin retrieval.

## Install

Unless you are developer, the simplest way to install Lassie is to download a binary for your machine from the [Lassie releases page](https://github.com/filecoin-project/lassie/releases)

Alternatively, you can clone the repository and build from source by running:

```
go build ./cmd/lassie
```

## Usage

### Fetch Command

The simplest way to use lassie it to just run the CLI with the `fetch` command:

```
lassie fetch <CID>
```

This will output to a CAR file with the name of the CID in the current directory.

For additional command options and parameters, use the `--help, -h` CLI option.

### HTTP Daemon Command

An HTTP server to fetch content over HTTP can also be started via the `daemon` command.

```
$ lassie daemon
```

The [HTTP specification](./docs/HTTP_SPEC.md) doc outlines the request and response formats.

For additional command options and parameters, use the `--help, -h` CLI option.

## Contribute

Early days PRs are welcome!

## License

This library is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2022. Protocol Labs, Inc.
