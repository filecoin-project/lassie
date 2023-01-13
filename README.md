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

### Using

The simplest way to use lassie it to just run the CLI, which currently has only one command:

```
lassie fetch <CID>
```

This will output to a CAR file with the name of the CID in the current directory.

## Contribute

Early days PRs are welcome!

## License

This library is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2022. Protocol Labs, Inc.
