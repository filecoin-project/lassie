# Lassie

> Fetches from Filecoin, every time

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Methods of Retrieval](#methods-of-retrieval)
  - [Command Line Interface](#command-line-interface)
    - [Extracting Content from a CAR](#extracting-content-from-a-car)
    - [Fetch Example](#fetch-example)
  - [HTTP API](#http-api)
    - [Daemon Example](#daemon-example)
  - [Golang Library](#golang-library)
- [Contribute](#contribute)
- [License](#license)

## Overview

Lassie is a simple retrieval client for Filecoin. It finds and fetches your data over the best retrieval protocols available. Lassie makes Filecoin retrieval.

## Installation

Download the [lassie binary form the latest release](https://github.com/filecoin-project/lassie/releases/latest) based on your system architecture, or download and install the [lassie](https://github.com/filecoin-project/lassie) package using the Go package manager:

```bash
$ go install github.com/filecoin-project/lassie/cmd/lassie@latest

go: downloading github.com/filecoin-project/lassie v0.3.1
go: downloading github.com/libp2p/go-libp2p v0.23.2
go: downloading github.com/filecoin-project/go-state-types v0.9.9

...
```

Optionally, download the [go-car binary from the latest release](https://github.com/ipld/go-car/releases/latest) based on your system architecture, or install the [go-car](https://github.com/ipld/go-car) package using the Go package manager:

```bash
$ go install github.com/ipld/go-car/cmd/car@latest

go: downloading github.com/ipld/go-car v0.6.0
go: downloading github.com/ipld/go-car/cmd v0.0.0-20230215023242-a2a8d2f9f60f
go: downloading github.com/ipld/go-codec-dagpb v1.6.0 

...
```

The go-car package makes it easier to work with files in the content-addressed archive (CAR) format, which is what Lassie uses to return the content it fetches. For the lassie use-case, go-car will be used to extract the contents of the CAR into usable files.

## Methods of Retrieval

### Command Line Interface

The lassie command line interface (CLI) is the simplest way to retrieve content from the Filecoin/IPFS network. The CLI is best used when needing to fetch content from the network on an ad-hoc basis. The CLI is also useful for testing and debugging purposes, such as making sure that a CID is retrievable from the network or from a specific provider.

The CLI can be used to retrieve content from the network by passing a CID to the `lassie fetch` command:

```bash
$ lassie fetch [-o <output file>] [-t <timeout>] <CID>[/path/to/content]
```

The `lassie fetch` command will return the content of the CID to a file in the current working directory by the name of `<CID>.car`. If the `-o` output flag is used, the content will be written to the specified file. If the `-t` timeout flag is used, the timeout will be set to the specified value. The default timeout is 20 seconds.

More information about available flags can be found by running `lassie fetch --help`.

#### Extracting Content from a CAR

The go-car package can be used to extract the contents of the CAR file into usable files. For example, if the content of the CID is a video, the go-car package can be used to extract the video into a file on the local filesystem.

```bash
$ car extract -f <CID>.car
```

The `-f` flag is used to specify the CAR file to extract the contents from. The contents of the CAR will be extracted into the current working directory.

#### Fetch Example

Let's grab some content from the Filecoin/IPFS network using the `lassie fetch` command:

```bash
$ lassie fetch -o fetch-example.car -p bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4
```

This will fetch the `bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4` CID from the network and save it to a file named `fetch-example.car` in our current working directory.

The `-p` progress flag is used to get more detailed information about the state of the retrieval.

_Note: If you received a timeout issue, try using the `-t` flag to increase your timeout time to something longer than 20 seconds. Retrievability of some CIDs is highly variable on local network characteristics._

_Note: For the internet cautious out there, the `bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4` CID is a directory that has a video titled `birb.mp4`, which is a video of a bird bouncing to the song "Around the World" by Daft Punk. We've been using it internally during the development of Lassie to test with._

To extract the contents of the `fetch-example.car` file we created in the previous example, we would run:

```bash
$ car extract -f fetch-example.car
```

To fetch and extract at the same time, we can use the `lassie fetch` command and pipe the output to the `car extract` command:

```bash
$ lassie fetch -o - -p bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4 | car extract
```

The `-o` output flag is used with the `-` character to specify that the output should be written to `stdout`. The `car extract` command reads input via `stdin` by default, so the output of the `lassie fetch` command is piped to the `car extract` command.

You should now have a `birb.mp4` file in your current working directory. Feel free to play it with your favorite video player!

### HTTP API

The lassie HTTP API allows one to run a web server that can be used to retrieve content from the Filecoin/IPFS network via HTTP requests. The HTTP API is best used when needing to retrieve content from the network via HTTP requests, whether that be from a browser or a programmatic tool like `curl`. We will be using `curl` for the following examples but know that any HTTP client can be used including a web browser. Curl specific behavior will be noted when applicable.

The API server can be started with the `lassie daemon` command:

```bash
$ lassie daemon

Lassie daemon listening on address 127.0.0.1:41443
Hit CTRL-C to stop the daemon
```

The port can be changed by using the `-p` port flag. Any available port will be used by default.

More information about available flags can be found by running `lassie daemon --help`.

To fetch content using the HTTP API, make a `GET` request to the `/ipfs/<CID>[/path/to/content]` endpoint:

```bash
$ curl http://127.0.0.1:41443/ipfs/<CID>[/path/to/content]
```

By default, this will output the contents of the CID to `stdout`.

To save the output to a file, use the `filename` query parameter:

```bash
$ curl http://127.0.0.1:41443/ipfs/<CID>[/path/to/content]?filename=<filename> --output <filename>
```

_CURL Note: With curl we need to also specify the `--output <filename>` option. However, putting the above URL into a browser will download the file with the given filename parameter value upon a successful fetch._

More information about HTTP API requests and responses, as well as the numerous request parameters that can be used to control fetch behavior on a per request basis, can be found in the [HTTP Specification](./HTTP_SPEC.md) document.

#### Daemon Example

We can start the lassie daemon by running:

```bash
$ lassie daemon

Lassie daemon listening on address 127.0.0.1:41443
Hit CTRL-C to stop the daemon
```

We can now fetch the same content we did in the [CLI example](#fetch-example) by running:

```bash
$ curl http://127.0.0.1:41443/ipfs/bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4?filename=daemon-example.car --output daemon-example.car
```

_CURL Note: With curl we need to also specify the `--output <filename>` option. However, putting the above URL into a browser will download the file with the given filename parameter value upon a successful fetch._

To extract the contents of the `daemon-example.car` file we created in the above example, we would run:

```bash
$ car extract -f daemon-example.car
```

### Golang Library

The lassie library allows one to integrate lassie into their own Go programs. The library is best used when needing to retrieve content from the network programmatically.

The lassie dependency can be added to a project with the following command:

```bash
$ go install github.com/filecoin-project/lassie/cmd/lassie@latest
```

The lassie library can then be imported into a project with the following import statement:

```go
import "github.com/filecoin-project/lassie/pkg/lassie"
```

The following code shows a small example for how to use the lassie library to fetch a CID:

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
)

// main creates a default lassie instance and fetches a CID
func main() {
  ctx := context.Background()

	// Create a default lassie instance
	lassie, err := lassie.NewLassie(ctx)
	if err != nil {
		panic(err)
	}

	// Prepare the fetch
	rootCid := cid.MustParse("bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4") // The CID to fetch
	store := storage.NewDeferredStorageCar(os.TempDir(), rootCid)                           // The place to put the CAR file
	request, err := types.NewRequestForPath(store, rootCid, "", types.DagScopeAll)          // The fetch request
	if err != nil {
		panic(err)
	}

  // Fetch the CID
	stats, err := lassie.Fetch(ctx, request, nil)
	if err != nil {
		panic(err)
	}

	// Print the stats
	fmt.Printf("Fetched %d blocks in %d bytes\n", stats.Blocks, stats.Size)
}
```

Let's break down the above code.

First, we create a default lassie instance:

```go
ctx := context.Background()

// Create a default lassie instance
lassie, err := lassie.NewLassie(ctx)
if err != nil {
  panic(err)
}
```

The `NewLassie` function creates a new lassie instance with default settings, taking a `context.Context`. The context is used to control the lifecycle of the lassie instance. The function returns a `*Lassie` instance and an `error`. The `*Lassie` instance is used to make fetch requests. The `error` is used to indicate if there was an error creating the lassie instance.

Additionally, the `NewLassie` function takes a variable number of `LassieOption`s. These options can be used to customize the lassie instance. For example, the `WithGlobalTimeout` option can be used to set a global timeout for all fetch requests made with the lassie instance. More information about the available options can be found in the [lassie.go](https://pkg.go.dev/github.com/filecoin-project/lassie/pkg/lassie) file.

Next, we prepare the fetch request:

```go
// Prepare the fetch
rootCid := cid.MustParse("bafybeic56z3yccnla3cutmvqsn5zy3g24muupcsjtoyp3pu5pm5amurjx4") // The CID to fetch
store := storage.NewDeferredStorageCar(os.TempDir(), rootCid)                           // The place to put the CAR file
request, err := types.NewRequestForPath(store, rootCid, "", types.DagScopeAll)          // The fetch request
if err != nil {
  panic(err)
}
```

The `rootCid` is the CID we want to fetch. The `store` is where we want to write the car file. In this case we are choosing to store it in the OS's temp directory. The `request` is the resulting fetch request that we'll hand to the `lassie.Fetch` function.

The `request` is created using the `NewRequestForPath` function. The only new information that this function takes that we haven't discussed is the `path` and the `dagScope`. The `path` is an optional path string to a file in the CID being requested. In this case we don't have a path, so pass an empty string. The `dagScope` has to do with traversal and describes the shape of the DAG fetched at the terminus of the specified path whose blocks are included in the returned CAR file after the blocks required to traverse path segments. More information on `dagScope` can be found in the [dag-scope HTTP Specification](./HTTP_SPEC.md#dag-scope-request-query-parameter) section. In this case we use `types.DagScopeAll` to specify we want everything from the root CID onward.

The function returns a `*types.Request` and an `error`. The `*types.Request` is the resulting fetch request we'll pass to `lassie.Fetch`, and the `error` is used to indicate if there was an error creating the fetch request.

Finally, we fetch the CID:

```go
// Fetch the CID
stats, err := lassie.Fetch(ctx, request, nil)
if err != nil {
  panic(err)
}
```

The `Fetch` function takes a `context.Context`, a `*types.Request`, and a `*types.FetchOptions`. The `context.Context` is used to control the lifecycle of the fetch. The `*types.Request` is the fetch request we made above. The `*types.FetchOptions` is used to control the behavior of the fetch. The function returns a `*types.FetchStats` and an `error`. The `*types.FetchStats` is the fetch stats. The `error` is used to indicate if there was an error fetching the CID.

## Contribute

Early days PRs are welcome!

## License

This library is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2022. Protocol Labs, Inc.
