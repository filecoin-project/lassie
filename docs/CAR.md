# Lassie: Returned CAR Specification

![wip](https://img.shields.io/badge/status-wip-orange.svg?style=flat-square)

**Author(s)**:

- [Hannah Howard](https://github.com/hannahhoward)
- [Kyle Huntsman](https://github.com/kylehuntsman)
- [Rod Vagg](https://github.com/rvagg)

**Maintainer(s)**:

- [Hannah Howard](https://github.com/hannahhoward)
- [Kyle Huntsman](https://github.com/kylehuntsman)
- [Rod Vagg](https://github.com/rvagg)

* * *

## Table of Contents

- [Table of Contents](#table-of-contents)
- [Introduction](#introduction)
- [Specification](#specification)
  - [Root CIDs](#root-cids)
  - [Verifiability](#verifiability)
  - [Block ordering](#block-ordering)
  - [Block deduplication](#block-deduplication)
  - [Identity CIDs](#identity-cids)
  - [DAG depth](#dag-depth)

## Introduction

Under normal operation, Lassie only returns IPLD data in CAR format. Specifically, the [CARv1](https://ipld.io/specs/transport/car/carv1/) format. This document describes the nature of the CAR data returned by Lassie and the various options available to the client for varying the data included.

## Specification

### Root CIDs

CARv1 allows for multiple roots to be included in a single CAR. Lassie will always return a CAR with a **single root CID**. This root CID will be the CID of the IPLD block that was requested by the client, even when an additional `path` is supplied.

### Verifiability

Lassie will always return a CAR that is **verifiable**. This means that the CAR will contain all blocks required to verify the root CID and any additional path traversals to the requested content. It should always be possible to verify the root block content against the requested CID, and every block that is traversed to reach the content as specified by the final `path`.

### Block ordering

Lassie will always return a CAR with a stable **block ordering** based on the traversal required to (a) navigate from the root CID to the final content where a `path` is provided, and (b) fetch the required DAG or partial DAG at the termination of that `path`, or from the root block where no `path` is provided. Blocks will be provided as they are encountered during a traversal, which works from "leftmost" link within a block/node to the "rightmost". The precise ordering will depend on how the data has been encoded, although this ordering is stable where standard (and properly implemented) codecs were used to generate the data. See https://ipld.io/specs/codecs/ for more information on IPLD codecs and block ordering, specifically see [DAG-PB Link Sorting](http://ipld-io.ipns.localhost:48084/specs/codecs/dag-pb/spec/#link-sorting) which defines how UnixFS directory listings are ordered.

### Block deduplication

Where blocks are referenced multiple times within a DAG, it will only be included once, when it is encountered first during the traversal.

### Identity CIDs

Identity CIDs will not be present as separate blocks within a CAR returned by Lassie.

### DAG depth

At the termination of a provided `path`, or from the root CID where no `path` is provided, the depth of the DAG returned may vary depending on the request or available data.

A "shallow" (e.g. when using `depthType=shallow` with the (HTTP)[HTTP_SPEC.md] server) traversal will include only the content at the termination of the `path` specifier, as well as all blocks from the root CID to the `path` terminus where a `path` is provided, as with a "full" depth DAG. If the content is found to be UnixFS data, the entire UnixFS entity will be included. i.e. if the terminus is a sharded UnixFS file, or a sharded UnixFS directory, the blocks required to reconsititute the entire file, or directory will be included. If the termination is a UnixFS sharded directory, only the full directory will be included, not the full DAG of the directory's contents.

A "full" (e.g. when using `depthType=full` with the (HTTP)[HTTP_SPEC.md] server) traversal will include only the content at the termination of the `path` specifier, as well as all blocks from the root CID to the `path` terminus where a `path` is provided, as with a "shallow" depth DAG. At the termination of a `path`, or from the root CID where no `path` is provided, the entire DAG from that point on will be included.

Under certain conditions, a complete DAG may not be returned.

 * When a Filecoin storage provider chosen to retrieve from does not have the full DAG, only the portion that they do have will be included.
 * When a Bitswap session is unable to exhaustively discover all blocks in a DAG, only the portion that was discovered will be included.
