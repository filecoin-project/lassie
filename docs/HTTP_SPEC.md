# Lassie: HTTP Specification

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

- [Introduction](#introduction)
- [Specification](#specification)
    - [`GET /ipfs/{cid}[?params]`](#get-ipfscidparams)


## Introduction

The Lassie HTTP Daemon is an HTTP interface for retrieving IPLD data from IPFS and Filecoin peers. It fetches content over the GraphSync and Bitswap protocols and provides the resulting data in CAR format.

## Specification

### `GET /ipfs/{cid}[?params]`

Retrieves from peers that have the content identified by the given root CID, streaming the DAG in the response in [CAR (v1)](https://ipld.io/specs/transport/car/carv1/) format.

#### Request

##### Headers

- `Accept` - _Optional_. Used to specify the response content type. Optional only if a `format` query parameter is provided, otherwise required.

    If provided, the value must explicitly or implicitly include `application/vnd.ipld.car`.

- `X-Request-Id` - _Optional_. Used to provide a unique request value that can be correlated with a unique retrieval ID in the logs.

##### Path Parameters

- `cid` - _Required_. A valid string representation of the root CID of the DAG being requested.

##### Query Parameters

- `filename` - _Optional_. Used to override the `filename` property of the `Content-Disposition` response header which dictates the default save filename for the response CAR data used by an HTTP client / browser.

    If provided, the filename extension cannot be missing and must be `.car`.

- `format` - _Optional_. `format=<format>` can be used to specify the response content type. This is a URL-friendly alternative to providing an `Accept` header. Optional only if an `Accept` header value is provided, otherwise required.

    If provided, the format value must be `car`. Example: `format=car`.

    `format=car` &rarr; `Accept: application/vnd.ipld.car`

#### Response

#### Status Codes

- `200` - OK

- `400` - Bad Request
    - No acceptable content type provided in the `Accept` header
    - Requested a non-supported format via the `format` query parameter
    - Neither providing a valid `Accept` header or `format` query parameter
    - No extension given in `filename` query parameter
    - Used a non-supported extension in the `filename` query parameter

- `404` - No candidates for the given CID were found

- `500` - Internal Server Error
    - The requested CID path parameter could not be parsed
    - An internal retrieval ID failed to generate
    - The internal blockstore file failed to write

- `504` - Timeout occured while retrieving the given CID

##### Headers

- `Accept-Ranges` - Returns with `none` if the block order in the CAR stream is not deterministic

- `Cache-Control` - Returns with `public, max-age=29030400, immutable`

- `Content-Disposition` - Returns as an attachment, using the given `filename` query parameter if provided, or if no `filename` query parameter is provided, uses the requested CID with a `.car` extension.

    Example: `bafy...foo.car`

- `Content-Type` - Returns with `application/vnd.ipld.car; version=1`

- `Etag` - Returns with the requested CID with the format as a suffix.

    Example: `bafy...foo.car`

- `X-Content-Type-Options` - Returns with `nosniff` to indicate that the `Content-Type` should be followed and not to be changed. This is a security feature, ensures that non-executable binary response types are not used in `<script>` and `<style>` HTML tags.

- `X-Ipfs-Path` - Returns the original, requested content path before any path resolution and traversal is performed.

    Example:  `/ipfs/bafy...foo`

- `X-Trace-ID` - Returns the given `X-Request-Id` header value if provided, otherwise returns an ID that uniquely identifies the retrieval request.