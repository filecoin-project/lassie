# Lassie: HTTP Specification

![wip](https://img.shields.io/badge/status-wip-orange.svg?style=flat-square)

**Editor(s)**:

- [Hannah Howard](https://github.com/hannahhoward)
- [Kyle Huntsman](https://github.com/kylehuntsman)
- [Rod Vagg](https://github.com/rvagg)

**Maintainer(s)**:

- [Hannah Howard](https://github.com/hannahhoward)
- [Kyle Huntsman](https://github.com/kylehuntsman)
- [Rod Vagg](https://github.com/rvagg)

The Lassie HTTP specification is an HTTP interface for retrieving IPLD data from IPFS and Filecoin peers. It fetches content over the HTTP protocol and provides the resulting data in CAR format.

An implementation of the [Trustless Gateway](https://specs.ipfs.tech/http-gateways/trustless-gateway) specification with a subset of the [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/) specification that focuses on returning verifiable CAR formatted data.

* * *

## Table of Contents

- [HTTP API](#http-api)
    - [`GET /ipfs/{cid}[?params]`](#get-ipfscidparams)
- [HTTP Request](#http-request)
    - [Request Headers](#request-headers)
        - [`Accept` (request header)](#accept-request-header)
            - [`version` (CAR content type parameter)](#version-car-content-type-parameter)
            - [`dups` (CAR content type parameter)](#dups-car-content-type-parameter)
            - [`order` (CAR content type parameter)](#order-car-content-type-parameter)
        - [`X-Request-Id` (request header)](#x-request-id-request-header)
    - [Request Query Parameters](#request-query-parameters)
        - [`filename` (request query parameter)](#filename-request-query-parameter)
        - [`format` (request query parameter)](#format-request-query-parameter)
        - [`dag-scope` (request query parameter)](#dag-scope-request-query-parameter)
        - [`protocols` (request query parameter)](#protocols-request-query-parameter)
        - [`providers` (request query parameter)](#providers-request-query-parameter)
- [HTTP Response](#http-response)
    - [Response Status Codes](#response-status-codes)
        - [`200` OK](#200-ok)
        - [`400` Bad Request](#400-bad-request)
        - [`404` Not Found](#404-not-found)
        - [`405` Method Not Allowed](#405-method-not-allowed)
        - [`500` Internal Server Error](#500-internal-server-error)
        - [`504` Gateway Timeout](#504-gateway-timeout)
    - [Response Headers](#response-headers)
        - [`Accept-Ranges` (response header)](#accept-ranges-response-header)
        - [`Cache-Control` (response header)](#cache-control-response-header)
        - [`Content-Disposition` (response header)](#content-disposition-response-header)
        - [`Content-Type` (response header)](#content-type-response-header)
        - [`Etag` (response header)](#etag-response-header)
        - [`X-Content-Type-Options` (response header)](#x-content-type-options-response-header)
        - [`X-Ipfs-Path` (response header)](#x-ipfs-path-response-header)
        - [`X-Trace-Id` (response header)](#x-trace-id-response-header)
    - [Response Payload](#response-payload)


# HTTP API

Same as [Trustless Gateway](https://specs.ipfs.tech/http-gateways/trustless-gateway/#http-api), but without the HEAD requests and `/ipns/` namespace support.

## `GET /ipfs/{cid}[/path][?params]`

Retrieves from peers that have the content identified by the given root CID, streaming the DAG in the response in [CAR (v1)](https://ipld.io/specs/transport/car/carv1/) format.

- `cid`: _REQUIRED_. A valid string representation of the root CID of the DAG being requested.

- `path`: _OPTIONAL_. A valid IPLD path to traverse within the DAG to the final content.

    The path must begin with a `/`, and must describe a valid path within the DAG. The path will be resolved as a [UnixFS](https://github.com/ipfs/specs/blob/main/UNIXFS.md) path where the encountered path segments are within valid UnixFS blocks and can be read as named links. Where the blocks do not describe valid UnixFS data, the path segment(s) will be interpreted as describing plain IPLD nodes to traverse.
    
    All blocks from the root `cid` to the final content via the provided path will be returned, allowing for a verifiable CAR. The entire DAG will also be returned from the point where the path terminates. This behavior can be modified with the `depthType` query parameter.

    Example:
    - `/ipfs/bafy...foo/bar/baz`, where `bafy...foo` is the CID and `/bar/baz` is a path.  

- `params`: _OPTIONAL_. Query parameters that adjust response behavior. See [HTTP Query Parameters](#request-query-parameters) for more information.

# HTTP Request

Same as [Trustless Gateway](https://specs.ipfs.tech/http-gateways/trustless-gateway/#http-request), but only supporting a single media type in the Accept header and some additional media type parameters from an open proposal [IPIP-412](https://github.com/ipfs/specs/pull/412).

## Request Headers

### `Accept` (request header)

Same as [Trustless Gateway](https://specs.ipfs.tech/http-gateways/trustless-gateway/#accept-request-header), but _SHALL NOT_ accept any other media types specified other than `application/vnd.ipld.car`.

Used to specify the response content type. _OPTIONAL_ only if a `format` query parameter is provided, otherwise this header is _REQUIRED_.

If provided, the value MUST explicitly or implicitly include the following media type:
- `application/vnd.ipld.car`

#### `version` (CAR content type parameter)

See [application/vnd.ipld.car](https://www.iana.org/assignments/media-types/application/vnd.ipld.car).

_OPTIONAL_. `version=1`. Defaults to `1`.

Used to specify the version of the CAR media type to respond with. Values other than `1` will respond with a 400 status code.

- `1`: Version 1 of the CAR media type.

Example:
- `application/vnd.ipld.car;version=1;` requests version 1 of the CAR media type

#### `dups` (CAR content type parameter)

Specified in [IPIP-412](https://github.com/ipfs/specs/blob/746cb5206e2346f8e4a37219898a735a498c3186/src/ipips/ipip-0412.md#order-car-content-type-parameter).

_OPTIONAL_. `dups=<y|n>`. Defaults to `y`.

Used to specify whether or not the response may include duplicate blocks in the CAR response where they exist within the DAG. Unspecified values will respond with a 400 status code.

- `y`: Include duplicate blocks in the response where they are encountered in a depth-first traversal of the DAG
- `n`: Strictly do not include duplicate blocks in the response

Examples:
- `application/vnd.ipld.car;dups=y;` includes duplicate blocks
- `application/vnd.ipld.car;dups=n;` does not include duplicate blocks

#### `order` (CAR content type parameter)

Specified in [IPIP-412](https://github.com/ipfs/specs/blob/746cb5206e2346f8e4a37219898a735a498c3186/src/ipips/ipip-0412.md#order-car-content-type-parameter), but Lassie will only ever produce a `dfs` ordered CAR response.

_OPTIONAL_. `order=<dfs|unk>`. Defaults to `dfs`.

Used to specify preference for a specific block order in the CAR response. Unspecified values will respond with a 400 status code.

- `dfs`: [Depth-First Search](https://en.wikipedia.org/wiki/Depth-first_search) order.
- `unk`: Unknown order. Although this option is acceptable, Lassie will still produce depth-first ordering regardless.

Depth-first traversal enables efficient incremental verifiability of content, as the consumption of the DAG can execute the same traversal on the data as it is received, verifying the content is exactly what is expected and rejecting the content as soon as a discrepancy occurs rather than waiting to consume the entire DAG before being able to reconstruct the ordered content.

When combined with `dups=y`, a depth-first traversal enables streaming responses, obviating the need to cache blocks in case they reappear later in a traversal.

Examples:
- `application/vnd.ipld.car;order=dfs;` will respond with a depth-first search ordered CAR
- `application/vnd.ipld.car;order=unk;` will respond with a depth-first search ordered CAR
    
### `X-Request-Id` (request header)

_OPTIONAL_. Used to provide a unique request ID that can be correlated in logs, via downstream requests and in the `X-Trace-Id` response header. When not present a UUIDv4 is generated for the request. Where a retrieval is attempted from a compatible HTTP Trustless Gateway candidate, this parameter is passed on. This value can be used to create a cross-system request traceability chain.

## Request Query Parameters

### `filename` (request query parameter)

Same as [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#filename-request-query-parameter), but only supports `.car` extensions.

_OPTIONAL_. Used to override the downloaded filename. If provided, it will be included in the `Content-Disposition` header.

If provided, the filename extension is _REQUIRED_ and _MUST_ be `.car`. A missing extension, or any other unspecified extension, will respond with a 400 status code.

Example:
- `filename=my-file.car` will download a CAR with the name filename `my-file.car`

### `format` (request query parameter)

Same as [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#format-request-query-parameter), but only supports the `car` format.

_OPTIONAL_. Used to specify the response content type.

This is a URL-friendly alternative to providing an [`Accept`](#accept-request-header) header. _OPTIONAL_ only if an `Accept` header value is provided, otherwise this parameter is _REQUIRED_. Unlike the [`Accept`](#accept-request-header), there is no way to specify media type parameter values. Default media type parameter values will be applied.

If provided, the value _MUST_ be `car`. Any other value will respond with a 400 status code.

Example:
- `format=car` &rarr; `Accept: application/vnd.ipld.car`

### `dag-scope` (request query parameter)

Specified in [IPIP-402](https://github.com/ipfs/specs/pull/402).

_OPTIONAL_. `dag-scope=<block|entity|all>`. Defaults to `all`.

Describes the shape of the DAG fetched at the terminus of the specified path whose blocks are included in the returned CAR file after the blocks required to traverse path segments. Unspecified values will respond with a 400 status code.

- `block`: Only the root block at the end of the path is returned after blocks are required to verify the specified path segments.

- `entity`: Returns only the content at the termination of the `{cid}[/path]` specifier, as well as all blocks from the `cid` to the `path` terminus where a `path` is provided. If the content is found to be UnixFS data, the entire UnixFS entity will be included. i.e. if `{cid}[/path]` terminates at a sharded UnixFS file, the blocks required to reconstitute the entire file will be included. If the termination is a UnixFS sharded directory, only the full directory structure itself will be included, not the full DAG of the directory's contents.

- `all`: Transmit the entire contiguous DAG that begins at the end of the path query, after blocks required to verify path segments.

Examples:
- `dag-scope=block` 
- `dag-scope=entity`
- `dag-scope=all`

### `protocols` (request query parameter)

_OPTIONAL_. `protocols=<http>`. Defaults to HTTP.

Used to specify the retrieval protocol to use. Unrecognized protocols will respond with a 400 status code.

The `protocols` query parameter is a Lassie specific query parameter and is not part of the [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/) specification.

Examples:
- `protocols=http` will attempt retrievals with the HTTP protocol

### `providers` (request query parameter)

_OPTIONAL_. `providers=<addr1, addr2, ...>`. Defaults to any providers returned by [IPNI](https://github.com/ipni/specs/blob/main/IPNI.md).

Used to specify the providers to retrieve from, delimited by a comma, via their peer [multiaddr](https://github.com/multiformats/multiaddr), including peer ID. Invalid provider multiaddrs will respond with a 400 status code.

The `providers` query parameter is a Lassie specific query parameter and is not part of the [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/) specification.

Examples:
- `providers=/ip4/1.2.3.4/tcp/1234/tls/p2p/QmFoo,/dns4/example.com/tcp/1234/tls/p2p/QmFoo` will only attempt to retrieve from these providers

### `blockLimit` (request query parameter)

_OPTIONAL_. `blockLimit=<limit>`. Defaults to `0`, or _infinite_ blocks.

Used to specify the maximum number of blocks to retrieve. Limit should be an unsigned 64-bit integer. A value of `0` translates to _infinite_ blocks.

The `blockLimit` query parameter is a Lassie specific query parameter and is not part of the [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/) specification.

Examples:
- `blockLimit=10` will only retrieve ten blocks

# HTTP Response

## Response Status Codes

### `200` OK

The request succeeded.

### `400` Bad Request

The request was invalid. Possible reasons include:

- No acceptable content type provided in the `Accept` header
    - Provided an invalid value for the `version` CAR content type parameter
    - Provided an invalid value for the `dups` CAR content type parameter
    - Provided an invalid value for the `order` CAR content type parameter
- Requested a non-supported format via the `format` query parameter
- Neither providing a valid `Accept` header or `format` query parameter
- No extension given in the `filename` query parameter
- Used a non-supported extension in the `filename` query parameter
- Provided an invalid value for the `dag-scope` query parameter
- Provided an unrecognized protocol in the `protocols` query parameter
- Provided an invalid provider peer ID in the `providers` query parameter

### `404` Not Found

The request was correct, but the content being requested could not be found because there were no candidates advertising that content.

### `405` Method Not Allowed

A request method other than those specified in [HTTP API](#http-api) were used.

### `500` Internal Server Error

Something went wrong with the application.

### `504` Gateway Timeout

A timeout occurred while retrieving the given CID.

## Response Headers

### `Accept-Ranges` (response header)

Same as [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#accept-ranges-response-header), but only ever returns with `none` as range requests are not currently supported.

### `Cache-Control` (response header)

Same as [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#cache-control-response-header), but only ever returns `immutable` since this API only supports the `/ipfs/` namespace.

- `Cache-Control: public, max-age=29030400, immutable`

### `Content-Disposition` (response header)

Same as [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#content-disposition-response-header), but only ever returns as an `attachment`, using the given `filename` query parameter if provided, or if no `filename` query parameter is provided, uses the requested CID with a `.car` extension.

- `Content-Disposition: attachment; filename=bafy...foo.car`

### `Content-Type` (response header)

Same as [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#content-type-response-header), but only ever returns the CAR content type.

- `Content-Type: application/vnd.ipld.car; version=1`

### `Etag` (response header)

Same as [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#etag-response-header), but returns a quoted string of the format `"<cid>.car.<hash>"`, with hash being a 32-bit string based on the elements of the request that determine the uniqueness of the response; including the CID, path, `dag-scope` query parameter and `dups` specifier in the `Accept` header.

- `Etag: "bafy...foo.car.abc123"`

### `X-Content-Type-Options` (response header)

Same as [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#x-content-type-options-response-header), but only ever returns `nosniff`.

- `X-Content-Type-Options: nosniff`

### `X-Ipfs-Path` (response header)

Same as [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#x-ipfs-path-response-header).

- `X-Ipfs-Path: /ipfs/bafy...foo`

### `X-Trace-Id` (response header)

Same as [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#x-trace-id-response-header).

Returns the given `X-Request-Id` header value if provided, otherwise returns an ID that uniquely identifies the retrieval request.

## Response Payload

The payload is a small subset of the [Path Gateway](https://specs.ipfs.tech/http-gateways/path-gateway/#response-payload) specification in that it only ever returns an arbitrary DAG as a verifiable CAR stream, see [application/vnd.ipld.car](https://www.iana.org/assignments/media-types/application/vnd.ipld.car).