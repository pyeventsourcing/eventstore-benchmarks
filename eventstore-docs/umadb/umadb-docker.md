---
head:
  - - meta
    - name: description
      content: How to get started with UmaDB Docker containers
  - - meta
    - name: keywords
      content: UmaDB, Docker, download, downloads, linux, macOS 
---
# Docker Containers

## Multi-Platform Images

UmaDB publishes multi-platform Docker images for `linux/amd64` and `linux/arm64`.

UmaDB Docker images are available from:
* [Docker Hub](https://hub.docker.com/r/umadb/umadb)
* [GitHub Container Registry](https://github.com/umadb-io/umadb/pkgs/container/umadb)

Each image is built from the Docker `scratch` base image and contains a copy of
the same statically linked Linux (musl) [binaries distributed in GitHub Releases](./install).
You can install the pre-built binaries if you prefer.

## Pulling the Docker Image

Pull `latest` from GitHub Container Registry.

```bash
docker pull ghcr.io/umadb-io/umadb:latest
```

Pull `latest` from Docker Hub.

```bash
docker pull umadb/umadb:latest
```

Images are tagged `latest`, `x.y.z` (semantic version number), `x.y` (major and minor), and `x` (major).

## Running the Container

The container's `ENTRYPOINT` is the `umadb` binary. By default, it is invoked with:

`--listen 0.0.0.0:50051 --db-path /data`

This means the container will start UmaDB listening on port **50051** and using **/data/uma.db** as the database file.

You may override the default [command line arguments](./cli) by supplying your own.

Print the `umadb` version:

```bash
docker run umadb/umadb:latest --version
```

Show the help message:

```bash
docker run umadb/umadb:latest --help
```

Because the image is built on Docker’s `scratch` base and contains only the
`umadb` executable, using `--entrypoint` to run any other command (such as `bash`)
will fail with a “failed to create task” error.

## Connecting to UmaDB

The umadb container listens on port **50051**. To make the server accessible from
outside the container, publish the port using `-p` / `--publish` when starting the
container:

```bash
docker run --publish 50051:50051 umadb/umadb:latest
```

## Data Storage

By default, the UmaDB container stores data in `/data/uma.db`. To persist
the database file on your host, mount a local directory to the container's `/data`
using `-v` / `--volume`:

```bash
docker run --volume /path/to/local/data:/data umadb/umadb:latest
```

UmaDB will then create and use `/path/to/local/data/uma.db` to store your events.

## Transaction Layer Security

By default, `umadb` starts an "insecure" gRPC server. To enable TLS, mount a local folder
containing your certificate and key, and provide their paths via environment variables
`UMADB_TLS_CERT` and `UMADB_TLS_KEY`. Use `-v` / `--volume` and `-e` / `--env` with `docker run`
to set this up:

```bash
docker run \
  --volume /path/to/local/secrets:/etc/secrets \
  --env UMADB_TLS_CERT=/etc/secrets/server.pem \
  --env UMADB_TLS_KEY=/etc/secrets/server.key \
  umadb/umadb:latest
```

This will start UmaDB with a **secure** gRPC channel using your TLS certificate and key.

## Examples

The following example will run the `umadb` container with the name `umadb-insecure`, publish the container
port at `50051`, store event data in the local file `/path/to/local/data/uma.db`, and start an "insecure"
gRPC channel.

```bash
docker run \
  --name umadb-insecure \
  --publish 50051:50051 \
  --volume /path/to/local/data:/data \
  umadb/umadb:latest
```

The following example will run the `umadb` container with the name `umadb-secure`, publish the container port
at `50051`, store event data in the local file `/path/to/local/data/uma.db`, and activate TLS using a PEM encoded
certificate in the local file `/path/to/local/secrets/server.pem` and key in
`/path/to/local/secrets/server.key`.

```bash
docker run \
  --name umadb-secure
  --publish 50051:50051 \
  --volume /path/to/local/data:/data \
  --volume /path/to/local/secrets:/etc/secrets \
  --env UMADB_TLS_CERT=/etc/secrets/server.pem \
  --env UMADB_TLS_KEY=/etc/secrets/server.key \
  umadb/umadb:latest
```

## Docker Compose

For convenience, you can use Docker Compose.

Write a `docker-compose.yaml` file:


```yaml
services:
  umadb:
    image: umadb/umadb:latest
    ports:
      - "50051:50051"
    volumes:
      - ./path/to/local/data:/data
```

And then run:

```bash
docker compose up -d
```