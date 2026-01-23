# Docker tooling for HPCC Platform

This folder contains Dockerfiles and helper scripts used for:

- building HPCC Platform in containerized build environments (multiple OS targets)
- producing a runnable `platform-core` image from your current working tree (debug/release)
- spinning up a local Kubernetes deployment using the repo Helm charts

## Prerequisites

- Docker (BuildKit enabled). Some workflows use `docker buildx`.
- For `startall.sh` / `stopall.sh`: `helm`, `kubectl`, and access to a Kubernetes cluster.
- Optional: a `.env` file at the repo root with `DOCKER_USERNAME` / `DOCKER_PASSWORD` (used by `build.sh` for `docker login`).

## Common workflows

### Build a local `platform-core` image from this working tree

This is the main developer workflow.

```bash
cd dockerfiles

# Debug build + install into an image
./image.sh build -m debug

# Release build + package (.deb) + install into an image
./image.sh build -m release

# Faster incremental debug build (equivalent to build -m debug)
./image.sh incr

# Re-run CMake configure step
./image.sh build -m debug -r

# Keep separate build volumes per git branch
./image.sh build -m debug -t

# Select the build OS image (default is ubuntu-22.04)
BUILD_OS=ubuntu-24.04 ./image.sh build -m debug

# Override architecture detection
./image.sh build -a arm64 -m debug
```

Notes:

- `image.sh` uses Docker volumes to cache sources and build artifacts.
- The final image is tagged as `hpccsystems/platform-core:<branch>-<mode>-<crc>` and also as `incr-core:<mode>`.
- Run `./image.sh status` to see the resolved environment values.

### Create an image from a local artifact

Use `install` to create an image from either a local `.deb` or a local build folder.

```bash
cd dockerfiles

# From a .deb
./image.sh install /path/to/hpccsystems-platform-community_*.deb

# From a build folder (must contain CMakeCache.txt)
./image.sh install /path/to/build/folder
```

### Build containerized CMake builds across OS targets

`build.sh` builds an OS-specific container image and runs a CMake/Ninja build inside it.

```bash
cd dockerfiles

# Run several builds in parallel (logs go to build/docker-logs/)
./build.sh

# Single target
./build.sh ubuntu-22.04

# Override architecture
ARCH=arm64 ./build.sh ubuntu-22.04
```

Build output is written back into the repo under `build/<target>/`.

### Deploy to a local Kubernetes cluster

`startall.sh` and `stopall.sh` use the Helm charts in `../helm/`.

```bash
cd dockerfiles

# Start or upgrade an HPCC install (defaults to cluster name "mycluster")
./startall.sh -n mycluster -l <image-tag>

# Stop it
./stopall.sh -n mycluster
```

Run `./startall.sh -h` for supported flags (including optional Elastic/Prometheus/Loki deployments).

## Dockerfiles in this folder

### Build environment images

These `*.dockerfile` files create “build environment” images that are thin wrappers over prebuilt
`hpccsystems/platform-build-base-<os>:<vcpkg-ref>` images.

- `ubuntu-20.04.dockerfile`, `ubuntu-22.04.dockerfile`, `ubuntu-24.04.dockerfile`
- `rockylinux-8.dockerfile`
- `centos-7.dockerfile`
- `wasm32-emscripten.dockerfile`

They’re used by `build.sh` and `image.sh`.

### Runtime base images (`platform-core-*`)

These Dockerfiles install runtime dependencies, create the `hpcc` user, and set up the container
filesystem layout (but do not build HPCC on their own).

- `platform-core-ubuntu-20.04.dockerfile`
- `platform-core-ubuntu-22.04.dockerfile`
- `platform-core-ubuntu-24.04.dockerfile`
- `platform-core-debug-ubuntu-22.04.dockerfile` (adds `gdb` ptrace capability via `setcap`)

`image.sh` uses these as a base and then installs HPCC from your build output.

### Build-from-deb Dockerfile

`platform-core-ubuntu-22.04/Dockerfile` is a standalone Dockerfile that expects an HPCC `.deb` to be present
in the Docker build context (see `PKG_FILE` in that Dockerfile). It can be useful for CI or manual builds where
you already have a packaged `.deb` and want a simple `docker build`.

## Helper scripts

- `image.sh`: build/install HPCC into a `platform-core` image from the current repo checkout.
- `build.sh`: run builds in OS-specific build containers; can build multiple targets in parallel.
- `cleanup.sh`: remove dangling images and older `hpccsystems/*` tags.
- `startall.sh` / `stopall.sh`: deploy/remove HPCC (and optional logging/metrics stacks) using Helm.

## Examples

- `examples/numpy/`: example of layering extra Python packages on top of `platform-core` (builds `hpccsystems/platform-core:numpy`).
- `examples/ldap/`: a `docker-compose.yaml` that starts a local LDAP server and phpLDAPadmin for development/testing.

## Notes about CI files

This folder also contains `action.yml` and `buildall-common.sh`, which are intended for CI publishing workflows.
As of this folder’s current contents, `action.yml` references a `Dockerfile` next to it, but there is no
top-level `Dockerfile` in this directory. The Dockerfiles in this folder live in subfolders (for example
`platform-core-ubuntu-22.04/Dockerfile` and `examples/numpy/addnumpy/Dockerfile`). If you intend to use
`action.yml` directly as a Docker Action, verify the expected `Dockerfile` is present or adjust the action accordingly.

