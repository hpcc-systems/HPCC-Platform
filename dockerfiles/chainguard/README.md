# Chainguard Builds

## TL;DR

From the HPCC-Platform repository root:

```bash
./dockerfiles/chainguard/build.sh
```

This builds HPCC Platform and creates APK, DEB, RPM, and SBOM files using
**glibc 2.34 and GCC 11**. Build files and packages go in:

```text
build/chainguard-hpcc-glibc2.34-gcc11-amd64/
```

No LN checkout is required.

## Setup

Use Docker or Podman with a running engine. macOS also supports Apple's
`container` CLI; Windows supports Docker, Podman, or WSL containers
(`wslc.exe`) from Git Bash.

The scripts automatically select the first usable runtime, trying Docker before
Podman. An installed Docker CLI without a running engine does not prevent falling
back to Podman. Set `CONTAINER_RUNTIME=podman` to explicitly select Podman.
Each script reports the selected container runtime on stderr before doing any
build work.
Docker Hub image references are fully qualified, so Podman does not require
short-name aliases or an unqualified-search registry configuration.

For access to the default JFrog images and shared dependency cache, create
`.env` in the HPCC-Platform repository root:

```bash
JFROG_USERNAME=your.name@example.com
JFROG_PASSWORD='your-password-or-PAT'
```

Keep credentials out of version control. The file is optional if the required
images are already available locally. Set `ENV_FILE` to use another file.

The build automatically pulls the builder image, reuses a local image if the
pull fails, or builds one if needed. The first build can take longer while
dependencies are downloaded and compiled.

APK builds require unprivileged user namespaces. The script checks this
before building and reports any required host configuration.

## Common Build Options

| Option | Purpose |
|---|---|
| `--glibc <version>` | Change the glibc baseline; GCC follows the baseline unless overridden. |
| `--gcc <major>` | Override the GCC version. |
| `--arch amd64` or `--arch arm64` | Select the architecture; defaults to the host. |
| `-f deb,rpm` | Build only the listed formats instead of all three. |
| `-b <directory>` | Change the build directory. |
| `-o <directory>` | Write packages and SBOMs to a separate directory. |
| `--containerized` | Build for Kubernetes rather than host installation. |
| `--no-pull` | Reuse or build a local builder image without pulling it. |
| `--help` | Show all options. |

For example, build DEB and RPM packages with glibc 2.39 and its paired GCC 13:

```bash
./dockerfiles/chainguard/build.sh --glibc 2.39 -f deb,rpm
```

Each baseline and architecture gets a separate default build directory.
Kubernetes builds also add `-containerized` to the directory name. If you use
`-b`, keep separate directories for different toolchains and build modes.

The default glibc 2.34 baseline targets Ubuntu 21.10 or later, Rocky 9 or later,
and Wolfi. Rocky 8 is not supported. Newer baselines require newer target
libraries.

## Run CMake Directly

Use [cmake-chainguard.sh](cmake-chainguard.sh) to run a CMake command inside the
builder with the same arguments:

```bash
./dockerfiles/chainguard/cmake-chainguard.sh --version
```

It defaults to glibc 2.34 and GCC 11. Set `CHAINGUARD_GLIBC` and
`CHAINGUARD_GCC` to override them.

The proxy runs only the requested CMake command. It does not supply HPCC's
configure flags, build automatically, or create packages. Use
[build.sh](build.sh) for the complete HPCC build.

## Build a Development Image

The build script handles this automatically. To prepare the default image
separately:

```bash
./dockerfiles/chainguard/create-build-image.sh --glibc 2.34
```

Run [create-build-image.sh](create-build-image.sh) with `--help` for options.

## Runtime Image Audit

The `runtime-audit` target in
[Dockerfile.runtime](containers/Dockerfile.runtime) checks the assembled runtime
before the final image can be built. Its Python check uses the native HPCC
compiler, `eclcc -Me`, to evaluate a foldable `EMBED(Python3)` function inside
the runtime rootfs. The function reads `sys.version_info`; its result must match
`RUNTIME_PYTHON`, which must also match `PYTHON` in the packaged `hpcc-baseline`.
Missing libraries, plugin initialization/evaluation failures, and version
mismatches fail the audit rather than bypassing it.

Do not test `libpy3embed.so` with Python's `ctypes.CDLL`. This is an HPCC
embedding plugin, not a Python extension: its global initialization initializes
the interpreter and releases the GIL with `PyEval_SaveThread`. Loading it from
an already-running Python interpreter interferes with that host's interpreter
and thread state. The previous `ctypes` audit segfaulted (exit 139) with Python
3.12; that failure alone does not establish a packaging or ABI defect. Native
ECL evaluation exercises the supported embedding path instead. This smoke
check does not replace the Python embedding regression suite or prove that
every distributed query works.

## LN Builds

To build LN, run its wrapper from the LN repository root instead:

```bash
./dockerfiles/chainguard/build.sh
```

The wrapper uses the sibling HPCC-Platform checkout for tooling, reads LN's
`.env`, and writes to `../build/chainguard-glibc2.34-gcc11/`.
Use `-s /path/to/HPCC-Platform` to select another checkout.

## References
 
| Image | Sysroot | GCC | `GLIBC` / `GLIBCXX` | Python | Supported targets |
|---|---|---:|---|---:|---|
| `hpcc-chainguard-glibc2.34-gcc11-dev` | Ubuntu 21.10 | 11 | 2.34 / 3.4.29 | 3.9 | Ubuntu >= 21.10, Rocky >= 9, Wolfi |
| `hpcc-chainguard-glibc2.35-gcc11-dev` | Ubuntu 22.04 | 11 | 2.35 / 3.4.30 | 3.10 | Ubuntu >= 22.04, Wolfi |
| `hpcc-chainguard-glibc2.36-gcc11-dev` | Ubuntu 22.10 | 11 | 2.36 / 3.4.30 | 3.10 | Ubuntu >= 22.10, Wolfi |
| `hpcc-chainguard-glibc2.37-gcc11-dev` | Ubuntu 23.04 | 11 | 2.37 / 3.4.32 | 3.11 | Ubuntu >= 23.04, Wolfi |
| `hpcc-chainguard-glibc2.38-gcc13-dev` | Ubuntu 23.10 | 13 | 2.38 / 3.4.32 | 3.11 | Ubuntu >= 23.10, Wolfi |
| `hpcc-chainguard-glibc2.39-gcc13-dev` | Ubuntu 24.04 | 13 | 2.39 / 3.4.33 | 3.12 | Ubuntu >= 24.04, Rocky >= 10, Wolfi |
| `hpcc-chainguard-glibc2.40-gcc13-dev` | Ubuntu 24.10 | 13 | 2.40 / 3.4.33 | 3.12 | Ubuntu >= 24.10, Wolfi |
| `hpcc-chainguard-glibc2.41-gcc13-dev` | Ubuntu 25.04 | 13 | 2.41 / 3.4.34 | 3.13 | Ubuntu >= 25.04, Wolfi |
| `hpcc-chainguard-glibc2.42-gcc15-dev` | Ubuntu 25.10 | 15 | 2.42 / 3.4.34 | 3.13 | Ubuntu >= 25.10, Wolfi |
| `hpcc-chainguard-glibc2.43-gcc15-dev` | Ubuntu 26.04 | 15 | 2.43 / 3.4.35 | 3.14 | Ubuntu >= 26.04, Wolfi |
| `hpcc-chainguard-glibc2.44-gcc16-dev` | Ubuntu 26.10 | 16 | 2.44 / 3.4.36 | 3.14 | Ubuntu >= 26.10, Wolfi |