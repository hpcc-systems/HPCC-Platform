```
HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

https://hpccsystems.com
```

# Build Instructions

## Prerequisites

### Ubuntu
#### Ubuntu 22.04

For Docker or other non-interactive environments, set these to prevent prompts:

```bash
export DEBIAN_FRONTEND=noninteractive
export TZ=UTC
sudo ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ | sudo tee /etc/timezone > /dev/null
```

Install core build dependencies:

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    apt-transport-https \
    autoconf \
    autoconf-archive \
    automake \
    autotools-dev \
    binutils-dev \
    bison \
    build-essential \
    ca-certificates \
    curl \
    dirmngr \
    flex \
    git \
    gnupg \
    groff-base \
    libtool \
    pkg-config \
    software-properties-common \
    tar \
    unzip \
    uuid-dev \
    zip
```

Install CMake, Ninja, and Mono (needed for NuGet tooling used by vcpkg binary cache):

```bash
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
echo "deb https://download.mono-project.com/repo/ubuntu stable-focal main" | sudo tee /etc/apt/sources.list.d/mono-official-stable.list
sudo apt-get update
sudo apt-get install -y --no-install-recommends cmake mono-complete ninja-build
```

Install additional tools used by HPCC builds and packaging:

```bash
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    ccache \
    default-jdk \
    ninja-build \
    nodejs \
    python3-dev \
    rsync \
    fop \
    libsaxonb-java \
    r-base \
    r-cran-rcpp \
    r-cran-rinside \
    r-cran-inline
```

(Optional) Install `fnm` and Node 22 for local frontend/tooling workflows:

```bash
curl -o- https://fnm.vercel.app/install | bash
source ~/.bashrc
fnm install 22
fnm default 22
```

## Get Latest HPCC Systems Sources

Visit [Git-step-by-step](https://github.com/hpcc-systems/HPCC-Platform/wiki/Git-step-by-step) for full instructions.

To get started quickly, simply:

```bash
git clone --recurse-submodules https://github.com/hpcc-systems/HPCC-Platform.git
cd HPCC-Platform
```

## vcpkg Setup

The build in this repository uses `vcpkg` for third-party dependencies.

```bash
./vcpkg/bootstrap-vcpkg.sh
```

Set the triplet (matching the containerized build template):

```bash
export TRIPLET=x64-linux-dynamic
export VCPKG_DEFAULT_HOST_TRIPLET=$TRIPLET
export VCPKG_DEFAULT_TRIPLET=$TRIPLET
```

Optional: configure NuGet binary cache (GitHub Packages):

To read/write packages from `https://nuget.pkg.github.com/hpcc-systems/index.json`, use a GitHub Personal Access Token (PAT):

- Use a classic PAT with at least `read:packages` (and `write:packages` if you publish cache entries).
- If your org enforces SSO, authorize the token for `hpcc-systems`.
- Set `GITHUB_ACTOR` to your GitHub username and `GITHUB_TOKEN` to that PAT.

Example:

```bash
export GITHUB_ACTOR=<github-user>
export GITHUB_TOKEN=<classic-pat-with-packages-scope>
```

If you see `401 Unauthorized` when running the NuGet commands, verify the token scope/SSO authorization, then refresh credentials:

```bash
mono "$(./vcpkg fetch nuget | tail -n 1)" sources remove -name "GitHub" || true
mono "$(./vcpkg fetch nuget | tail -n 1)" locals all -clear
```

```bash
export VCPKG_BINARY_SOURCES="clear;nuget,GitHub,readwrite"
export VCPKG_NUGET_REPOSITORY=https://github.com/hpcc-systems/vcpkg

mono "$(./vcpkg fetch nuget | tail -n 1)" sources add \
    -name "GitHub" \
    -source "https://nuget.pkg.github.com/hpcc-systems/index.json" \
    -storepasswordincleartext \
    -username "$GITHUB_ACTOR" \
    -password "$GITHUB_TOKEN"

mono "$(./vcpkg fetch nuget | tail -n 1)" setapikey "$GITHUB_TOKEN" \
    -source "https://nuget.pkg.github.com/hpcc-systems/index.json"
```

Install dependencies:

```bash
mkdir -p ~/.vcpkg
./vcpkg install \
    --x-abi-tools-use-exact-versions \
    --downloads-root=~/.vcpkg/downloads \
    --x-buildtrees-root=~/.vcpkg/buildtrees \
    --x-packages-root=~/.vcpkg/packages \
    --x-install-root=~/.vcpkg/installed \
    --host-triplet=$TRIPLET \
    --triplet=$TRIPLET
```

## CMake

Use the CMake options from `.vscode/settings.json` (see `cmake.configureSettings` and `cmake.buildDirectory`).

Example configure command for Ubuntu 22.04 development builds:

```bash
cmake -B build/debug -S . -G Ninja \
    -DCONTAINERIZED=OFF \
    -DUSE_OPTIONAL=OFF \
    -DUSE_CPPUNIT=ON \
    -DINCLUDE_PLUGINS=ON \
    -DSUPPRESS_V8EMBED=ON \
    -DSUPPRESS_REMBED=ON \
    -DCMAKE_BUILD_TYPE=Debug
```

## Building

```bash
cmake --build build/debug --parallel
```

This will build binaries, libraries and scripts needed for HPCC Platform.

## Creating a package

```bash
cmake --build build/debug --parallel --target package
```

The package file is generated in the configured build directory.

## Notes

- If you use VS Code CMake Tools, select the same configure preset/options used above.
- If your local Node setup differs, keep the system `nodejs` package installed for packaging steps.
- Add your checkout path as safe for Git if required:

```bash
git config --global --add safe.directory '*'
```
