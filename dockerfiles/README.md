# Container images and local Kubernetes

Run the commands below from the HPCC-Platform repository root.

## TL;DR: Docker Engine on Linux (no Docker Desktop)

Install Docker Engine, kind, kubectl, and Helm first; see
[Prerequisites](#prerequisites). Use the Docker Engine running on the Linux host;
Docker Desktop is not needed.

```bash
export PATH="$HOME/.local/bin:$PATH"  # If kind is installed here
export CONTAINER_RUNTIME=docker
export KIND_EXPERIMENTAL_PROVIDER=docker

# Confirm the host Docker Engine is running
docker info

# Build the local HPCC image; skip this if already built by this Docker Engine
./dockerfiles/image.sh build

# Create Kubernetes once
kind create cluster --name hpcc --wait 120s
kubectl config use-context kind-hpcc
kubectl get nodes

# Load the local image and install HPCC without Elastic
./dockerfiles/startall.sh -e
kubectl rollout status deployment/eclwatch --timeout=300s

# Keep this running, then open http://localhost:8010
kubectl port-forward service/eclwatch 8010:8010
```

After changing code, rebuild and update the existing deployment:

```bash
./dockerfiles/image.sh build
./dockerfiles/startall.sh -u -e
```

## TL;DR: Podman on Linux

Install Podman, kind, kubectl, and Helm first; see [Prerequisites](#prerequisites).
Podman Desktop is not required.

```bash
export PATH="$HOME/.local/bin:$PATH"  # If kind is installed here
export CONTAINER_RUNTIME=podman
export KIND_EXPERIMENTAL_PROVIDER=podman

# Build the local HPCC image; skip this if already built in Podman
./dockerfiles/image.sh build

# Create Kubernetes once
kind create cluster --name hpcc --wait 120s
kubectl config use-context kind-hpcc
kubectl get nodes

# Load the local image and install HPCC without Elastic
./dockerfiles/startall.sh -e
kubectl rollout status deployment/eclwatch --timeout=300s

# Keep this running, then open http://localhost:8010
kubectl port-forward service/eclwatch 8010:8010
```

After changing code, rebuild and update the existing deployment:

```bash
./dockerfiles/image.sh build
./dockerfiles/startall.sh -u -e
```

Startup discovers the first non-dangling `platform-core` tag returned by the
selected engine. When several builds exist, check the selected tag in its output
or use the [explicit-tag workflow](#using-an-explicit-image-tag).

## Prerequisites

- Bash, Git, and a running Docker or Podman engine. The local
  [image builder](image.sh) supports these two engines; other scripts in this
  directory have not all been migrated from Docker.
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/),
  [kubectl](https://kubernetes.io/docs/tasks/tools/), and
  [Helm](https://helm.sh/docs/intro/install/). Put the executables on `PATH`.
  Startup also looks for kind at `~/.local/bin/kind`.
- For rootless Podman on Linux, cgroup v2 and systemd CPU-controller delegation.
  Follow [kind's rootless requirements](https://kind.sigs.k8s.io/docs/user/rootless/)
  for networking, PID/inotify limits, and log-driver troubleshooting.
- Enough memory and disk for the node, images, build cache, and workloads.
  Elastic, Prometheus, and Loki need additional resources. Image loading
  temporarily exports an archive, so the temporary directory also needs space.
- Registry access for the builder's base image and other uncached images, plus
  internet access for optional chart dependencies. Local builds use the vcpkg
  submodule revision recorded in this checkout to select their builder base.

A container engine alone does **not** provide Kubernetes. Before deploying,
check that the intended cluster is reachable:

```bash
kubectl config current-context
kubectl get nodes
```

The lifecycle scripts operate in the current kubectl context and namespace.
Use a dedicated development cluster/namespace: shutdown deletes all jobs and
NetworkPolicies in that namespace, and `-f` can force-delete remaining pods.
These scripts enable privileged development settings by default, not a
production configuration.

## Runtime selection and Docker Desktop

[image.sh](image.sh) and local tag discovery in [startall.sh](startall.sh)
use the shared [container-engine.sh](chainguard/container-engine.sh). They
automatically try Docker before Podman and report the selected engine.
An installed Docker CLI with a stopped daemon does not prevent Podman fallback.
Set `CONTAINER_RUNTIME=podman` or `CONTAINER_RUNTIME=docker` to be explicit.

The helper sources the repository-root `.env`, or the file named by `ENV_FILE`.
Use shell syntax and quote values containing spaces or special characters.
Do not commit credentials. Images and named build volumes are engine-specific;
Docker and Podman do not share them.

To use Docker Desktop, start it, enable its Kubernetes cluster, and select its
context:

```bash
export CONTAINER_RUNTIME=docker
kubectl config use-context docker-desktop
kubectl get nodes
./dockerfiles/image.sh build
./dockerfiles/startall.sh -e
```

If you use kind with Docker instead, create the cluster with
`KIND_EXPERIMENTAL_PROVIDER=docker`. A kind cluster's provider must match the
engine used to load its images. For non-kind clusters, ensure the image is in
the cluster's image store or available from a registry; startup does not import it.

## Startup, images, and storage

[startall.sh](startall.sh) installs Helm releases into an **existing** Kubernetes
cluster; it does not create Kubernetes. The default HPCC release is `mycluster`.

| Option | Purpose |
|---|---|
| `-u` | Upgrade releases, installing any that do not exist yet. |
| `-e` | Skip Elastic deployment and its HPCC log-access configuration. |
| `-c` | Update dependencies for enabled optional charts. |
| `-l <tag>` | Use an explicit tag; skip local image discovery and loading. |
| `-d <repository>` | Change the image root from `hpccsystems`. |
| `-n <name>` | Change the HPCC Helm release name, not the Kubernetes cluster name. |
| `-p <path>` | Configure local host-path storage; see the [local storage chart](../helm/examples/local/hpcc-localfile/README.md). |
| `-m` / `-g` | Deploy Prometheus / Loki. |
| `-t` | Render templates instead of deploying; does not load images or apply automatic kind storage defaults. |
| `-h` | Show all options. |

Omitting `-e` enables Elastic by default. Skipping a previously installed optional
stack does not uninstall it.

Helm options are forwarded to HPCC and enabled optional stacks. Use
`--kubeconfig <file>` (or `--kubeconfig=<file>`) to select a configuration file,
and `--kube-context <name>` to override its current context. Startup uses the
same configuration and context for Kubernetes preflight checks, kind image
loading, and the local storage chart. Quote paths containing spaces.

When the selected context is `kind-<name>` and no `-l` is supplied, startup exports
the discovered local image and imports it into that kind cluster. Docker Hub names
are qualified as `docker.io/...` so Podman's `localhost/...` aliases do not cause
Kubernetes to pull a different image.

For single-node kind clusters, startup also applies
[kind-values.yaml](kind-values.yaml), using `ReadWriteOnce` volumes from kind's
`standard` local-path storage class. Explicit `-p` settings bypass these defaults.
Multi-node clusters need a suitable shared-storage configuration; node-local
volumes are not shared across nodes. See the [storage guide](../helm/hpcc/docs/storage.md).

### Using an explicit image tag

With `-l`, the image must already be available to Kubernetes. For a local Podman
image, load it manually first:

```bash
export KIND_EXPERIMENTAL_PROVIDER=podman
TAG=your-local-build-tag
podman tag "hpccsystems/platform-core:$TAG" "docker.io/hpccsystems/platform-core:$TAG"
ARCHIVE=$(mktemp)
podman save --output "$ARCHIVE" "docker.io/hpccsystems/platform-core:$TAG"
kind load image-archive "$ARCHIVE" --name hpcc
rm -f "$ARCHIVE"
./dockerfiles/startall.sh -l "$TAG" -u -e
```

Reload after rebuilding. Use `load image-archive` with Podman: kind's
`load docker-image` command reads images through the Docker CLI even when its
node provider is Podman. Published images accessible to the cluster do not need
a local engine or manual import.

## Optional Elastic logging

Fetch dependencies and enable Elastic on an existing HPCC installation:

```bash
./dockerfiles/startall.sh -u -c
```

On clusters enforcing NetworkPolicies, HPCC's namespace-wide default-deny policy
also affects logging pods. Enable the Elastic chart's HTTP ingress rule so pods
in the same namespace can reach Elasticsearch on port 9200:

```bash
helm upgrade myelastic4hpcclogs ./helm/managed/logging/elastic --reuse-values \
  --set elasticsearch.networkPolicy.http.enabled=true \
  --set-json 'elasticsearch.networkPolicy.http.additionalRules=[{"podSelector":{}}]'
```

This rule does not allow cross-namespace or external ingress. Reapply these values
if a later chart upgrade resets them. See the [Elastic guide](../helm/managed/logging/elastic/README.md)
and [managed services](../helm/managed/README.md) for other configurations.

## Access and shutdown

kind does not automatically assign external IPs to `LoadBalancer` services.
Use port forwarding for ECL Watch:

```bash
kubectl port-forward service/eclwatch 8010:8010
```

Keep it running and open http://localhost:8010. For other services, use
`kubectl get services` to find their names and ports.

[stopall.sh](stopall.sh) uses Helm and kubectl, not the container engine:

```bash
./dockerfiles/stopall.sh -w
```

It removes HPCC and the optional stacks, deletes the Elastic data claim by default,
and leaves Kubernetes running. Use `-n <name>` for a non-default HPCC release;
`-e` preserves Elastic and its claim. It is not a backup or data-preservation tool.
Deleting a kind cluster with `kind delete cluster --name hpcc` also deletes its
node-local data.

## Build tools and further reading

- [image.sh](image.sh): local development images, defaulting to Ubuntu 22.04 and
  release mode. Supports debug/relwithdebinfo, branch-specific build volumes
  (`-t`), and installation from a local DEB or build directory. Run `--help`
  for options. The resulting `hpccsystems/platform-core` image runs HPCC components.
- [Chainguard builds](chainguard/README.md): standalone APK/DEB/RPM package builds,
  a container-backed CMake command, toolchain images, and runtime validation.
  These are separate from the local Kubernetes image workflow.
- [buildall-common.sh](buildall-common.sh): release image publishing support used
  by CI. [build.sh](build.sh) and [cleanup.sh](cleanup.sh) also contain direct Docker
  calls; `CONTAINER_RUNTIME` does not migrate those scripts to Podman.
- [Helm values](../helm/hpcc/values.yaml): global defaults and per-component
  settings for Dali, ESP, Roxie, Thor, and other services. Keep deployed images
  compatible with the chart from this checkout.
- [Expert Helm configuration](../helm/hpcc/docs/expert.md) and
  [examples](../helm/examples/): advanced configuration, storage, and integrations.
- [Custom image example](examples/numpy/): extending a base image with extra
  dependencies.
