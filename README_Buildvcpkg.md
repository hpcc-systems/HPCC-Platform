# Purpose
- Build/Test HPCC Systems components using VCPKG package manager

- Validate builds across multiple operating systems and architectures.

- Run various test suites including unit tests, smoke tests, regression tests, and UI tests.

- Package artifacts for platforms like Linux, Windows, and macOS.

- Automate periodic scheduled builds (via cron).

# Prerequisites 
In order to use the buildvcpkg workflow the following tools/enviroments must be installed 

- CMake: ≥ 3.3.2 (implied in usage)

- VCPKG: Used for dependency management

- Docke: Required for containerized builds

- NodeJS: 20.x.x (used in ECL Watch build)

- GitHub Runners: Supports Ubuntu, macOS, Windows

- Secrets Config: For DockerHub, GH tokens, internal builds

# Build Steps
How the workflow proceeds depending on the trigger type:
1.Trigger Event

supports:
- workflow_dispatch(manual run with inputs)
- pull_request(on main and candidate branches)
- push (on valid tags)
- schedule(cron job)

2. Workflow Dispatch Flow(manual inputs)
   
build-workflow-dispatch: Builds using input OS and options

test-workflow-dispatch: Optional smoke test if smoketest is true

3. Pull Request and Push Flow
build:

build docker is able to compile the HPCC platform across many different platforms 

- Ubuntu (20.04, 22.04, 24.04)
-	RockyLinux 8
-	CentOS 7
-	WebAssembly (Emscripten)
-	Windows (2019, 2022)
- macOS (13, 14)

Tests:

- test-smoke: Lightweight smoketests

- test-unit: Unit tests

- test-ui: Web UI tests

- test-bundles-on-thor: Test bundles on Thor cluster

- test-regression-suite: Full regressions tests

Component-Specific Builds
build-eclwatch: Compiles the ECL Watch front end

# Supported Platforms: 
- Ubuntu 20.04-24.04
- CentOS 7
- RockyLinux 8
- Windows 2019/2022
- macOS 13/14
- wasm32-emscripten
