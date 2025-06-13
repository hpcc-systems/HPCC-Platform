# HPCC Systems Build Workflows

# Overview 
This document outlines the Build Workflows of the HPCC Platform. These workflows are executed using GitHub Actions. Specifically, these workflows are facilitated through the build-assets.yml and build-vcpkg.yml algorithim.Both of these YAML files are apllicable for both enterprise and commmunity builds of the HPCC Platform.
# Purpose
- Build/Test HPCC Systems components using VCPKG package manager

- Validate builds across multiple operating systems and architectures.

- Run various test suites including unit tests, smoke tests, regression tests, and UI tests.

- Package artifacts for platforms like Linux, Windows, and macOS.

- Automate periodic scheduled builds (via cron).

# Prerequisites 
In order to use the buildvcpkg workflow the following tools/enviroments must be installed 

- CMake: ≥ 3.3.2 (required to generate builds)

- Docker: Required for containerized builds

- NodeJS: 20.x.x (used in UI component ECL Watch build)

- GitHub Runners: Supports Ubuntu, macOS, Windows

- vcpkg(manage C++ tools such as system libraries)

# Steps
How the workflow proceeds depending on the trigger type:
1.Trigger Event

runs when:
- workflow_dispatch(manual run with inputs)
- pull_request(on main and candidate branches)
- push (on valid tags)
- schedule(cron job)

2. Build Jobs: compiling/ running HPCC platform across different OS
   - sets up the build enviroment from the repository depending on the OS
   - Runs the new build process which is where the compilation/running of the HPCC platform happens
   - Lastly once everything has been run, the output is packaged in form of docker image, packages, etc
   - One specifc build is the build-workflow-dispatch, here the workflow is manually triggered based on user input and the HPCC platform is compiled based on this input
   
3. Tests Jobs: once the build has been setup and the HPCC Platform is compiled these tests check for if this platform is benhaving correctly now

- test-smoke: Lightweight smoketests - Which check the basic requirements of the build ensuring, anything does't crash. Essentially makes sure the platform can be installed and run successfully

- test-unit: Unit tests - validate the indivual components and functions of the platforms, veryifying eveything works correctly

- test-ui: Web UI tests - Checks the web UI(web-based user interface in managing HPCC System platform) to makesure it loads and behaves as expected

- test-bundles-on-thor: Test bundles(pre-packaged reusable componets(ECL libraries, function,etc) to see if they run properly on thor engine in data processing

- test-regression-suite: Full regression suite(automated tests) to ensure any new code doesn't break any if the exisiting features

- test-workflow-dispatch: runs when the manual workflow is intiatedfrom user input, but this is the case only if the input smoketest is equal to true by the user. Essentially this validates the HPCC platform, making sure everything installs correctly doesn't crash due to basic functionality


Component-Specific Builds
build-eclwatch: Compiles the ECL Watch front end

# Supported Platforms: 
- Ubuntu 20.04-24.04
- CentOS 7
- RockyLinux 8
- Windows 2019/2022
- macOS 13/14
- wasm32-emscripten
