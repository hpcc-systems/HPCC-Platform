# HPCC Systems Build Workflows

# Overview 
This document outlines the Build Workflows of the HPCC Platform. These workflows are executed using GitHub Actions. Specifically, these workflows are facilitated through the build-assets.yml and build-vcpkg.yml algorithim.Both of these YAML files are apllicable for both enterprise and commmunity builds of the HPCC Platform.
# Purpose
Build-vcpkg: 
- Validate code changes in pull requests, ensuring new contributions do not break existing functions of the HPCC Platform/main codebase
- Ensuring the HPCC Platform can compile and perform on a variety of different operating systems's/architecures
- A main part of this is due to running a test suite(a series of tests for the HPCC-Platform)
Build-assets:
- To automate the release process when a version tag is pushed to the Github
- This release process is a set of actions where the HPCC platform is compiled using through production configurations based on the OS
- Bundle the compiled file into installable packages
- Then publish these artifacts based on the specific image type
- And finally notify the user and systems for this release 

# Steps
How the workflow proceeds depending on the trigger type:
1.Trigger Event/Workflow
   Both files run on:
   - Pull Requests
   - Tagged releases
   - input from user
2. Execution
- Compiles the HPCC Platform across the different Operating Systems supported
3. Tests run
  - This tests are important to ensure that the HPCC platform is behaving as expxected and no new pull request have disrupted its functions
  - This done through a rigorous test suite including smoke ,UI ,regression, and bundle tests. 
4. Generate and Upload Artifiacts
- Package the compiled file into the binarie,Docker format, etc
5.
Publish this artifact to the known users/systems

# Supported Platforms: 
- Ubuntu 20.04-24.04
- CentOS 7
- RockyLinux 8
- Windows 2019/2022
- macOS 13/14
- wasm32-emscripten
