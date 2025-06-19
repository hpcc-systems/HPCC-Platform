# HPCC Systems Test Workflows

## Overview

This document details the Test Workflows of the HPCC Platform, which are performed via GitHub Actions, starting via the build-assets.yml and build-vcpkg.yml processes. These workflows are compatible with both the community and enterprise/internal builds of the HPCC Platform.


The test process currently supports the following operating systems:

- Ubuntu-20.04

- Ubuntu-22.04

- Ubuntu-24.04

- RockyLinux-8

- CentOS-7

- Windows-2019

- Windows-2022

- macOS-13

- macOS-14

## Purpose

The Test Workflows are utilized with the goals of:

- Preventing errors from getting pushed to the HPCC Platform

- Catching mistakes in code early

- Supporting automated testing of code before it is published

- Ensuring the stability of the HPCC Platform

## Test Workflow Stages

### 1. Build Process

- On code push, the build workflow runs, creating build variables, acquiring dependencies (via vcpkg), and triggering the creation of Docker Images, documentation, and packages for the HPCC Platform. See BuildWorkflows.md for more information.

### 2. Test Execution

- In the build-vcpkg.yml workflow, after the builds are created, a runner environment is initialized and the necessary tools are installed (e.g., CMake, cppunit) for each version being tested (e.g. build-docker-ubuntu-22_04 is tested by test-smoke-docker-ubuntu-22_04). This ensures that each build is tested properly for bugs and overall success, leading to a more secure platform. Tests utilize external workflows, such as test-smoke-gh_runner.yml or test-ui-gh_runner.yml, which check for necessary patterns in the code (Regular Expressions), dependency management, core code files, and successful execution of the build. Custom scripts additionally run ECL-specific tests to ensure successful data processing.

### 3. Result Analysis

- The results of the tests are then analyzed to ensure that the HPCC Platform will be functional. Failing tests will result in the GitHub Action failing to complete, preventing broken code from being published. The exact errors are then made visible so that they can be fixed for reuploading, where the process repeats to ensure success. If all tests pass, the GitHub push is confirmed and merged into the master branch.

## Tools

- GitHub Actions: Triggers the testing workflows

- CMake: Configures test environments

- vcpkg: Manages building and testing dependencies

- Helm: A package manager for Kubernetes, generates YAML files

- Kube-Score: An analysis tool for Kubernetes YAML files