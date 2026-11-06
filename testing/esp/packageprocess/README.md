# Overview

These are integration tests for WsPackageProcess target validation and package-map metadata operations.

# Implementation

## Prerequisites and Assumptions

The tests require a running ESP with WsPackageProcess enabled. Defaults can be overridden on the CLI:

1. HTTP on `127.0.0.1:8010`
2. Roxie target named `roxie`
3. Non-Roxie target named `thor`
4. No authentication
5. `ecl` available on `PATH` or under `$HOME/runtime/opt/HPCCSystems/bin`

Credentials may be supplied with `--user` and `--password`, or with `$HPCC_USER` and `$HPCC_PASSWORD`.

## Running Tests

### Basic Usage

```bash
python3 packageprocess_test.py
```

By default, `get-part`, `remove-part`, and `delete` use direct SOAP requests with `GlobalScope=0` to avoid known CLI option-initialization defects.

### Advanced Options

```bash
# Custom host and targets
python3 packageprocess_test.py --host 192.168.1.100 --roxie-target myroxie --non-roxie-target mythor

# Run one test
python3 packageprocess_test.py -t test_01_reject_non_roxie_add

# Exercise the known-unreliable CLI paths
python3 packageprocess_test.py --use-cli-for-known-failures -v

# Show help
python3 packageprocess_test.py --help
```

## Tests

Each test creates and cleans up its own uniquely named package map.

1. `T01`: Reject add to a non-Roxie target with `Roxie target required`.
2. `T02`: Add a package map to Roxie.
3. `T03`: List the added package-map metadata.
4. `T04`: Activate the package map.
5. `T05`: Read the active package map.
6. `T06`: Deactivate the package map.
7. `T07`: Read a target-scoped package-map part.
8. `T08`: Remove a target-scoped package-map part.
9. `T09`: Delete a target-scoped package map.
10. `T10`: Reject add-part to a non-Roxie target.
11. `T11`: Reject copying a package map to a non-Roxie target.
12. `T12`: Reject package-map validation against a non-Roxie target.
