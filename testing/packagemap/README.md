# ECL Packagemap Test Script

This is a script for testing ecl packagemaps from command line. This script is based on
bare-metal, but should be easily modified for a container environment. The things to
consider are the url for the server as well as adding username and password as well as a
secure flag for all the ecl statements.

---

## Prerequisites

### Python dependencies

```bash
pip install pytest requests
```

### Required directory structure

Your working directory should look like this:

```test
your-test-folder/
├── test_packagemap.py
├── JSONs/
│   ├── Nopkg.json
│   ├── Inactivepkg.json
│   └── Activepkg.json
└── ecl/
    ├── icecream_data.ecl
    ├── icecream_query.ecl
    └── icecream.pkg
```

### JSON comparison files

- `Nopkg.json`: Expected Roxie response with no packagemap applied.
- `Inactivepkg.json`: Expected Roxie response with an inactive packagemap.
- `Activepkg.json`: Expected Roxie response with an active packagemap.

These files must be captured from a known-good environment before running the tests.

### HPCC Services

The following services must be running and accessible on `localhost`:

- `Thor`: Running `icecream_data.ecl`.
- `Roxie`: Publishing and querying.
- `WsEcl` (`8002`): HTTP JSON query endpoint.

---

## Running the Tests

From the directory containing `test_packagemap.py`:

```bash
# Basic run with verbose output
pytest test_packagemap.py -v

# Stop on first failure
pytest test_packagemap.py -v -x

# Show live print output (useful for debugging)
pytest test_packagemap.py -v -s

# Run a single specific test
pytest test_packagemap.py -v -k test_04_inactive_packagemap_matches_inactivepkg_json

# Recommended for debugging — verbose, live output, stop on first failure
pytest test_packagemap.py -v -s -x
```

---

## Test Summary

| Test | Command / Action | Verification |
|------|-----------------|--------------|
| Setup | `ecl run -t thor ecl/icecream_data.ecl -s localhost` | Exits cleanly |
| Setup | `ecl publish -t roxie ecl/icecream_query.ecl -s localhost` | Exits cleanly |
| 01 | Roxie query (no packagemap) | Response matches `Nopkg.json` |
| 02 | `ecl packagemap add roxie ecl/icecream.pkg -s localhost` | Exits cleanly |
| 03 | `ecl packagemap list roxie -s localhost` | Output contains `Package Id = roxie::icecream.pkg  active = 0` |
| 04 | Roxie query (packagemap inactive) | Response matches `Inactivepkg.json` |
| 05 | `ecl packagemap activate roxie icecream.pkg -s localhost` | Exits cleanly |
| 06 | `ecl packagemap list roxie -s localhost` | Output contains `Package Id = roxie::icecream.pkg  active = 1` |
| 07 | Roxie query (packagemap active) | Response matches `Activepkg.json` |
| 08 | `ecl packagemap validate roxie icecream.pkg -s localhost` | Exits cleanly |
| 09 | `ecl packagemap deactivate roxie icecream.pkg -s localhost` | Exits cleanly |
| 10 | Roxie query (packagemap deactivated) | Response matches `Inactivepkg.json` |
| 11 | `ecl packagemap delete roxie icecream.pkg -s localhost` | Exits cleanly |

---

## Adapting for a Container Environment

The following changes are needed when running against a containerised HPCC cluster:

1. **Server URL** — update `LOCALHOST` and `CURL_URL` in the configuration section:

   ```python
   LOCALHOST = "my-hpcc-host.example.com"
   CURL_URL = "https://my-hpcc-host.example.com:8002/WsEcl/submit/query/roxie/icecream_query/json"
   ```

2. **ECL credentials** — add `-u username:password` to every `ecl` command in the `run()` calls, for example:

   ```python
   run(["ecl", "packagemap", "add", ROXIE_TARGET, f"ecl/{PKG_NAME}", "-s", LOCALHOST, "-u", "user:pass"])
   ```

3. **Secure flag** — add `--ssl` (or the appropriate flag) to all `ecl` commands if TLS is enabled.

4. **Roxie query credentials** — update `CURL_PAYLOAD` if the query requires different credentials:

   ```python
   CURL_PAYLOAD = {"icecream_query": {"name": "actualuser", "password": "actualpassword"}}
   ```
