# Regression Testing

## Why Regression Testing is Important

Adding new features and making improvements is crucial for any product's evolution. However, these changes can sometimes inadvertently introduce regressions – previously fixed bugs that resurface or unexpected issues in existing functionality.

The Regression Suite acts as a safety net, systematically verifying that core application functionality continues to work as expected after modifications. By running these tests, we can proactively identify and address regressions early in the development cycle, preventing them from reaching users and ensuring a stable experience. Think of it as a comprehensive health check for our software after any significant change.

For official releases, the regression suite runs automatically as part of the build cycle. However, individual developers can (and should) use it when making significant changes, before issuing a Pull Request.

## When to Use the Regression Suite

Run the Regression Suite at these key points during the software development lifecycle:

* **After each new build**: Following significant code changes, feature branch merges, or bug fixes, running the Regression Suite helps immediately detect any unintended side effects.
* **Before a release candidate**: As we prepare a version for final testing and potential release, a thorough run of the Regression Suite is mandatory to confirm overall stability and identify any last-minute regressions.
* **Periodically for maintenance releases**: Even for smaller updates and patches, executing a subset or the full Regression Suite helps ensure these changes haven't negatively impacted existing functionality.

By consistently utilizing the Regression Suite at these critical junctures, we maintain a high level of quality and minimize the risk of introducing regressions into our production environment.

Regression Testing consists of two parts: the Regression Test Engine (RTE) and the Regression Suite—a set of ECL files and XML files containing expected results.

## Elements of Regression Testing

### Regression Test Engine

The RTE is a front-end tool (written in Python 3) for the ECL CLI and the ECL CC compiler. The RTE adds functionality to the command line tools to support:

* **Test case execution on**:
  * All target engines defined in the config file, or
  * Only engines in a comma-separated list using the `-t|--target` parameter
* **Bulk execution** of test cases in alphabetical order:
  * In *run* mode, it executes all Regression Suite test cases on the target (except those excluded by `//no<target_engine_name>` tag) with one command
  * In *query* mode, it can execute a subset of tests by name and/or wildcards
* **Parallel execution** of test cases (starting multiple ecl command threads in the background)
* **Timeout management**:
  * Uses the global timeout defined in the RTE config file (ecl-test.json)
  * Handles the command line timeout parameter to override the global value
  * Supports individual test case timeout handling, specified with `//timeout <in_sec>` tag in the ECL code
  * When a test case reaches a timeout, the RTE will:
    * Generate stack traces if that feature is enabled
    * Abort the test with the `ecl abort ...` command
    * Generate a ZAP report file
* **Generate a report** (log file) with these sections:
  * **Report header**: Target engine and number of test cases to be executed
  * **Executed tests list**: Elapsed time (in seconds), outcome (Pass/Fail), and a link to workunit in the HPCC Platform
  * **Result**: A summary including the number of passed and failed test cases
  * **Output**: If any test is forced to generate output, their output appears here
  * **Error**: If any test failed, they will be reported here with the error message
  * **Log**: The path of the log file
  * **Elapsed time**: The whole RTE session time in hh:mm:ss format

  For more details on the Regression Test Engine and its options, see [RTE Usage](https://github.com/hpcc-systems/HPCC-Platform/blob/master/testing/regress/README.rst)

### The Regression Test Suite

The Regression Suite is a collection of ECL files and XML result files (the expected results from the ECL). This includes ECL activities with different usage and parameters, plugins (e.g., file, spray, despray, Kafka, Redis, Memcached, MySQL, etc.) and embedded languages (e.g., C++, Java, Python, etc.).

It covers every aspect of activities typically performed by an HPCC Platform. As new functionality is added to the platform, new ECL test files are added to the Regression Suite.

## Running Regression Tests

### Prerequisites

* **A running HPCC Platform** (the one you want to test)
  * This can be local or remote
  * It can be a bare-metal installation or a containerized deployment

* **A terminal that can reach the running platform**
  * Your terminal can be a Windows command prompt, Windows PowerShell, macOS Terminal, or a Linux Bash terminal. The instructions in this document are for a Linux Bash terminal, so some commands may need minor tweaks to work in Windows terminals.

* **HPCC Client tools installed locally** - The Client Tools and Platform versions must match. For details about using the ecl command line interface, see the [HPCC Client Tools Documentation](https://hpccsystems.com/training/documentation/ecl-ide-and-client-tools/). 
* **Python 3.x (or later) installed locally**
* **A cloned repository of the HPCC Platform**

### Run Setup

This initializes the Regression Suite and creates all the files and queries needed for your environment.

From a terminal:

1. Change directory to the **regress** folder in the repository (`/HPCC-Platform/testing/regress`).

   For example:

   ```bash
   cd /mnt/c/Repos/HPCC-Platform/testing/regress
   ```

2. Run one of the following commands (depending on your environment):

   **For a Local Bare-Metal deployment:**

   ```bash
   ./ecl-test setup  
   ```

   **For a Remote Bare-Metal deployment:**

   ```bash
   ./ecl-test setup --server <remote_ecl_watch_ip>[:<remote_ecl_watch_port>]
   ```

   **For a Local Docker Desktop Helm deployment:**

   ```bash
   ./ecl-test setup --config ecl-test-azure.json
   ```

   **For a Remote Containerized deployment:**
   This runs the Regression Suite on a remote Containerized (AKS, Minikube, other K8s) Platform.

   ```bash
   ./ecl-test setup --config ecl-test-azure.json \
   --server <remote_ecl_watch_ip>[:<remote_ecl_watch_port>] 
   ```

   **Note**: To run on deployments using a custom configuration, you must create a json file similar to ecl-test-azure.json specifying the engines of interest, then pass it on the command line using the --config option.

## Run the Regression Suite Tests with the Regression Test Engine 
You can run the entire suite on a single engine or on all engines in your environment as specified in the config file.

**Examples for specific engines:**

**Tip**: The `-pq` option used in these examokes specifies parallel query execution to utilize multiple cores on your machine, reducing execution time. The `-pq 8` option in the examples specify using 8 cores. You should use half of your available cores.

For hThor only:

```bash
./ecl-test run -t hthor --config ecl-test-azure.json --pq 8 
```

For Thor only:

```bash
./ecl-test run -t thor --config ecl-test-azure.json --pq 8 
```

For Roxie only:

```bash
./ecl-test run -t roxie-workunit --config ecl-test-azure.json --pq 8 
```

**Note**: On a containerized system, the Roxie `-t` argument (target) is called `roxie-workunit`. On bare-metal, the `-t` argument is `roxie`.

For all engines:

```bash
./ecl-test run --config ecl-test-azure.json --pq 8 
```

**Examples for specific queries**

For a single query:

```bash
./ecl-test query -t hthor --config ecl-test-azure.json action1.ecl --pq 8
```

For a set is queries using a wildcard:

```bash
./ecl-test query -t hthor --config ecl-test-azure.json action*.ecl --pq 8
```

**Examples by deployment type:**

**For a Bare-Metal deployment:**

```bash
./ecl-test run  
```

**For a Local Docker Desktop Helm deployment:**

```bash
./ecl-test run --config ecl-test-azure.json
```

**For a Remote Containerized deployment:**

```bash
./ecl-test run --config ecl-test-azure.json \
--server <remote_ecl_watch_ip>[:<remote_ecl_watch_port>] 
```

**Note**: To run on deployments using a custom configuration, you must create a json file similar to ecl-test-azure.json specifying the engines of interest, then pass it on the command line using the --config option.

## Analyze the Results

When a suite of tests completes, a summary displays along with other information, including the target engine(s), the number of test cases run, a list of executed test cases, and their results with links to their workunit on ECL Watch. For example:

```text
-------------------------------------------------
Result:
Passing: 942
Failure: 28
-------------------------------------------------
```

Each test outputs its results to the command line. This output is also saved to a log file. Open the log file to review the results and identify failures.

For example:

```text
[Action] 853. Test: stepping3.ecl ( version: multiPart=true )
[Pass] 846. Pass stepping10.ecl ( version: multiPart=false, useExplicitSuper=true ) - W20250701-214710 (46 sec)
```

Any test that fails reports the failure and also creates a ZAP (Zipped Analysis Package) file. This allows you to share details with a colleague or someone on the HPCC Platform team. You can further investigate individual failures by examining the contents of the associated ZAP file.

```text
[Failure] 757. Fail soapcall_multihttpheader_proxy.ecl - W20250701-213726 (227 sec)
[Failure] 757. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20250701-213726
[Failure] 757. Zipped Analysis Package: ZAP file written to /home/username/HPCCSystems-regression/zap/ZAPReport_W20250701-213726_regress.zip.
```

You can examine the logs and/or look at the ZAP file contents for details that may help you debug the regression.

### Folders

**outputs**
The outputs folder contains the outputs (in XML format) from each query performed by the regression suite. These are compared to expected results.

**log**
The log folder contains all the outputs from operations performed by the regression suite.

**results**
The results folder contains the results of each query in XML format. These are the expected results used to compare with regression suite outputs.

**zap**
The zap folder contains all the ZAP report files created by the regression suite.
