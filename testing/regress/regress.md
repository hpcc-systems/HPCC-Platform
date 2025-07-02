# Using the HPCC Platform Regression Suite

## Why Regression Testing is Important

Adding new features and making improvements is crucial for any product's evolution. However, these changes can sometimes inadvertently introduce regressions – previously fixed bugs or unexpected issues in existing functionality.

The Regression Suite acts as a safety net, systematically verifying that the core aspects of our application continue to function as expected after any modifications. By running these scripts, we can proactively identify and address regressions early in the development cycle, preventing them from reaching our users and ensuring a smoother, more stable experience. Think of it as a comprehensive health check for our software after any significant change.

For official releases, the regression suite is run automatically as part of the build cycle. However, individual developers can (and should) use it when making significant changes, before issuing a Pull Request.

## When to Use the Regression Suite

The Regression Suite should be executed at several key points during the software development lifecycle:

* After each new build: Following any significant code changes, merging of feature branches, or bug fixes, running the Regression Suite helps to immediately detect any unintended side effects.
* Before a release candidate: As we prepare a version for final testing and potential release, a thorough run of the Regression Suite is mandatory to confirm overall stability and identify any last-minute regressions.
* Periodically for maintenance releases: Even for smaller updates and patches, executing a subset or the full Regression Suite can help ensure that these changes haven't negatively impacted existing functionality.

By consistently utilizing the Regression Suite at these critical junctures, we maintain a high level of quality and minimize the risk of introducing regressions into our production environment.

## How to Use the Regression Suite

### Prerequisites

* A running HPCC Platform (the one you want to test)

    This can be local or remote.
    It can be a bare-metal installation or a containerized deployment.

* A terminal that can reach the running platform.

    Your terminal can be a Windows command prompt, Windows PowerShell, macOS Terminal, or a Linux Bash terminal. The instructions in this document are for a Linux Bash terminal, so some commands may need minor tweaks to work in Windows terminals.

* HPCC Client tools installed locally
* Python 3.x (or later) installed locally
* A cloned repository of the HPCC platform

### Run Setup

This initializes the engines you want to use and creates all the files and queries needed.

From a terminal:

1. Change directory to the **regress** folder in the repository (/HPCC-Platform/testing/regress).

    For example:

    ```bash
    cd /mnt/c/Repos/HPCC-Platform/testing/regress
    ```

2. Run one of the following commands:

   **For a Bare-Metal deployment:**

   ```bash
   ./ecl-test setup  
   ```

   **For a Local Docker Desktop Helm deployment:**

   ```bash
   ./ecl-test setup --config ecl-test-azure.json
   ```

   **For a Remote Containerized deployment:**

   ```bash
   ./ecl-test setup --config ecl-test-azure.json \
   --server <remote_ecl_watch_ip>[:<remote_ecl_watch_port>] 
   ```

   **Note**:  To run on deployments using a custom configuration, you must create a json file similar to ecl-test-azure.json specifying the engines of interest, then pass it on the command line using the --config option.

## Run ECL Test

You can run the entire suite on a single engine or on all engines in your environment as specified in the config file.

For example, for hThor only:

```bash
./ecl-test run -t hthor --config ecl-test-azure.json --pq 8 
```

Or for example, for Thor only:

```bash
./ecl-test run -t thor --config ecl-test-azure.json --pq 8 
```

Or for example, for Roxie only:

```bash
./ecl-test run -t roxie-workunit --config ecl-test-azure.json --pq 8 
```

**Note**: On a containerized system, the Roxie -t argument (target) is called roxie-workunit. On bare-metal, the -t argument is roxie.

Or for all engines:

```bash
./ecl-test run --config ecl-test-azure.json --pq 8 
```

**Tip**: The *-pq* option specifies parallel query execution to utilize multiple cores on your machine, reducing execution time. The *-pq 8* option in the example specifies to use 8 cores. You should use half of the available cores.

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

**Note**:  To run on deployments using a custom configuration, you must create a json file similar to ecl-test-azure.json specifying the engines of interest, then pass it on the command line using the --config option.

## Analyze the Results

When a suite of tests completes, a summary displays. For example:

```text
-------------------------------------------------
Result:
Passing: 942
Failure: 28
-------------------------------------------------
```

Each test conducted outputs its results to the command line. This output is also saved to a log file. Open the log file to review the results and identify failures.

For example:

```text
[Action] 853. Test: stepping3.ecl ( version: multiPart=true )
[Pass] 846. Pass stepping10.ecl ( version: multiPart=false, useExplicitSuper=true ) - W20250701-214710 (46 sec)
```

Any test that fails, reports the failure and also creates a ZAP (Zipped Analysis Package) file. This allows you to share details with a colleague or someone on the HPCC Platform team. You can further investigate individual failures by looking at the contents of the associated ZAP file.

```text
[Failure] 757. Fail soapcall_multihttpheader_proxy.ecl - W20250701-213726 (227 sec)
[Failure] 757. URL http://127.0.0.1:8010/?Widget=WUDetailsWidget&Wuid=W20250701-213726
[Failure] 757. Zipped Analysis Package: ZAP file written to /home/username/HPCCSystems-regression/zap/ZAPReport_W20250701-213726_regress.zip.
```

You can examine the logs and/or look at the ZAP file contents for details that may help you debug the regression.

### outputs

The outputs folder contains the outputs (in XML format) from each query performed by the regression suite. These are compared to expected results.

### log

The log folder contains all the outputs from operations performed by the regression suite.

### results

The results folder contains the results of each query in XML format. These are the expected results used to compare with regression suite outputs.

### zap

The zap folder contains all the ZAP report files created by the  regression suite.

## Cleanup

After running the regression suite, you may want to cleanup your platform by removing the workunits it created.

```bash
./ecl-test run --cleanup [mode]
```

Or

```bash
./ecl-test query --cleanup [mode]
```

Supported *modes*  are 'workunits', 'passed', and 'none'. If omitted, the default is 'none'.

* **workunits** - all passed and failed workunits are deleted.

* **passed** - only the passed workunits of the queries executed are deleted.  

* **none** - no workunits created during the current run command are deleted.
