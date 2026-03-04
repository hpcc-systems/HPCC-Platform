# Configuration File Processor

This Python script processes configuration files with multiple sections and applies environment variable replacements to XML files. It supports automated deployment to remote machines, test execution with timing analysis, and event tracing.

## Configuration File Format

Configuration files support multiple sections and can include other configuration files using `@filename` syntax.

### File Inclusion

Include other configuration files by adding lines starting with `@`:
```
@common.config
@settings/defaults.config
```

**Important**: Include files are processed **immediately** when encountered, so order matters. This allows you to:
1. Include a file with default values at the top
2. Override specific values later in the current file

Example:
```
# Load defaults first
@defaults.config

[config]
# Override specific defaults
ROXIE_PORT=9999
```

Relative paths are resolved relative to the including file's directory. Sections from included files are merged with sections in the current file, with later definitions taking precedence.

### [config]
Contains name=value assignments for configuration variables. These values override system 
environment variables but can be overridden by command-line arguments.

Supported variables:
- `ROXIE_IPS` - Comma-separated list of Roxie node IP addresses
- `SUBMIT_IP` - IP address used to submit queries to Roxie
- `ROXIE_PORT` - Port number associated with Roxies
- `INPUT` - Input XML file path (overrides default if --input-xml not specified)
- `TESTFILE` - Test file to pass to testsocket with -ff option
- `LOCALROOT` - Prefix for local paths (e.g., testsocket location). Default: empty
- `REMOTEROOT` - Prefix for remote paths (e.g., /etc/init.d, environment.xml). Default: empty
- `RESULTS` - Base directory for test results (default: `results`)

Path construction:
- testsocket: `$LOCALROOT/opt/HPCCSystems/bin/testsocket`
- init.d: `$REMOTEROOT/etc/init.d`
- environment.xml: `$REMOTEROOT/etc/HPCCSystems/environment.xml`

### [environment]
Contains replacements to be applied to the XML file. Supports two formats:
- `name=value` - Creates XML attribute pattern replacements
- `pattern --> replacement` - Direct regex pattern/replacement pairs

### [prepare]
Contains a list of commands, one per line. These commands will be executed on each remote node 
after copying the environment file but before starting HPCC services.

### [options]
Contains name=value assignments for additional options.

Supported options:
- `eventTrace` - Set to 1 to enable event recording during tests (default: 0)
- `copyEvents` - Set to 1 to copy event trace files from remote machines (default: 0)

### [tests]
Contains test definitions in the format "name: options". Each test will be executed after 
successful deployment using testsocket. Test options are passed directly to testsocket.

Example:
```
basic: --timeout=30
extreme: --load=high --duration=60
```

### Comments
Lines starting with `#` are treated as comments and ignored.

## Usage

```bash
python config_processor.py <config_file> [options]
```

### Required Arguments
- `config_file` - Path to the configuration file to process

### Optional Arguments
- `--input-xml <path>` - Path to the XML file to modify (default: /etc/HPCCSystems/environment.xml)
- `--roxie-ips <ips>` - Comma-separated list of Roxie node IP addresses (overrides [config])
- `--submit-ip <ip>` - IP address for query submission (overrides [config])
- `--roxie-port <port>` - Roxie port number (overrides [config])
- `--dry-run` - Preview changes without modifying files or executing commands
- `--copy-events` - Copy event trace files from remote machines to local results directory

### Examples

Basic usage with a configuration file:
```bash
python config_processor.py test-config.txt
```

Override configuration with command-line arguments:
```bash
python config_processor.py test-config.txt --roxie-ips 10.0.0.1,10.0.0.2 --submit-ip 10.0.0.1
```

Preview changes without applying them:
```bash
python config_processor.py test-config.txt --dry-run
```

Run tests with event tracing and copy event files:
```bash
python config_processor.py test-config.txt --copy-events
```

## Test Execution and Results

When tests are defined in the [tests] section, the script will:

1. **Deploy Configuration**: Copy modified environment.xml and execute [prepare] commands on remote nodes
2. **Create Results Directory**: `$RESULTS/{testname}/{timestamp}/` for each test
3. **Generate summary.txt**: Records all configuration values, test parameters, and metadata before execution
4. **Execute Test**: Run testsocket with specified options
5. **Capture Results**: Save results.xml from testsocket output
6. **Extract Statistics**: Parse <SummaryStats> XML and save to stats.txt
7. **Process Timings**: Run extract-roxie-timings.py to generate stats-summary.csv
8. **Copy Events** (if --copy-events): Copy trace files from remote machines
9. **Generate CSV Summary**: Create statssummary.csv with configuration and statistics

### Results Directory Structure

```
results/
  {testname}/
    {timestamp}/
      summary.txt           # Complete configuration and test metadata
      results.xml           # Full testsocket output
      stats.txt            # Extracted <SummaryStats> XML
      stats-summary.csv    # Processed timing statistics
      statssummary.csv     # Combined configuration and statistics
      events/              # Event trace files (if --copy-events used)
        {hostname}-{pid}.json
```

### summary.txt Format

The summary.txt file contains:
- **[config]** section with all configuration values (including defaults)
- **[environment]** section with XML replacement patterns
- **[prepare]** section with deployment commands
- **[options]** section with runtime options
- **timestamp** - ISO 8601 timestamp when test was started
- **testsocket_command** - Full command line executed
- **event_files** - List of event trace files (if event tracing enabled)

### Statistics Extraction

If extract-roxie-timings.py is present in the same directory, the script will:
1. Extract all <SummaryStats> tags from results.xml and save to stats.txt
2. Run extract-roxie-timings.py on stats.txt to generate stats-summary.csv
3. Merge statistics into statssummary.csv alongside configuration values

The statssummary.csv contains:
- All [config] values as columns
- All statistics from stats-summary.csv (mean times, counts, etc.)

## How It Works

1. Parse specified configuration file(s), processing @include directives recursively
2. Merge configuration sections from included files with main configuration
3. Determine configuration values using precedence: CLI args > config file > environment
4. Apply XML replacements from [environment] section to input XML file
6. Deploy configuration to remote machines:
   - Stop HPCC services on all Roxie nodes
   - Copy modified environment.xml to each node
   - Execute [prepare] commands on each node
   - Start HPCC services on all nodes
7. Execute tests from [tests] section:
   - Create timestamped results directory for each test
   - Generate summary.txt with complete configuration
   - Run testsocket with specified options
   - Capture and process results
   - Extract statistics and generate CSV summaries
   - Copy event trace files if requested

**Note**: Deployment requires SSH keys configured for passwordless authentication. If no [environment] replacements are defined, deployment is skipped automatically.

## Configuration Precedence

Configuration values are resolved in the following order (highest priority first):

1. **Command-line arguments** (--roxie-ips, --submit-ip, --roxie-port)
2. **[config] section** in the specified config file (including values from @included files)
3. **System environment variables**

### Precedence Example

Given:
- System environment: `ROXIE_IPS=192.168.0.1,192.168.0.2`
- Config file `[config]`: `ROXIE_IPS=192.168.2.10,192.168.2.11`
- Command line: `--roxie-ips=10.0.0.1,10.0.0.2`

The final value will be: `10.0.0.1,10.0.0.2` (command line wins)

**Note**: When using @include files, later definitions override earlier ones within the same configuration file processing.

## Dry-Run Mode

Use `--dry-run` to preview what would be executed without actually running commands or modifying files:

```bash
python config_processor.py --dry-run sample_config.txt
```

In dry-run mode:
- XML replacements are still shown
- SSH/SCP commands are displayed but not executed
- Test commands are shown but not run
- Results directories are still created for structure verification
- Summary files are still written for preview

## Sample Configuration File

```
# Configuration file with comments
@common-settings.config   # Include shared settings

[config]
ROXIE_IPS=192.168.1.10,192.168.1.11,192.168.1.12
SUBMIT_IP=192.168.1.10
ROXIE_PORT=9876
LOCALROOT=/usr/local
REMOTEROOT=/opt/hpcc
RESULTS=my_test_results
TESTFILE=queries.txt

[environment]
# Simple attribute replacements
TEST_VAR=sample_value
OLD_VALUE=new_value

# Regex pattern replacements
old_server_name --> new_server_name
192\.168\.0\.\d+ --> 10.0.0.1

[prepare]
# Commands to run after deploying environment.xml
start_roxie_cluster
check_cluster_health
deploy_queries

[options]
eventTrace=1
copyEvents=1

[tests]
basic_test: --timeout=30
performance_test: --timeout=60 --load=high
smoke_test: --quick
```

Note: The [environment] section supports two formats:
- `name=value` - Creates XML attribute pattern replacements
- `pattern --> replacement` - Direct regex pattern/replacement pairs

## Dependencies

- Python 3.x
- `extract-roxie-timings.py` (optional, for statistics processing) - must be in same directory as config_processor.py
- SSH access to remote Roxie nodes with passwordless key-based authentication
- testsocket executable (part of HPCC Systems installation)

## Advanced Features

### Include Files

Use `@filename` to include other configuration files. This allows modular configuration management:

```
# main-config.txt
@common/defaults.config
@environment/production.config

[tests]
prod_test: --timeout=60
```

Included files are processed recursively, and sections are merged. Later definitions override earlier ones within the same section.

### Event Tracing

Enable event tracing to record detailed execution information:

1. Set `eventTrace=1` in [options] section or testing.defaults
2. Events are recorded to `/var/log/HPCCSystems/myesp/trace_{timestamp}_{pid}.json` on each Roxie node
3. Use `--copy-events` or set `copyEvents=1` in [options] to automatically copy trace files to results directory

Event files are listed in summary.txt and copied to `results/{testname}/{timestamp}/events/`.

### Path Customization

Use LOCALROOT and REMOTEROOT to adapt to different deployment environments:

- **LOCALROOT**: Prefix for local paths (where testsocket is installed locally)
- **REMOTEROOT**: Prefix for remote paths (where services and configs are on remote machines)

Example for containerized environments:
```
[config]
LOCALROOT=/opt/mycompany
REMOTEROOT=/data/hpcc
```

This changes paths to:
- testsocket: `/opt/mycompany/opt/HPCCSystems/bin/testsocket`
- init.d: `/data/hpcc/etc/init.d`
- environment.xml: `/data/hpcc/etc/HPCCSystems/environment.xml`
