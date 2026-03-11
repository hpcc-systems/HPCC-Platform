#!/usr/bin/env python3
"""
Configurat  ion File Processor

Processes configuration files with multiple sections and applies environment
variable replacements to XML files.
"""

import os
import sys
import argparse
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple


class ConfigSection:
    """Represents a section in the configuration file."""

    def __init__(self, name: str):
        self.name = name.lower()
        self.lines = []

    def add_line(self, line: str):
        """Add a line to this section."""
        self.lines.append(line)


class ConfigFile:
    """Parses and represents a configuration file."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.basename = Path(filepath).stem
        self.sections = {}
        self._parse()

    def _parse(self):
        """Parse the configuration file into sections."""
        self._parse_file(self.filepath)

    def _parse_file(self, filepath: str):
        """Parse a configuration file and any included files into sections.

        Include files (@filename) are processed immediately when encountered,
        allowing later definitions in the current file to override values from
        included files.

        Args:
            filepath: Path to the configuration file to parse
        """
        current_section = None

        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.rstrip('\n\r')

                # Skip comment lines starting with #
                if line.strip().startswith('#'):
                    continue

                # Check for include files starting with @
                if line.strip().startswith('@'):
                    include_filename = line.strip()[1:].strip()
                    if include_filename:
                        # Resolve relative paths relative to the directory of the current file
                        if not Path(include_filename).is_absolute():
                            include_path = Path(filepath).parent / include_filename
                        else:
                            include_path = Path(include_filename)

                        if include_path.exists():
                            # Process the included file immediately
                            self._parse_file(str(include_path))
                        else:
                            print(f"  Warning: Include file not found: {include_path}", file=sys.stderr)
                    continue

                # Check for section header
                section_match = re.match(r'^\[([^\]]+)\]$', line.strip())
                if section_match:
                    section_name = section_match.group(1).lower()
                    # Get or create section (to merge with existing if from include)
                    if section_name not in self.sections:
                        self.sections[section_name] = ConfigSection(section_name)
                    current_section = self.sections[section_name]
                elif current_section is not None and line.strip():
                    # Add non-empty lines to current section
                    current_section.add_line(line)

    def get_section(self, name: str) -> ConfigSection:
        """Get a section by name (case-insensitive)."""
        return self.sections.get(name.lower())

    def get_environment_replacements(self) -> List[Tuple[str, str]]:
        """Parse environment section and return list of (pattern, replacement) tuples.

        Supports two formats:
        1. pattern --> replacement : Direct pattern/replacement pairs
        2. name=value : Creates pattern 'name="[^"]*"' with replacement 'name="value"'
        """
        replacements = []
        env_section = self.get_section('environment')

        if env_section:
            for line in env_section.lines:
                # Check for pattern --> replacement format
                if ' --> ' in line:
                    parts = line.split(' --> ', 1)
                    pattern = parts[0].strip()
                    replacement = parts[1].strip()
                    replacements.append((pattern, replacement))
                else:
                    # Parse name=value assignments
                    match = re.match(r'^([^=]+)=(.*)$', line)
                    if match:
                        name = match.group(1).strip()
                        value = match.group(2).strip()
                        # Create pattern and replacement for XML attribute format
                        pattern = rf'{re.escape(name)}="[^"]*"'
                        replacement = f'{name}="{value}"'
                        replacements.append((pattern, replacement))

        return replacements

    def get_config_vars(self) -> Dict[str, str]:
        """Extract name=value pairs from config section.

        Returns:
            Dictionary of configuration variable names and values
        """
        config_vars = {}
        config_section = self.get_section('config')

        if config_section:
            for line in config_section.lines:
                # Skip pattern --> replacement format
                if ' --> ' in line:
                    continue
                # Parse name=value assignments
                match = re.match(r'^([^=]+)=(.*)$', line)
                if match:
                    name = match.group(1).strip()
                    value = match.group(2).strip()
                    config_vars[name] = value

        return config_vars

    def get_prepare_commands(self) -> List[str]:
        """Get list of commands from prepare section."""
        prepare_section = self.get_section('prepare')
        return prepare_section.lines if prepare_section else []

    def get_options(self) -> Dict[str, str]:
        """Get options from options section as name=value pairs."""
        options = {}
        options_section = self.get_section('options')

        if options_section:
            for line in options_section.lines:
                # Parse name=value assignments
                match = re.match(r'^([^=]+)=(.*)$', line)
                if match:
                    name = match.group(1).strip()
                    value = match.group(2).strip()
                    options[name] = value

        return options

    def get_tests(self) -> List[Tuple[str, str]]:
        """Get list of tests as (name, options) tuples."""
        tests = []
        tests_section = self.get_section('tests')

        if tests_section:
            for line in tests_section.lines:
                # Parse "name: options" format
                match = re.match(r'^([^:]+):(.*)$', line)
                if match:
                    name = match.group(1).strip()
                    options = match.group(2).strip()
                    tests.append((name, options))

        return tests


class XMLProcessor:
    """Processes XML files and replaces environment variables."""

    @staticmethod
    def replace_variables(input_file: str, output_file: str, replacements: List[Tuple[str, str]]):
        """
        Replace patterns in XML file with replacement strings.

        Args:
            input_file: Path to input XML file
            output_file: Path to output XML file
            replacements: List of (pattern, replacement) tuples to apply
        """
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Apply each replacement
        for pattern, replacement in replacements:
            print(f"    Replacing: {pattern} --> {replacement}")
            content = re.sub(pattern, replacement, content)

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(content)


class DeploymentManager:
    """Manages deployment of environment configuration to remote machines."""

    def __init__(self, dry_run: bool = False, initd_path: str = '/etc/init.d',
                 env_path: str = '/etc/HPCCSystems/environment.xml', sudo: bool = False,
                 cluster: str = ''):
        self.dry_run = dry_run
        self.initd_path = initd_path
        self.env_path = env_path
        self.sudo = sudo
        self.cluster = cluster

    def _execute_command(self, command: List[str], description: str) -> bool:
        """
        Execute a command or print it if in dry-run mode.

        Args:
            command: List of command arguments
            description: Human-readable description of the command

        Returns:
            True if successful (or dry-run), False otherwise
        """
        cmd_str = ' '.join(command)

        if self.dry_run:
            print(f"  [DRY-RUN] {description}")
            print(f"    Command: {cmd_str}")
            return True
        else:
            print(f"  {description}")
            print(f"    Executing: {cmd_str}")
            try:
                result = subprocess.run(command, check=True, capture_output=True, text=True)
                if result.stdout:
                    print(f"    Output: {result.stdout.strip()}")
                return True
            except subprocess.CalledProcessError as e:
                print(f"    Error: {e.stderr.strip()}", file=sys.stderr)
                return False

    def stop_services(self, ip_addresses: List[str]) -> bool:
        """
        Stop HPCC and Dafilesrv services on remote machines.

        Args:
            ip_addresses: List of IP addresses to stop services on

        Returns:
            True if all operations succeed, False otherwise
        """
        print("\nStopping services on remote machines...")

        for ip in ip_addresses:
            # Stop hpcc-init
            cluster_arg = f'-c {self.cluster} ' if self.cluster else ''
            hpcc_init = f'{self.initd_path}/hpcc-init {cluster_arg}stop'
            remote_cmd = f'sudo {hpcc_init}' if self.sudo else hpcc_init
            cmd = ['ssh', ip, remote_cmd]
            if not self._execute_command(cmd, f"Stopping hpcc-init on {ip}"):
                return False

            # Stop dafilesrv
            remote_cmd = f'sudo {self.initd_path}/dafilesrv stop' if self.sudo else f'{self.initd_path}/dafilesrv stop'
            cmd = ['ssh', ip, remote_cmd]
            if not self._execute_command(cmd, f"Stopping dafilesrv on {ip}"):
                return False

        return True

    def copy_environment(self, source_file: str, ip_addresses: List[str]) -> bool:
        """
        Copy environment.xml to remote machines.

        Args:
            source_file: Local path to environment.xml
            ip_addresses: List of IP addresses to copy to

        Returns:
            True if all operations succeed, False otherwise
        """
        print(f"\nCopying {source_file} to remote machines...")

        for ip in ip_addresses:
            cmd = ['sudo', 'scp', source_file, f'{ip}:{self.env_path}'] if self.sudo else ['scp', source_file, f'{ip}:{self.env_path}']
            if not self._execute_command(cmd, f"Copying environment.xml to {ip}:{self.env_path}"):
                return False

        return True

    def execute_commands(self, commands: List[str], ip_addresses: List[str]) -> bool:
        """
        Execute custom commands on remote machines.

        Args:
            commands: List of commands to execute
            ip_addresses: List of IP addresses to execute commands on

        Returns:
            True if all operations succeed, False otherwise
        """
        if not commands:
            return True

        print(f"\nExecuting {len(commands)} custom command(s) on remote machines...")

        for ip in ip_addresses:
            for command in commands:
                remote_cmd = f'sudo {command}' if self.sudo else command
                cmd = ['ssh', ip, remote_cmd]
                if not self._execute_command(cmd, f"Executing '{command}' on {ip}"):
                    return False

        return True

    def start_services(self, ip_addresses: List[str]) -> bool:
        """
        Start HPCC services on remote machines.

        Args:
            ip_addresses: List of IP addresses to start services on

        Returns:
            True if all operations succeed, False otherwise
        """
        print("\nStarting services on remote machines...")

        for ip in ip_addresses:
            cluster_arg = f'-c {self.cluster} ' if self.cluster else ''
            hpcc_init = f'{self.initd_path}/hpcc-init {cluster_arg}start'
            remote_cmd = f'sudo {hpcc_init}' if self.sudo else hpcc_init
            cmd = ['ssh', ip, remote_cmd]
            if not self._execute_command(cmd, f"Starting hpcc-init on {ip}"):
                return False

        return True

    def deploy(self, environment_file: str, ip_addresses: List[str], commands: List[str] = None) -> bool:
        """
        Deploy environment configuration to remote machines.

        Args:
            environment_file: Path to environment.xml file
            ip_addresses: List of IP addresses to deploy to
            commands: Optional list of commands to execute before starting services

        Returns:
            True if deployment succeeds, False otherwise
        """
        if not ip_addresses:
            print("Warning: No IP addresses specified for deployment")
            return False

        if commands is None:
            commands = []

        print(f"\n{'='*60}")
        print(f"Deploying environment to {len(ip_addresses)} machine(s)")
        print(f"{'='*60}")

        # Stop services
        if not self.stop_services(ip_addresses):
            print("\nDeployment failed: Could not stop services", file=sys.stderr)
            return False

        # Copy environment file
        if not self.copy_environment(environment_file, ip_addresses):
            print("\nDeployment failed: Could not copy environment file", file=sys.stderr)
            return False

        # Execute custom commands
        if not self.execute_commands(commands, ip_addresses):
            print("\nDeployment failed: Could not execute custom commands", file=sys.stderr)
            return False

        # Start services
        if not self.start_services(ip_addresses):
            print("\nDeployment failed: Could not start services", file=sys.stderr)
            return False

        print(f"\n{'='*60}")
        print("Deployment completed successfully")
        print(f"{'='*60}")
        return True


class TestRunner:
    """Manages test execution with testsocket."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def _run_testsocket(self, testsocket_path: str, args: List[str],
                       commandargs: List[str], output_file: str = None) -> str:
        """
        Run testsocket command and optionally redirect output.

        Args:
            testsocket_path: Path to testsocket executable
            args: Arguments to pass to testsocket
            commandargs: Additional arguments to append to args
            output_file: Optional file to redirect output to

        Returns:
            Command output as string
        """
        if output_file:
            args.extend(['-or', output_file])
        else:
            args.extend(['-o', 'out.log'])

        args.extend(commandargs)
        cmd = [testsocket_path] + args
        cmd_str = ' '.join(cmd)

        if self.dry_run:
            print(f"  [DRY-RUN] Would execute: {cmd_str}")
            return ""

        print(f"  Executing: {cmd_str}")

        try:
            if output_file:
                with open(output_file, 'w') as f:
                    result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
                    if result.returncode != 0:
                        print(f"    Error: {result.stderr.strip()}", file=sys.stderr)
                return ""
            else:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                return result.stdout
        except subprocess.CalledProcessError as e:
            print(f"    Error: {e.stderr.strip()}", file=sys.stderr)
            return ""
        except Exception as e:
            print(f"    Error: {e}", file=sys.stderr)
            return ""

    def _extract_filename_from_xml(self, xml_output: str) -> str:
        """
        Extract filename from XML output between <filename> tags.

        Args:
            xml_output: XML output containing filename tags

        Returns:
            Extracted filename or empty string
        """
        match = re.search(r"filename='([^']+)'", xml_output)
        if match:
            return match.group(1)
        return ""

    def run_tests(self, tests: List[Tuple[str, str]], config_vars: Dict[str, str],
                  options: Dict[str, str], test_file_basename: str, copy_events_arg: bool = False,
                  environment_replacements: List[Tuple[str, str]] = None, prepare_commands: List[str] = None) -> bool:
        """
        Run tests using testsocket.

        Args:
            tests: List of (test_name, test_options) tuples
            config_vars: Configuration variables from [config] section
            options: Options from [options] section
            test_file_basename: Base name of the test file
            copy_events_arg: Command-line flag to copy event files
            environment_replacements: List of (pattern, replacement) tuples from [environment] section
            prepare_commands: List of commands from [prepare] section

        Returns:
            True if all tests succeed, False otherwise
        """
        if not tests:
            return True

        if environment_replacements is None:
            environment_replacements = []
        if prepare_commands is None:
            prepare_commands = []

        # Get configuration values with defaults
        localroot = config_vars.get('LOCALROOT', '').rstrip('/')
        testsocket = f"{localroot}/opt/HPCCSystems/bin/testsocket" if localroot else '/opt/HPCCSystems/bin/testsocket'
        submit_ip = config_vars.get('SUBMIT_IP', '')
        roxie_port = config_vars.get('ROXIE_PORT', '')
        roxie_ips = config_vars.get('ROXIE_IPS', '')
        results_base = config_vars.get('RESULTS', 'results')
        testfile = options.get('testfile', '')

        if not submit_ip or not roxie_port:
            print("Warning: SUBMIT_IP or ROXIE_PORT not configured")
            return False

        roxie_ips_list = [ip.strip() for ip in roxie_ips.split(',')] if roxie_ips else []
        event_trace = options.get('eventTrace', '0') == '1'
        # Copy events if command-line flag is set OR copyEvents option is enabled
        copy_events = copy_events_arg or (options.get('copyEvents', '0') == '1')

        # Create results base directory if it doesn't exist (skip in dry-run)
        results_path = Path(results_base)
        if not self.dry_run:
            results_path.mkdir(parents=True, exist_ok=True)

        print(f"\n{'='*60}")
        print(f"Running {len(tests)} test(s)")
        print(f"{'='*60}")

        for test_name, test_options in tests:
            full_test_name = f"{test_file_basename}_{test_name}"
            print(f"\nTest: {full_test_name}")

            # Capture start timestamp
            start_time = time.time()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # Create test-specific directory structure: results/full_test_name/timestamp/
            test_dir = results_path / full_test_name / timestamp
            summary_txt = test_dir / "summary.txt"

            if not self.dry_run:
                test_dir.mkdir(parents=True, exist_ok=True)
                print(f"  Results directory: {test_dir}")

                # Write configuration settings to summary.txt
                with open(summary_txt, 'w') as f:
                    # Write [config] section
                    f.write("[config]\n")
                    for key, value in sorted(config_vars.items()):
                        f.write(f"{key}={value}\n")

                    # Write [options] section
                    if options:
                        f.write("\n[options]\n")
                        for key, value in sorted(options.items()):
                            f.write(f"{key}={value}\n")

                    # Write [environment] section
                    if environment_replacements:
                        f.write("\n[environment]\n")
                        for pattern, replacement in environment_replacements:
                            f.write(f"{pattern} --> {replacement}\n")

                    # Write [prepare] section
                    if prepare_commands:
                        f.write("\n[prepare]\n")
                        for cmd in prepare_commands:
                            f.write(f"{cmd}\n")

                    # Write test-specific settings
                    f.write("\n[test]\n")
                    f.write(f"name={test_name}\n")
                    f.write(f"full_name={full_test_name}\n")
                    f.write(f"timestamp={timestamp}\n")
                    f.write(f"options={test_options}\n")

                    # Add event trace header for later appending
                    if event_trace:
                        f.write("\n[event_files]\n")

                print(f"  Configuration written to: {summary_txt}")
            else:
                print(f"  [DRY-RUN] Would write configuration to: {summary_txt}")

            # a) Start event recording if enabled
            if event_trace and roxie_ips_list:
                print("  Starting event recording...")
                for ip in roxie_ips_list:
                    event_args = [f"{ip}:{roxie_port}"]
                    command_args = ["<control:startEventRecording options='all'/>"]
                    if int(roxie_port) > 9999:
                        event_args.append('-ssl')
                    self._run_testsocket(testsocket, event_args, command_args)

            # b) Run the actual test
            print("  Running test...")
            test_args = [f"{submit_ip}:{roxie_port}"]
            command_args = []

            # Add SSL flag if port > 9999
            if int(roxie_port) > 9999:
                test_args.append('-ssl')

            # Add delay option if specified
            delay = options.get('delay')
            if delay and str(delay).strip():
                test_args.extend(['-qd', str(delay).strip()])

            # Add threads option with default 0
            threads = options.get('threads', '0')
            if threads and str(threads).strip():
                test_args.append(f'-u{str(threads).strip()}')

            # Add extra parameters
            test_args.extend(['-ts', '-q'])

            # Add test file (command_args guarantees this goes after the -or output arg)
            if testfile:
                command_args.extend(['-ff', testfile])

            # Add test options (command_args guarantees this goes after the -or output arg)
            if test_options.strip():
                command_args.extend(test_options.split())

            # Run test and redirect to results file in timestamp directory
            results_file = test_dir / "results.xml"
            self._run_testsocket(testsocket, test_args, command_args, str(results_file))


            # Calculate elapsed time
            elapsed_time = time.time() - start_time
            print(f"  Results written to: {results_file}")
            print(f"  Elapsed time: {elapsed_time:.2f} seconds")

            # Extract summary statistics from results file
            if not self.dry_run and results_file.exists():
                try:
                    with open(results_file, 'r', encoding='utf-8') as f:
                        content = f.read()

                    # Extract all <SummaryStats>...</SummaryStats> sections
                    stats_pattern = r'<SummaryStats>(.*?)</SummaryStats>'
                    matches = re.findall(stats_pattern, content, re.DOTALL)

                    if matches:
                        stats_file = test_dir / "stats.txt"
                        with open(stats_file, 'w') as f:
                            for i, stats in enumerate(matches, 1):
                                if i > 1:
                                    f.write("\n")
                                f.write(stats.strip())
                                f.write("\n")
                        print(f"  Statistics written to: {stats_file}")

                        # Run extract-roxie-timings.py to process statistics
                        script_dir = Path(__file__).parent
                        extract_script = script_dir / "extract-roxie-timings.py"
                        summary_csv = test_dir / "stats-summary.csv"

                        if extract_script.exists():
                            try:
                                with open(summary_csv, 'w') as csv_out:
                                    result = subprocess.run(
                                        ['python3', str(extract_script), str(stats_file), '--avgonly'],
                                        stdout=csv_out,
                                        stderr=subprocess.PIPE,
                                        text=True,
                                        check=True
                                    )
                                print(f"  Summary statistics written to: {summary_csv}")
                            except subprocess.CalledProcessError as e:
                                print(f"  Warning: Failed to extract timing summary: {e.stderr.strip()}", file=sys.stderr)

                            full_summary_csv = test_dir / "stats-full-summary.csv"
                            try:
                                with open(full_summary_csv, 'w') as csv_out:
                                    result = subprocess.run(
                                        ['python3', str(extract_script), str(stats_file), '--summaryonly'],
                                        stdout=csv_out,
                                        stderr=subprocess.PIPE,
                                        text=True,
                                        check=True
                                    )
                                print(f"  Full summary statistics written to: {full_summary_csv}")
                            except subprocess.CalledProcessError as e:
                                print(f"  Warning: Failed to extract full timing summary: {e.stderr.strip()}", file=sys.stderr)
                        else:
                            print(f"  Warning: extract-roxie-timings.py not found at {extract_script}", file=sys.stderr)


                except Exception as e:
                    print(f"  Warning: Failed to extract statistics: {e}", file=sys.stderr)

                # Delete results file after processing
                try:
                    results_file.unlink()
                    print(f"  Deleted results file: {results_file}")
                except Exception as e:
                    print(f"  Warning: Failed to delete results file: {e}", file=sys.stderr)

            # c) Stop event recording if enabled
            if event_trace and roxie_ips_list:
                print("  Stopping event recording...")

                if not self.dry_run:

                    with open(summary_txt, 'a') as f:
                        f.write(f"\n[event_files]\n")

                        for ip in roxie_ips_list:
                            event_args = [f"{ip}:{roxie_port}"]
                            if int(roxie_port) > 9999:
                                event_args.append('-ssl')
                            command_args = ["<control:stopEventRecording/>"]
                            output = self._run_testsocket(testsocket, event_args, command_args)
                            filename = self._extract_filename_from_xml(output)
                            if filename:
                                # Always append to summary.txt in timestamp directory
                                f.write(f"{ip},{filename}\n")

                            # Copy event trace file only if copy_events is enabled
                            if copy_events:
                                local_filename = Path(filename).name
                                local_path = test_dir / f"{ip}_{local_filename}"

                                try:
                                    cmd = ['scp', f'{ip}:{filename}', str(local_path)]
                                    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
                                    print(f"    Copied to: {local_path}")
                                except subprocess.CalledProcessError as e:
                                    print(f"    Warning: Failed to copy event trace file: {e.stderr.strip()}", file=sys.stderr)

            # Append summary to CSV file (skip in dry-run)
            if not self.dry_run:
                summary_file = results_path / f"summary_{full_test_name}.csv"
                file_exists = summary_file.exists()

                # Check if stats-summary.csv was generated and read it
                summary_csv = test_dir / "stats-summary.csv"
                stats_header = ""
                stats_data = ""

                if summary_csv.exists():
                    try:
                        with open(summary_csv, 'r') as csv_f:
                            lines = csv_f.readlines()
                            if len(lines) >= 1:
                                # First line is header (without newline)
                                stats_header = lines[0].strip()
                            if len(lines) >= 2:
                                # Last line is data (without newline)
                                stats_data = lines[-1].strip()
                    except Exception as e:
                        print(f"  Warning: Failed to read stats-summary.csv: {e}", file=sys.stderr)

                with open(summary_file, 'a') as f:
                    # Write header if file is new
                    if not file_exists:
                        header = "timestamp,elapsed_time"
                        if stats_header:
                            header += "," + stats_header
                        f.write(header + "\n")
                    # Write data row
                    data = f"{timestamp},{elapsed_time:.2f}"
                    if stats_data:
                        data += "," + stats_data
                    f.write(data + "\n")

                print(f"  Summary appended to: {summary_file}")

        print(f"\n{'='*60}")
        print("Test execution completed")
        print(f"{'='*60}")
        return True


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Process configuration files and apply environment changes to XML files.'
    )

    parser.add_argument(
        'config_files',
        nargs='+',
        help='List of configuration files to process'
    )

    parser.add_argument(
        '--roxie-ips',
        default=os.environ.get('ROXIE_IPS', ''),
        help='Comma-separated list of Roxie node IPs (default: from environment)'
    )

    parser.add_argument(
        '--submit-ip',
        default=os.environ.get('SUBMIT_IP', ''),
        help='IP address used to submit queries to Roxie (default: from environment)'
    )

    parser.add_argument(
        '--roxie-port',
        default=os.environ.get('ROXIE_PORT', ''),
        help='Port number associated with Roxies (default: from environment)'
    )

    parser.add_argument(
        '--input-xml',
        default=None,
        help='Input XML file to process (default: from [config] INPUT or environment.in.xml)'
    )

    parser.add_argument(
        '--output-xml',
        default='environment.out.xml',
        help='Output XML file to write (default: environment.out.xml)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Trace the deployment steps without executing them'
    )

    parser.add_argument(
        '--deploy',
        action='store_true',
        help='Deploy the environment configuration to remote machines'
    )

    parser.add_argument(
        '--copy-events',
        action='store_true',
        help='Copy event trace files from remote machines to results directory'
    )

    parser.add_argument(
        '--delay',
        default=None,
        help='Delay for testsocket execution (adds -qd <delay> to test arguments)'
    )

    parser.add_argument(
        '--threads',
        default=None,
        help='Number of threads for testsocket execution (adds -u<threads> to test arguments, defaults to 0)'
    )

    parser.add_argument(
        '--sudo',
        action='store_true',
        help='Prefix remote commands and scp with sudo'
    )

    return parser.parse_args()


def build_config_value(name: str, system_env: Dict[str, str],
                       config_vars: Dict[str, str],
                       cmd_args: argparse.Namespace) -> str:
    """
    Build configuration value with proper precedence.

    Precedence order (highest to lowest):
    1. Command-line arguments
    2. Configuration file [config] section
    3. System environment variables

    Args:
        name: Name of the configuration variable
        system_env: System environment variables
        config_vars: Configuration file [config] section variables
        cmd_args: Parsed command-line arguments

    Returns:
        Configuration value or empty string if not found
    """
    # Check command-line arguments (highest precedence)
    arg_name = name.lower().replace('_', '-')
    cmd_value = getattr(cmd_args, arg_name.replace('-', '_'), '')
    if cmd_value:
        return cmd_value

    # Check config file [config] section
    if name in config_vars:
        return config_vars[name]

    # Check system environment variables (lowest precedence)
    if name in system_env:
        return system_env[name]

    return ''


def main():
    """Main entry point for the script."""
    args = parse_arguments()

    # Capture system environment variables
    system_env = {
        'ROXIE_IPS': os.environ.get('ROXIE_IPS', ''),
        'SUBMIT_IP': os.environ.get('SUBMIT_IP', ''),
        'ROXIE_PORT': os.environ.get('ROXIE_PORT', '')
    }

    # Process each configuration file
    for config_path in args.config_files:
        print(f"Processing configuration file: {config_path}")

        try:
            config = ConfigFile(config_path)
            print(f"  Basename: {config.basename}")

            # Get configuration variables from [config] section
            config_vars = config.get_config_vars()

            # Build final configuration values with proper precedence
            # Precedence: command-line > [config] section > system environment
            roxie_ips = build_config_value('ROXIE_IPS', system_env, config_vars, args)
            submit_ip = build_config_value('SUBMIT_IP', system_env, config_vars, args)
            roxie_port = build_config_value('ROXIE_PORT', system_env, config_vars, args)

            # Determine input file: command-line > [config] INPUT > default
            input_xml = args.input_xml
            if not input_xml:
                input_xml = config_vars.get('INPUT', 'environment.in.xml')

            print(f"\n  Configuration values (after precedence resolution):")
            print(f"    INPUT = {input_xml}")
            if roxie_ips:
                print(f"    ROXIE_IPS = {roxie_ips}")
            if submit_ip:
                print(f"    SUBMIT_IP = {submit_ip}")
            if roxie_port:
                print(f"    ROXIE_PORT = {roxie_port}")

            # Parse ROXIE_IPS list for deployment
            roxie_ips_list = []
            if roxie_ips:
                roxie_ips_list = [ip.strip() for ip in roxie_ips.split(',')]

            # Get environment replacements from [environment] section only
            replacements = config.get_environment_replacements()
            print(f"\n  Found {len(replacements)} replacement(s) from [environment] section")

            # Display the replacements
            if replacements:
                print("\n  Replacements:")
                for pattern, replacement in replacements:
                    print(f"    {pattern} --> {replacement}")

            # Get prepare commands
            commands = config.get_prepare_commands()
            if commands:
                print(f"\n  Found {len(commands)} prepare command(s):")
                for cmd in commands:
                    print(f"    {cmd}")

            # Get options
            options = config.get_options()
            if args.delay is not None:
                options['delay'] = args.delay
            if args.threads is not None:
                options['threads'] = args.threads
            if args.sudo:
                options['sudo'] = '1'

            if options:
                print(f"\n  Found {len(options)} option(s):")
                for name, value in options.items():
                    print(f"    {name} = {value}")

            # Get tests
            tests = config.get_tests()
            if tests:
                print(f"\n  Found {len(tests)} test(s):")
                for test_name, test_opts in tests:
                    print(f"    {test_name}: {test_opts}")

            # Process XML file if it exists
            if os.path.exists(input_xml):
                print(f"\n  Applying replacements to {input_xml}...")
                XMLProcessor.replace_variables(
                    input_xml,
                    args.output_xml,
                    replacements
                )
                print(f"  Output written to: {args.output_xml}")

                # Deploy if requested
                if args.deploy:
                    if replacements:
                        remoteroot = config_vars.get('REMOTEROOT', '').rstrip('/')
                        initd_path = f"{remoteroot}/etc/init.d" if remoteroot else '/etc/init.d'
                        env_path = f"{remoteroot}/etc/HPCCSystems/environment.xml" if remoteroot else '/etc/HPCCSystems/environment.xml'
                        use_sudo = options.get('sudo', '0') == '1'
                        cluster = config_vars.get('CLUSTER', '')
                        deployment = DeploymentManager(dry_run=args.dry_run, initd_path=initd_path, env_path=env_path, sudo=use_sudo, cluster=cluster)
                        if not deployment.deploy(args.output_xml, roxie_ips_list, commands):
                            sys.exit(1)
                    else:
                        print("\n  Skipping deployment: No environment replacements defined")

                # Run tests if tests are defined
                if tests:
                    test_runner = TestRunner(dry_run=args.dry_run)
                    if not test_runner.run_tests(tests, config_vars, options, config.basename,
                                                args.copy_events, replacements, commands):
                        sys.exit(1)
            else:
                print(f"\n  Warning: Input XML file not found: {input_xml}")

            print()

        except FileNotFoundError:
            print(f"  Error: Configuration file not found: {config_path}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"  Error processing {config_path}: {e}", file=sys.stderr)
            sys.exit(1)

    print("Processing complete.")


if __name__ == '__main__':
    main()
