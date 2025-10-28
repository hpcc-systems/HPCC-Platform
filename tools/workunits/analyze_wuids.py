#!/usr/bin/env python3

'''
/*#############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################ */
'''

"""
analyze_wuids.py - Analyze workunits from ESP WsWorkunits service

This script fetches detailed information about specified workunits from an
ESP server's WsWorkunits service.

Usage:
    analyze_wuids.py <espserver:port> <wuid1> <wuid2> ...
    analyze_wuids.py <espserver:port> -f <wuids-file>
    analyze_wuids.py <espserver:port> -f <wuids-file> -e <error-patterns-file>
    cat <wuids-file> | analyze_wuids.py <espserver:port>

Arguments:
    espserver:port  ESP server address (e.g., localhost:8010)
    wuid1 wuid2     One or more workunit IDs
    -f file         Read WUIDs from file (one per line)
    -e file         File containing error patterns to match (one per line)
                    When specified, only workunits with matching errors are displayed
    -v              Show verbose information including timings
    -s              Show summary table only
    stdin           Read WUIDs from standard input (one per line)

Examples:
    # Analyze specific WUIDs
    analyze_wuids.py localhost:8010 W20240101-120000 W20240101-120001

    # Read WUIDs from a file
    analyze_wuids.py localhost:8010 -f wuids.txt

    # Filter to specific error patterns
    analyze_wuids.py localhost:8010 -f wuids.txt -e error_patterns.txt

    # Pipe WUIDs from another command and filter by errors
    ./get_wuids.py localhost:8010 2024-01-01 2024-01-01 -s failed | tail -n +3 | analyze_wuids.py localhost:8010 -e errors.txt

The script connects to the ESP server and queries the WUInfo service endpoint
for each workunit, displaying detailed information including state, owner,
jobname, cluster, timing information, and the first ERROR exception if present.

When using the -e flag, only workunits whose first ERROR exception matches
one of the specified patterns (case-insensitive substring match) will be
displayed. This is useful for filtering large numbers of workunits to find
specific error types.
"""

import sys
import argparse
import requests
from requests.auth import HTTPBasicAuth
import json
import re
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from datetime import datetime

def parse_error_info(error_message):
    """Parse graph name and worker number from error message.

    Returns dict with 'graph_name' and 'worker_number' or None if not found.
    Examples:
        "Graph graph30[2]" -> graph_name="graph30", subgraph="2"
        "WORKER #118" -> worker_number="118"
        "Graph graph149, The machine 100.65.250.75:21200" -> graph_name="graph149", machine_endpoint="100.65.250.75:21200"
    """
    info = {'graph_name': None, 'subgraph_id': None, 'worker_number': None, 'machine_endpoint': None}

    # Parse graph name: "Graph <graphName>[<subgraphID>]"
    graph_match = re.search(r'Graph\s+(\w+)\[(\d+)\]', error_message, re.IGNORECASE)
    if graph_match:
        info['graph_name'] = graph_match.group(1)
        info['subgraph_id'] = graph_match.group(2)
    else:
        # Try alternate pattern: "Graph <graphName>," (without subgraph ID)
        graph_match_alt = re.search(r'Graph\s+(\w+),', error_message, re.IGNORECASE)
        if graph_match_alt:
            info['graph_name'] = graph_match_alt.group(1)

    # Parse worker number: "WORKER #<number>"
    worker_match = re.search(r'WORKER\s+#(\d+)', error_message, re.IGNORECASE)
    if worker_match:
        info['worker_number'] = worker_match.group(1)

    # Parse machine endpoint: "The machine <ip>:<port>"
    machine_match = re.search(r'[Tt]he machine\s+(\d+\.\d+\.\d+\.\d+:\d+)', error_message)
    if machine_match:
        info['machine_endpoint'] = machine_match.group(1)

    return info

def get_workunit_xml(esp_url, wuid, auth=None, verbose=False):
    """Fetch workunit XML for parsing process information."""
    if not esp_url.startswith(('http://', 'https://')):
        esp_url = f"http://{esp_url}"

    url = f"{esp_url}/WsWorkunits/WUFile"
    params = {
        'Wuid': wuid,
        'Type': 'XML'
    }

    try:
        response = requests.get(url, params=params, auth=auth, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        if verbose:
            print(f"  [VERBOSE] ERROR fetching workunit XML: {e}", file=sys.stderr)
        return None

def fetch_helper_file(esp_url, wuid, filename, auth=None, verbose=False):
    """Fetch a helper file (like dmesg.log) from the workunit."""
    if not esp_url.startswith(('http://', 'https://')):
        esp_url = f"http://{esp_url}"

    url = f"{esp_url}/WsWorkunits/WUFile"
    params = {
        'Wuid': wuid,
        'Name': filename,
        'Type': 'postmortem'  # Required for postmortem files
    }

    try:
        response = requests.get(url, params=params, auth=auth, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        # Log the error if verbose mode is enabled
        if verbose:
            print(f"  [VERBOSE] ERROR fetching {filename}: {e}", file=sys.stderr)
        return None

def analyze_oom_in_dmesg(dmesg_content):
    """Analyze dmesg.log for OOM killer invocations and extract memory info.

    Returns dict with 'oom_detected' (bool), 'process_killed', 'memory_info', or None
    """
    if not dmesg_content:
        return None

    # Search for OOM killer invocation
    if 'invoked oom-killer:' not in dmesg_content:
        return {'oom_detected': False}

    result = {
        'oom_detected': True,
        'processes_killed': [],
        'memory_info': {}
    }

    lines = dmesg_content.split('\n')

    # Look for killed process information
    for i, line in enumerate(lines):
        # Pattern: "Memory cgroup out of memory: Killed process 12345 (process_name)"
        # Or: "Out of memory: Killed process 12345 (process_name)"
        if 'Killed process' in line and ('out of memory' in line.lower() or 'oom' in line.lower()):
            match = re.search(r'Killed process\s+(\d+)\s+\(([^)]+)\)(?:.*anon-rss:(\d+)kB)?', line, re.IGNORECASE)
            if match:
                proc_info = {
                    'pid': match.group(1),
                    'name': match.group(2)
                }
                if match.group(3):
                    proc_info['anon_rss_kb'] = match.group(3)
                result['processes_killed'].append(proc_info)

        # Extract memory information from OOM killer output
        # Look for memory statistics
        if 'active_anon' in line or 'inactive_anon' in line:
            # Parse memory stats line - format: "active_anon 32768"
            match = re.search(r'(active_anon|inactive_anon)\s+(\d+)', line)
            if match:
                key = match.group(1)
                value_bytes = int(match.group(2))
                # Convert to MB for readability
                value_mb = value_bytes / (1024 * 1024)
                if 'memory_stats' not in result['memory_info']:
                    result['memory_info']['memory_stats'] = {}
                result['memory_info']['memory_stats'][key] = f"{value_mb:.1f} MB"

        # Look for MemFree or MemAvailable
        if 'MemAvailable' in line or 'MemFree' in line:
            result['memory_info']['mem_status'] = line.strip()

    return result

def analyze_sigterm_in_postmortem(postmortem_content):
    """Analyze postmortem log for SIGTERM signal.

    Returns dict with 'sigterm_detected' (bool) or None
    """
    if not postmortem_content:
        return None

    # Search for SIGTERM detection
    if 'SIGTERM detected' in postmortem_content:
        return {'sigterm_detected': True}

    return {'sigterm_detected': False}

def find_worker_pod_info_from_xml(xml_content, graph_name, worker_number):
    """Parse workunit XML to find Thor worker pod/container info.

    Args:
        xml_content: Workunit XML string
        graph_name: Graph name to search for (e.g., "graph1"), or None to skip graph matching
        worker_number: Worker sequence number as string (e.g., "118"). If None, tries 1st worker for that instance

    Returns:
        dict with pod_name, container_name, or None if not found
    """
    if not xml_content:
        return None

    try:
        # Parse XML
        root = ET.fromstring(xml_content)

        # Step 1: Find Thor element, then find child process with the graph name to get instanceNum
        thor_instance_num = None
        if graph_name:
            thor_element = root.find('.//Thor')
            if thor_element is not None:
                # Thor element contains child elements named after the cluster (e.g., <thor-nonphidelivery>)
                for thor_process in thor_element:
                    # Check if this Thor process has the graph we're looking for
                    graphs = thor_process.find('graphs')
                    if graphs is not None:
                        # Graphs are represented as empty elements with the graph name as tag
                        for graph in graphs:
                            if graph.tag == graph_name:
                                thor_instance_num = thor_process.get('instanceNum')
                                break
                    if thor_instance_num is not None:
                        break

        # Step 2: Find ThorWorker element, then find child with matching sequence (and optionally instanceNum)
        thorworker_element = root.find('.//ThorWorker')
        pod_name = None
        container_name = None
        note = None

        if thorworker_element is not None:
            if worker_number:
                # If we have an instanceNum from graph matching, use it for more precise matching
                if thor_instance_num is not None:
                    for worker_process in thorworker_element:
                        if worker_process.get('instanceNum') == thor_instance_num:
                            sequence = worker_process.get('sequence')
                            if sequence == worker_number:
                                pod_name = worker_process.get('podName')
                                container_name = worker_process.get('containerName')
                                break
                else:
                    # No graph name provided, search all ThorWorker elements by sequence only
                    for worker_process in thorworker_element:
                        sequence = worker_process.get('sequence')
                        if sequence == worker_number:
                            pod_name = worker_process.get('podName')
                            container_name = worker_process.get('containerName')
                            note = 'Matched by worker sequence only (no graph info)'
                            break

                # If exact match not found and we have an instanceNum, try pod name pattern matching
                if pod_name is None and thor_instance_num is not None:
                    for worker_process in thorworker_element:
                        if worker_process.get('instanceNum') == thor_instance_num:
                            pod_name_candidate = worker_process.get('podName', '')

                            # Try to extract worker number from pod name pattern
                            # Common pattern: thorworker-job-...-###-...
                            pod_worker_match = re.search(r'-(\d+)-', pod_name_candidate)
                            if pod_worker_match and pod_worker_match.group(1) == worker_number:
                                pod_name = worker_process.get('podName')
                                container_name = worker_process.get('containerName')
                                note = 'Matched by pod name pattern'
                                break

            # If still no match and we have an instanceNum, return first worker for that instance
            if pod_name is None and thor_instance_num is not None:
                for worker_process in thorworker_element:
                    if worker_process.get('instanceNum') == thor_instance_num:
                        pod_name = worker_process.get('podName')
                        container_name = worker_process.get('containerName')
                        note = 'Approximate match - exact worker not identified'
                        break

        if pod_name is None:
            return None

        result = {
            'pod_name': pod_name,
            'container_name': container_name,
            'instance_number': thor_instance_num
        }

        if note:
            result['note'] = note
        else:
            result['sequence'] = worker_number

        return result

    except ET.ParseError as e:
        return None

def get_workunit_info(esp_url, wuid, verbose=False, auth=None, quick=False, stop_on_error=False):
    """Fetch detailed workunit information."""
    if verbose:
        print(f"  [VERBOSE] Fetching workunit info for {wuid}", file=sys.stderr)

    # Track processing errors for this workunit
    processing_errors = []

    # Ensure URL has protocol prefix
    if not esp_url.startswith(('http://', 'https://')):
        esp_url = f"http://{esp_url}"

    url = f"{esp_url}/WsWorkunits/WUInfo.json"
    params = {
        'Wuid': wuid,
        'IncludeExceptions': 1,
        'IncludeGraphs': 0,
        'IncludeSourceFiles': 0,
        'IncludeResults': 0,
        'IncludeVariables': 0,
        'IncludeTimers': 0,
        'IncludeDebugValues': 0,
        'IncludeApplicationValues': 0,
        'IncludeWorkflows': 0,
        'IncludeXmlSchemas': 0,
        'IncludeResourceURLs': 0,
        'IncludeECL': 0,
        'IncludeHelpers': 1 if not quick else 0,  # Skip helpers in quick mode
        'IncludeAllowedClusters': 0,
        'SuppressResultSchemas': 1,
    }

    try:
        response = requests.get(url, params=params, auth=auth, timeout=30)
        response.raise_for_status()
        data = response.json()

        workunit = data.get('WUInfoResponse', {}).get('Workunit', {})

        exceptions = []
        exception_list = workunit.get('Exceptions', {}).get('ECLException', [])
        if isinstance(exception_list, dict):
            exception_list = [exception_list]

        for exc in exception_list:
            exceptions.append({
                'severity': exc.get('Severity', ''),
                'code': exc.get('Code', ''),
                'message': exc.get('Message', ''),
                'source': exc.get('Source', ''),
                'filename': exc.get('FileName', ''),
                'lineno': exc.get('LineNo', ''),
                'column': exc.get('Column', '')
            })

        # Extract first ERROR exception
        first_error = None
        for exc in exceptions:
            if exc.get('severity', '').lower() == 'error':
                first_error = exc
                break

        # Parse error message for graph/worker info and fetch process details
        error_details = None
        worker_pod_info = None
        if first_error:
            error_msg = first_error.get('message', '')
            error_details = parse_error_info(error_msg)

            if verbose:
                print(f"  [VERBOSE] Error detected: {error_msg[:100]}{'...' if len(error_msg) > 100 else ''}", file=sys.stderr)
                print(f"  [VERBOSE] Error details: graph={error_details.get('graph_name')}, worker={error_details.get('worker_number')}, machine={error_details.get('machine_endpoint')}", file=sys.stderr)

            # Skip detailed analysis in quick mode
            if quick:
                if verbose:
                    print(f"  [VERBOSE] Quick mode: skipping XML/postmortem analysis", file=sys.stderr)
            # If we found worker info OR graph name, fetch workunit XML to find process information
            elif error_details.get('worker_number') or error_details.get('graph_name'):
                if verbose:
                    if error_details.get('graph_name'):
                        print(f"  [VERBOSE] Fetching workunit XML to find pod/container info (graph-based search)", file=sys.stderr)
                    else:
                        print(f"  [VERBOSE] Fetching workunit XML to find pod/container info (sequence-only search)", file=sys.stderr)

                xml_content = get_workunit_xml(esp_url, wuid, auth=auth, verbose=verbose)
                if xml_content is None:
                    error_msg = "Failed to fetch workunit XML"
                    processing_errors.append(error_msg)
                    if verbose:
                        print(f"  [VERBOSE] ERROR: {error_msg}", file=sys.stderr)
                    if stop_on_error:
                        print(f"ERROR: {wuid}: {error_msg}", file=sys.stderr)
                        sys.exit(1)
                else:
                    worker_pod_info = find_worker_pod_info_from_xml(
                        xml_content,
                        error_details['graph_name'],
                        error_details['worker_number']
                    )

                    if verbose:
                        if worker_pod_info:
                            print(f"  [VERBOSE] Found pod: {worker_pod_info.get('pod_name')}, container: {worker_pod_info.get('container_name')}", file=sys.stderr)
                        else:
                            error_msg = "No pod/container info found in XML"
                            print(f"  [VERBOSE] WARNING: {error_msg}", file=sys.stderr)
                            processing_errors.append(error_msg)

                # Extract postmortem files from helpers matching the pod/container
                if worker_pod_info:
                    pod_name = worker_pod_info.get('pod_name')
                    container_name = worker_pod_info.get('container_name')

                    helpers = workunit.get('Helpers', {})
                    help_files = helpers.get('ECLHelpFile', [])
                    if isinstance(help_files, dict):
                        help_files = [help_files]

                    postmortem_files = []
                    dmesg_log_path = None
                    postmortem_log_files = []
                    postmortem_dir = None
                    for help_file in help_files:
                        if help_file.get('Type') == 'postmortem':
                            filename = help_file.get('Name', '')
                            # Check if filename contains both pod name and container name
                            if pod_name in filename and container_name in filename:
                                if f'/{pod_name}/{container_name}/' in filename:
                                    postmortem_files.append(filename)
                                    # Extract directory path for verbose logging
                                    if postmortem_dir is None and '/' in filename:
                                        postmortem_dir = filename.rsplit('/', 1)[0]
                                    # Track dmesg.log for OOM analysis
                                    if filename.endswith('/dmesg.log'):
                                        dmesg_log_path = filename
                                    # Track postmortem.*.log.* files for SIGTERM analysis
                                    elif re.search(r'/postmortem\..*\.log\.\d+$', filename):
                                        postmortem_log_files.append(filename)

                    worker_pod_info['postmortem_files'] = postmortem_files

                    # Check if we found any postmortem files
                    if not postmortem_files:
                        error_msg = f"No postmortem files found for pod {pod_name}, container {container_name}"
                        processing_errors.append(error_msg)
                        if verbose:
                            print(f"  [VERBOSE] WARNING: {error_msg}", file=sys.stderr)

                    # Log the postmortem directory if verbose
                    if verbose and postmortem_dir:
                        print(f"  [VERBOSE] Searching postmortem directory: {postmortem_dir}", file=sys.stderr)

                    # Analyze dmesg.log for OOM killer if it exists
                    oom_detected = False
                    if dmesg_log_path:
                        if verbose:
                            print(f"  [VERBOSE] Checking for OOM in: {dmesg_log_path}", file=sys.stderr)
                        dmesg_content = fetch_helper_file(esp_url, wuid, dmesg_log_path, auth=auth, verbose=verbose)
                        if dmesg_content is None:
                            error_msg = "Failed to fetch dmesg.log"
                            processing_errors.append(error_msg)
                            if verbose:
                                print(f"  [VERBOSE] WARNING: {error_msg} - OOM detection skipped", file=sys.stderr)
                            if stop_on_error:
                                print(f"ERROR: {wuid}: {error_msg}", file=sys.stderr)
                                sys.exit(1)
                        else:
                            oom_info = analyze_oom_in_dmesg(dmesg_content)
                            if verbose:
                                if oom_info is None:
                                    print(f"  [VERBOSE] analyze_oom_in_dmesg returned None (empty content?)", file=sys.stderr)
                                elif oom_info.get('oom_detected'):
                                    print(f"  [VERBOSE] OOM DETECTED in dmesg - {len(oom_info.get('processes_killed', []))} processes killed", file=sys.stderr)
                                else:
                                    print(f"  [VERBOSE] No OOM found in dmesg (searched for 'invoked oom-killer:')", file=sys.stderr)

                            if oom_info:
                                worker_pod_info['oom_info'] = oom_info
                                oom_detected = oom_info.get('oom_detected', False)
                                if verbose and oom_detected:
                                    print(f"  [VERBOSE] OOM detected - skipping SIGTERM check", file=sys.stderr)

                    # If no OOM detected, check postmortem logs for SIGTERM
                    if not oom_detected and postmortem_log_files:
                        if verbose:
                            print(f"  [VERBOSE] Found {len(postmortem_log_files)} postmortem log file(s) to check", file=sys.stderr)

                        # Sort postmortem log files by numeric suffix if present, otherwise by name
                        # Extract the numeric suffix for sorting
                        def extract_log_number(filepath):
                            match = re.search(r'\.log\.(\d+)$', filepath)
                            return int(match.group(1)) if match else -1

                        postmortem_log_files.sort(key=extract_log_number)
                        last_postmortem_log = postmortem_log_files[-1]

                        if verbose:
                            print(f"  [VERBOSE] Checking for SIGTERM in: {last_postmortem_log}", file=sys.stderr)

                        postmortem_content = fetch_helper_file(esp_url, wuid, last_postmortem_log, auth=auth, verbose=verbose)
                        if postmortem_content is None:
                            error_msg = "Failed to fetch postmortem log"
                            processing_errors.append(error_msg)
                            if verbose:
                                print(f"  [VERBOSE] WARNING: {error_msg} - SIGTERM detection skipped", file=sys.stderr)
                            if stop_on_error:
                                print(f"ERROR: {wuid}: {error_msg}", file=sys.stderr)
                                sys.exit(1)
                        else:
                            sigterm_info = analyze_sigterm_in_postmortem(postmortem_content)
                            if verbose:
                                if sigterm_info is None:
                                    print(f"  [VERBOSE] analyze_sigterm_in_postmortem returned None (empty content?)", file=sys.stderr)
                                elif sigterm_info.get('sigterm_detected'):
                                    print(f"  [VERBOSE] SIGTERM DETECTED in postmortem log", file=sys.stderr)
                                else:
                                    print(f"  [VERBOSE] No SIGTERM found in postmortem (searched for 'SIGTERM detected')", file=sys.stderr)

                            if sigterm_info:
                                worker_pod_info['sigterm_info'] = sigterm_info
                                worker_pod_info['sigterm_log_file'] = last_postmortem_log

        return {
            'wuid': wuid,
            'state': workunit.get('State', ''),
            'owner': workunit.get('Owner', ''),
            'cluster': workunit.get('Cluster', ''),
            'jobname': workunit.get('Jobname', ''),
            'protected': workunit.get('Protected', False),
            'created': workunit.get('DateTimeScheduled', ''),
            'total_time': workunit.get('TotalClusterTime', ''),
            'compile_time': workunit.get('CompileTime', ''),
            'execute_time': workunit.get('ExecuteTime', ''),
            'exceptions': exceptions,
            'first_error': first_error,
            'error_details': error_details,
            'worker_pod_info': worker_pod_info,
            'processing_errors': processing_errors if processing_errors else None
        }

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print(f"ERROR: Authentication failed for {wuid}. Please check your credentials (-u user:password).", file=sys.stderr)
            sys.exit(1)
        error_msg = f"HTTP error {e.response.status_code}: {e}"
        if stop_on_error:
            print(f"ERROR: {wuid}: {error_msg}", file=sys.stderr)
            sys.exit(1)
        return {
            'wuid': wuid,
            'error': error_msg
        }
    except requests.exceptions.RequestException as e:
        error_msg = f"Failed to fetch workunit info: {e}"
        if stop_on_error:
            print(f"ERROR: {wuid}: {error_msg}", file=sys.stderr)
            sys.exit(1)
        return {
            'wuid': wuid,
            'error': error_msg
        }
    except Exception as e:
        error_msg = f"Script failed: {e}"
        processing_errors.append(error_msg)
        if verbose:
            print(f"  [VERBOSE] ERROR: {error_msg}", file=sys.stderr)
        if stop_on_error:
            print(f"ERROR: {wuid}: {error_msg}", file=sys.stderr)
            sys.exit(1)
        return {
            'wuid': wuid,
            'error': error_msg,
            'processing_errors': processing_errors if processing_errors else None
        }

def read_wuids_from_file(filepath):
    """Read WUIDs from a file, one per line."""
    wuids = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                wuid = line.strip()
                if wuid and not wuid.startswith('#'):  # Skip empty lines and comments
                    wuids.append(wuid)
    except IOError as e:
        print(f"Error reading file '{filepath}': {e}", file=sys.stderr)
        sys.exit(1)
    return wuids

def read_wuids_from_stdin():
    """Read WUIDs from standard input, one per line."""
    wuids = []
    for line in sys.stdin:
        wuid = line.strip()
        if wuid and not wuid.startswith('#'):  # Skip empty lines and comments
            wuids.append(wuid)
    return wuids

def read_error_patterns_from_file(filepath):
    """Read error patterns from a file, one per line."""
    patterns = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                pattern = line.strip()
                if pattern and not pattern.startswith('#'):  # Skip empty lines and comments
                    patterns.append(pattern)
    except IOError as e:
        print(f"Error reading error patterns file '{filepath}': {e}", file=sys.stderr)
        sys.exit(1)
    return patterns

def validate_regex_patterns(patterns, filepath):
    """Validate regex patterns and abort if any are invalid.

    Args:
        patterns: List of regex pattern strings
        filepath: Path to the file (for error reporting)

    Returns:
        List of compiled regex patterns
    """
    compiled_patterns = []
    errors = []

    for i, pattern in enumerate(patterns, 1):
        try:
            compiled = re.compile(pattern, re.IGNORECASE)
            compiled_patterns.append(compiled)
        except re.error as e:
            errors.append(f"  Line {i}: Invalid regex pattern: {pattern}\n           Error: {e}")

    if errors:
        print(f"Error: Invalid regex patterns in '{filepath}':", file=sys.stderr)
        for error in errors:
            print(error, file=sys.stderr)
        sys.exit(1)

    return compiled_patterns

def match_error_pattern(error_message, patterns, use_regex=False):
    """Check if error message matches any of the patterns.

    Args:
        error_message: The error message to check
        patterns: List of patterns (strings for substring, compiled regexes for regex mode)
        use_regex: If True, patterns are compiled regex objects

    Returns:
        The matched pattern string if a match is found, None otherwise
    """
    if not patterns:
        return True  # No patterns means match all

    if use_regex:
        # Use regex matching with pre-compiled patterns
        for compiled_pattern in patterns:
            if compiled_pattern.search(error_message):
                return compiled_pattern.pattern
    else:
        # Use substring matching (original behavior)
        error_message_lower = error_message.lower()
        for pattern in patterns:
            pattern_lower = pattern.lower()
            if pattern_lower in error_message_lower:
                return pattern
    return None

def format_time(time_str):
    """Format time string, handling None."""
    if time_str is None:
        return 'N/A'
    return time_str

def print_workunit_info(info, verbose=False, matched=None):
    """Print workunit information in a readable format."""
    if info.get('error'):
        print(f"ISSUE: {info['wuid']} {info['error']}")
        print(f"WUID: {info['wuid']}")
        print(f"  ERROR: {info['error']}")
        print()
        return

    # Add matched indicator if pattern matching was used
    wuid_display = info['wuid']
    if matched is not None:
        wuid_display += f" {'[MATCHED]' if matched else '[NO MATCH]'}"

    print(f"WUID: {wuid_display}")
    print(f"  State:        {info['state']}")
    print(f"  Owner:        {info['owner']}")
    print(f"  Jobname:      {info['jobname']}")
    print(f"  Cluster:      {info['cluster']}")
    print(f"  Protected:    {info['protected']}")
    if verbose:
        print(f"  Created:      {info['created']}")
        print(f"  Total Time:   {format_time(info['total_time'])}")
        print(f"  Compile Time: {format_time(info['compile_time'])}")
        print(f"  Execute Time: {format_time(info['execute_time'])}")

    # Display processing errors if any
    processing_errors = info.get('processing_errors')
    if processing_errors:
        for error in processing_errors:
            print(f"ISSUE: {info['wuid']} {error}")
        print(f"  WUID: {info['wuid']}: {', '.join(processing_errors)}")

    # Display first ERROR exception only
    first_error = info.get('first_error')
    if first_error:
        severity = first_error.get('severity', 'Unknown')
        source = first_error.get('source', '')
        code = first_error.get('code', '')
        message = first_error.get('message', '')
        filename = first_error.get('filename', '')
        line = first_error.get('line', '')
        column = first_error.get('column', '')

        # Check if we have OOM or SIGTERM detection to include in ISSUE line
        worker_pod_info = info.get('worker_pod_info')
        issue_suffix = ""
        if worker_pod_info:
            oom_info = worker_pod_info.get('oom_info')
            if oom_info and oom_info.get('oom_detected'):
                issue_suffix = " (OOM Killer detected)"
            else:
                sigterm_info = worker_pod_info.get('sigterm_info')
                if sigterm_info and sigterm_info.get('sigterm_detected'):
                    issue_suffix = " (SIGTERM detected)"

        # Output ISSUE line with error message (truncate if too long)
        error_summary = message.split('\n')[0]  # First line only
        if len(error_summary) > 100:
            error_summary = error_summary[:97] + '...'
        print(f"ISSUE: {info['wuid']} {error_summary}{issue_suffix}")

        print(f"  Error:        {severity.upper()}", end='')
        if code:
            print(f" (Code {code})", end='')
        if source:
            print(f" - {source}", end='')
        print()

        if message:
            # Indent and wrap long messages
            for msg_line in message.split('\n'):
                print(f"                {msg_line}")

        if filename:
            location = f"                File: {filename}"
            if line:
                location += f", Line: {line}"
            if column:
                location += f", Column: {column}"
            print(location)

        # Display parsed error details and worker pod info
        error_details = info.get('error_details')
        if error_details:
            if error_details.get('graph_name'):
                print(f"  Graph:        {error_details['graph_name']}", end='')
                if error_details.get('subgraph_id'):
                    print(f"[{error_details['subgraph_id']}]", end='')
                print()
            if error_details.get('worker_number'):
                print(f"  Worker:       #{error_details['worker_number']}")

        worker_pod_info = info.get('worker_pod_info')
        if worker_pod_info:
            print(f"  Pod:          {worker_pod_info.get('pod_name', 'N/A')}")
            print(f"  Container:    {worker_pod_info.get('container_name', 'N/A')}")
            if worker_pod_info.get('note'):
                print(f"  Note:         {worker_pod_info['note']}")

            # Display postmortem files
            postmortem_files = worker_pod_info.get('postmortem_files', [])
            if postmortem_files:
                print(f"  Postmortem:   {len(postmortem_files)} file(s)")
                for pm_file in postmortem_files:
                    print(f"                {pm_file}")
            else:
                print(f"  Postmortem:   No files found")

            # Display OOM killer information if detected
            oom_info = worker_pod_info.get('oom_info')
            if oom_info and oom_info.get('oom_detected'):
                print(f"  OOM Killer:   DETECTED - Process was killed by out-of-memory killer")

                processes_killed = oom_info.get('processes_killed', [])
                if processes_killed:
                    # Show the main process (usually the first thorslave_lcr)
                    main_proc = None
                    for proc in processes_killed:
                        if 'thorslave' in proc.get('name', '').lower():
                            main_proc = proc
                            break
                    if not main_proc and processes_killed:
                        main_proc = processes_killed[0]

                    if main_proc:
                        proc_name = main_proc.get('name')
                        proc_pid = main_proc.get('pid')
                        anon_rss = main_proc.get('anon_rss_kb')

                        print(f"                Killed: {proc_name} (PID {proc_pid})", end='')
                        if anon_rss:
                            # Convert KB to GB for readability
                            anon_rss_gb = int(anon_rss) / (1024 * 1024)
                            print(f", RSS: {anon_rss_gb:.1f} GB", end='')
                        print()

                    if len(processes_killed) > 1:
                        print(f"                Total processes killed: {len(processes_killed)}")

                mem_info = oom_info.get('memory_info', {})
                if mem_info:
                    mem_stats = mem_info.get('memory_stats', {})
                    if mem_stats:
                        if 'active_anon' in mem_stats:
                            print(f"                Active memory: {mem_stats['active_anon']}")
                        if 'inactive_anon' in mem_stats:
                            print(f"                Inactive memory: {mem_stats['inactive_anon']}")
                    elif mem_info.get('mem_status'):
                        print(f"                {mem_info['mem_status']}")

            # Display SIGTERM information if detected
            sigterm_info = worker_pod_info.get('sigterm_info')
            if sigterm_info and sigterm_info.get('sigterm_detected'):
                sigterm_log_file = worker_pod_info.get('sigterm_log_file', '')
                log_filename = sigterm_log_file.split('/')[-1] if sigterm_log_file else 'postmortem log'
                print(f"  SIGTERM:      DETECTED - Process received SIGTERM signal")
                print(f"                Found in: {log_filename}")

    print()

def print_summary_table(infos, show_matched=False):
    """Print a summary table of all workunits."""
    headers = f"{'WUID':<20} {'State':<12} {'Owner':<15} {'Cluster':<15}"
    if show_matched:
        headers += f" {'Match':<8}"
    headers += f" {'Error'}"
    print(headers)
    print("-" * (110 if show_matched else 100))

    for info in infos:
        if info.get('error'):
            line = f"{info['wuid']:<20} {'ERROR':<12} {'':<15} {'':<15}"
            if show_matched:
                line += f" {'':<8}"
            line += f" {info['error']}"
            print(line)
        else:
            wuid = info['wuid'][:20]
            state = info['state'][:12]
            owner = info['owner'][:15]
            cluster = info['cluster'][:15]

            first_error = info.get('first_error')
            error_text = ""
            if first_error:
                error_text = first_error.get('message', '')[:60]

            line = f"{wuid:<20} {state:<12} {owner:<15} {cluster:<15}"
            if show_matched:
                matched = info.get('matched', False)
                match_str = "YES" if matched else "NO"
                line += f" {match_str:<8}"
            line += f" {error_text}"
            print(line)

def main():
    parser = argparse.ArgumentParser(
        description='Analyze workunit details from ESP WsWorkunits service',
        usage='%(prog)s <espserver:port> [<wuid1> <wuid2> ...] [-f <file>] [-e <file>] [-re <file>] [-v] [-s] [-q] [--stop-on-error] [-u <user>:<pwd>]',
        epilog=r'''
Examples:
  %(prog)s localhost:8010 W20240101-120000 W20240101-120001
      Analyze specific workunits

  %(prog)s localhost:8010 -f wuids.txt
      Read WUIDs from file (one per line)

  cat wuids.txt | %(prog)s localhost:8010
      Read WUIDs from stdin

  ./get_wuids.py localhost:8010 2024-01-01 2024-01-01 -s failed | tail -n +3 | %(prog)s localhost:8010
      Pipe WUIDs from get_wuids.py

  %(prog)s localhost:8010 -f wuids.txt -e error_patterns.txt
      Filter to workunits matching specific error patterns (substring match)

  %(prog)s localhost:8010 -f wuids.txt -re regex_patterns.txt
      Filter using regex patterns (e.g., "Graph \w+\[\d+\], WORKER #\d+.*Watchdog")

  %(prog)s localhost:8010 -f wuids.txt -re patterns.txt -q
      Quick mode: match patterns but skip XML/postmortem analysis

  %(prog)s localhost:8010 -f wuids.txt --show-oom
      Show only workunits with OOM killer events

  %(prog)s localhost:8010 -f wuids.txt -u myuser:mypassword
      Use authentication when connecting to ESP server
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('espserver',
                       metavar='espserver:port',
                       help='ESP server address in format host:port (e.g., localhost:8010)')
    parser.add_argument('wuids',
                       nargs='*',
                       help='Workunit IDs to analyze')
    parser.add_argument('-f', '--file',
                       metavar='<file>',
                       help='Read WUIDs from file (one per line)')
    parser.add_argument('-v', '--verbose',
                       action='store_true',
                       help='Show detailed information including timings')
    parser.add_argument('-s', '--summary',
                       action='store_true',
                       help='Show summary table only')
    parser.add_argument('--stop-on-error',
                       action='store_true',
                       help='Stop processing on any error')
    parser.add_argument('-q', '--quick',
                       action='store_true',
                       help='Quick mode: skip XML/postmortem analysis (faster for pattern matching)')
    parser.add_argument('-e', '--errors',
                       metavar='<file>',
                       help='File containing error patterns to match (one per line, substring match)')
    parser.add_argument('-re', '--regex-errors',
                       metavar='<file>',
                       dest='regex_errors',
                       help='File containing regex error patterns to match (one per line)')
    parser.add_argument('--show-oom',
                       action='store_true',
                       help='Show only workunits with OOM killer events')
    parser.add_argument('-u', '--user',
                       metavar='<user>:<pwd>',
                       help='HTTP basic authentication credentials in format user:password')

    args = parser.parse_args()

    # Validate conflicting options
    if args.quick and args.show_oom:
        print("Error: Cannot use --quick with --show-oom (OOM detection requires full analysis)", file=sys.stderr)
        return 1

    # Parse authentication credentials if provided
    auth = None
    if args.user:
        if ':' not in args.user:
            print("Error: Authentication credentials must be in format user:password", file=sys.stderr)
            return 1
        username, password = args.user.split(':', 1)
        auth = HTTPBasicAuth(username, password)

    # Read error patterns if provided
    error_patterns = []
    use_regex = False

    if args.errors and args.regex_errors:
        print("Error: Cannot specify both -e and -re at the same time. Use one or the other.", file=sys.stderr)
        return 1

    if args.errors:
        error_patterns = read_error_patterns_from_file(args.errors)
        use_regex = False
        if not error_patterns:
            print(f"Warning: No error patterns found in {args.errors}")
    elif args.regex_errors:
        raw_patterns = read_error_patterns_from_file(args.regex_errors)
        if not raw_patterns:
            print(f"Warning: No error patterns found in {args.regex_errors}")
        else:
            # Validate and compile regex patterns
            error_patterns = validate_regex_patterns(raw_patterns, args.regex_errors)
            use_regex = True
        if not error_patterns:
            print(f"Warning: No error patterns found in {args.regex_errors}")

    # Collect WUIDs from various sources
    wuids = []

    if args.file:
        # Read from file
        wuids = read_wuids_from_file(args.file)
    elif args.wuids:
        # Use command line arguments
        wuids = args.wuids
    elif not sys.stdin.isatty():
        # Read from stdin if available
        wuids = read_wuids_from_stdin()
    else:
        print("Error: No WUIDs provided. Use command line arguments, -f option, or pipe from stdin.", file=sys.stderr)
        parser.print_help(sys.stderr)
        return 1

    if not wuids:
        print("Error: No valid WUIDs found.", file=sys.stderr)
        return 1

    print(f"Analyzing {len(wuids)} workunit(s)...\n")

    # Fetch information for each workunit
    # Initialize pattern match counters
    pattern_counts = {}
    if error_patterns:
        for pattern in error_patterns:
            # For regex mode, use the pattern string; for substring mode, use as-is
            pattern_key = pattern.pattern if use_regex else pattern
            pattern_counts[pattern_key] = 0

    infos = []
    for wuid in wuids:
        info = get_workunit_info(args.espserver, wuid, verbose=args.verbose, auth=auth, quick=args.quick, stop_on_error=args.stop_on_error)

        # Check for OOM if --show-oom flag is set
        has_oom = False
        if args.show_oom:
            worker_pod_info = info.get('worker_pod_info')
            if worker_pod_info:
                oom_info = worker_pod_info.get('oom_info')
                has_oom = oom_info and oom_info.get('oom_detected')

        # If error patterns specified, filter workunits and mark matched ones
        if error_patterns:
            # Check if there was an error fetching this workunit
            if info.get('error'):
                # Always include workunits that had fetch errors so user can see the problem
                infos.append(info)
            else:
                first_error = info.get('first_error')
                if first_error:
                    error_msg = first_error.get('message', '')
                    matched_pattern = match_error_pattern(error_msg, error_patterns, use_regex=use_regex)
                    if matched_pattern:
                        info['matched'] = True
                        info['matched_pattern'] = matched_pattern
                        pattern_counts[matched_pattern] += 1
                        # Only include workunits that match error patterns
                        # Also apply OOM filter if requested
                        if not args.show_oom or has_oom:
                            infos.append(info)
                    else:
                        info['matched'] = False
                # Skip workunits without errors when filtering by patterns
        elif args.show_oom:
            # Always include workunits with fetch errors, or those with OOM events
            if info.get('error') or has_oom:
                infos.append(info)
        else:
            infos.append(info)

    if len(infos) == 0:
        if error_patterns and args.show_oom:
            print("No workunits matched the specified error patterns and had OOM events.")
        elif error_patterns:
            print("No workunits matched the specified error patterns.")
        elif args.show_oom:
            print("No workunits with OOM killer events found.")
        return 0

    # Display results
    if args.summary:
        print_summary_table(infos, show_matched=bool(error_patterns))
    else:
        for info in infos:
            matched = info.get('matched', False) if error_patterns else None
            print_workunit_info(info, verbose=args.verbose, matched=matched)

        if len(infos) > 1 and not args.verbose:
            print("\n" + "=" * 100)
            print("SUMMARY")
            print("=" * 100 + "\n")
            print_summary_table(infos, show_matched=bool(error_patterns))

    # Print statistics
    print(f"\nTotal workunits analyzed: {len(infos)}")

    # Count by state and exceptions
    states = {}
    errors = 0
    total_first_errors = 0
    wus_with_errors = 0
    matched_wus = 0
    oom_wus = 0
    sigterm_wus = 0
    processing_errors_count = 0
    earliest_wuid = None
    latest_wuid = None

    for info in infos:
        if info.get('error'):
            errors += 1
        else:
            wuid = info.get('wuid', '')

            # Track earliest and latest WUID for date range calculation
            if wuid:
                if earliest_wuid is None or wuid < earliest_wuid:
                    earliest_wuid = wuid
                if latest_wuid is None or wuid > latest_wuid:
                    latest_wuid = wuid

            state = info.get('state', 'unknown')
            states[state] = states.get(state, 0) + 1

            # Count first ERROR exception
            if info.get('first_error'):
                wus_with_errors += 1
                total_first_errors += 1

            # Count matched patterns
            if info.get('matched'):
                matched_wus += 1

            # Count processing errors
            if info.get('processing_errors'):
                processing_errors_count += 1

            # Count OOM and SIGTERM events (only available when not in quick mode)
            worker_pod_info = info.get('worker_pod_info')
            if worker_pod_info:
                oom_info = worker_pod_info.get('oom_info')
                if oom_info and oom_info.get('oom_detected'):
                    oom_wus += 1

                sigterm_info = worker_pod_info.get('sigterm_info')
                if sigterm_info and sigterm_info.get('sigterm_detected'):
                    sigterm_wus += 1

    if states:
        print("\nBy state:")
        for state, count in sorted(states.items()):
            print(f"  {state}: {count}")

    if wus_with_errors > 0:
        print(f"\nWorkunits with errors: {wus_with_errors}")
        if error_patterns:
            print(f"Workunits matching patterns: {matched_wus}")

            # Display counts for each pattern
            print(f"\nPattern match breakdown:")
            for pattern in error_patterns:
                # For regex mode, use the pattern string; for substring mode, use as-is
                pattern_key = pattern.pattern if use_regex else pattern
                count = pattern_counts.get(pattern_key, 0)
                # Truncate long patterns for display
                display_pattern = pattern_key if len(pattern_key) <= 60 else pattern_key[:57] + '...'
                print(f"  {display_pattern}: {count}")

        # Display OOM and SIGTERM counts (only meaningful when not in quick mode)
        if not args.quick:
            if oom_wus > 0 or sigterm_wus > 0:
                # Calculate date range from WUIDs for rate calculation
                if earliest_wuid and latest_wuid:
                    # WUID format: W{YYYYMMDD}-{HHMMSS}
                    # Extract date portion (characters 1-9, e.g., "20240101" from "W20240101-120000")
                    try:
                        earliest_date_str = earliest_wuid[1:9]  # YYYYMMDD
                        latest_date_str = latest_wuid[1:9]      # YYYYMMDD

                        earliest_date = datetime.strptime(earliest_date_str, '%Y%m%d')
                        latest_date = datetime.strptime(latest_date_str, '%Y%m%d')

                        # Calculate days in range (add 1 to include both start and end days)
                        days_in_range = (latest_date - earliest_date).days + 1

                        if oom_wus > 0:
                            oom_per_day = oom_wus / days_in_range
                            print(f"Workunits with OOM events: {oom_wus} (avg {oom_per_day:.2f}/day over {days_in_range} days)")

                        if sigterm_wus > 0:
                            sigterm_per_day = sigterm_wus / days_in_range
                            print(f"Workunits with SIGTERM events: {sigterm_wus} (avg {sigterm_per_day:.2f}/day over {days_in_range} days)")
                    except (ValueError, IndexError) as e:
                        # Fallback if WUID format is unexpected
                        if oom_wus > 0:
                            print(f"Workunits with OOM events: {oom_wus}")
                        if sigterm_wus > 0:
                            print(f"Workunits with SIGTERM events: {sigterm_wus}")
                else:
                    # No valid WUIDs for date range calculation
                    if oom_wus > 0:
                        print(f"Workunits with OOM events: {oom_wus}")
                    if sigterm_wus > 0:
                        print(f"Workunits with SIGTERM events: {sigterm_wus}")

    if errors > 0:
        print(f"\nQuery errors: {errors}")

    if processing_errors_count > 0:
        print(f"Workunits with processing errors: {processing_errors_count}")

    return 0

if __name__ == '__main__':
    sys.exit(main())
