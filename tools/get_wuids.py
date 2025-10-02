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
get_wuids.py - Query workunits from ESP WsWorkunits service

This script fetches a list of workunit IDs (WUIDs) and their states from an
ESP server's WsWorkunits service for a given date range.

Usage:
    get_wuids.py <espserver:port> <start-date> <end-date>
    get_wuids.py <espserver:port> <start-date> <end-date> -s <state>

Arguments:
    espserver:port  ESP server address (e.g., localhost:8010)
    start-date      Start date/time in format:
                      YYYY-MM-DD (assumes 00:00:00)
                      YYYY-MM-DDTHH:MM:SS (explicit time)
    end-date        End date/time in format:
                      YYYY-MM-DD (assumes 23:59:59)
                      YYYY-MM-DDTHH:MM:SS (explicit time)
    -s state        Filter by state (e.g., failed, completed)
                    When specified, only WUIDs are output (no state column)

Examples:
    # Query by date (entire days)
    get_wuids.py localhost:8010 2024-01-01 2024-01-31

    # Query with specific times
    get_wuids.py localhost:8010 2024-01-01T08:00:00 2024-01-01T17:00:00

    # Mix date and datetime
    get_wuids.py localhost:8010 2024-01-01 2024-01-31T12:00:00

    # Get only failed workunits (WUID only)
    getwuids.py localhost:8010 2024-01-01 2024-01-31 -s failed

The script connects to the ESP server and queries the WUQuery service endpoint,
filtering workunits by the specified start and end dates. Results are displayed
in a tabular format showing the WUID and state for each workunit found. When
using the -s flag to filter by state, only the WUID is output (one per line),
making it easy to pipe to other commands like analyze_wuids.py.
"""

import sys
import argparse
import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
from urllib.parse import urljoin

def parse_wuquery_response(xml_content):
    """Parse WUQuery XML response and extract workunits with their states."""
    try:
        root = ET.fromstring(xml_content)

        # Find all workunit elements in the response
        # The namespace might be present, so we need to handle both cases
        workunits = []

        # Try with namespace-aware search
        ns = {'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
              'ws': 'urn:hpccsystems:ws:wsworkunits'}

        # Look for workunits in the response
        for wu_elem in root.findall('.//ws:ECLWorkunit', ns):
            wuid = wu_elem.find('ws:Wuid', ns)
            state = wu_elem.find('ws:State', ns)
            if wuid is not None:
                workunits.append({
                    'wuid': wuid.text if wuid.text else '',
                    'state': state.text if state is not None and state.text else ''
                })

        # If no workunits found with namespace, try without
        if not workunits:
            for wu_elem in root.findall('.//ECLWorkunit'):
                wuid = wu_elem.find('Wuid')
                state = wu_elem.find('State')
                if wuid is not None:
                    workunits.append({
                        'wuid': wuid.text if wuid.text else '',
                        'state': state.text if state is not None and state.text else ''
                    })

        return workunits
    except ET.ParseError as e:
        print(f"Error parsing XML response: {e}", file=sys.stderr)
        return []

def normalize_date(date_str):
    """Normalize date string to YYYY-MM-DD format.

    Accepts:
        YYYYMMDD -> YYYY-MM-DD
        YYYY-MM-DD -> YYYY-MM-DD (unchanged)
        YYYY-MM-DDTHH:MM:SS -> YYYY-MM-DDTHH:MM:SS (unchanged)
    """
    import re

    # If already has 'T', return as-is (datetime format)
    if 'T' in date_str:
        return date_str

    # If already has hyphens, return as-is
    if '-' in date_str:
        return date_str

    # Try to parse YYYYMMDD format
    match = re.match(r'^(\d{4})(\d{2})(\d{2})$', date_str)
    if match:
        year, month, day = match.groups()
        normalized = f"{year}-{month}-{day}"
        return normalized

    # Return as-is if we can't parse it (let the server error handle it)
    return date_str

def query_workunits(esp_server, start_date, end_date, auth=None, page_size=500):
    """Query workunits from ESP server between start and end dates."""

    # Normalize date formats
    start_date = normalize_date(start_date)
    end_date = normalize_date(end_date)

    # Construct the URL for the WUQuery service
    # Ensure URL has protocol prefix
    if not esp_server.startswith(('http://', 'https://')):
        base_url = f"http://{esp_server}"
    else:
        base_url = esp_server

    service_url = urljoin(base_url, "/WsWorkunits/WUQuery.json")

    # Convert date strings to datetime format required by WUQuery
    # WUQuery expects ISO 8601 datetime format (YYYY-MM-DDTHH:MM:SS)
    if 'T' not in start_date:
        start_date = f"{start_date}T00:00:00"
    if 'T' not in end_date:
        end_date = f"{end_date}T23:59:59"

    all_workunits = []
    page_start_from = 0

    while True:
        # Build query parameters
        params = {
            'StartDate': start_date,
            'EndDate': end_date,
            'PageSize': page_size,
            'PageStartFrom': page_start_from
        }

        try:
            # Make the request
            response = requests.get(service_url, params=params, auth=auth, timeout=30)
            response.raise_for_status()

            # Parse JSON response
            data = response.json()

            # Check for exceptions in the response
            if 'Exceptions' in data:
                exceptions = data['Exceptions']
                print("Error response from ESP server:", file=sys.stderr)

                # Handle both single exception and array of exceptions
                exception_list = exceptions.get('Exception', [])
                if isinstance(exception_list, dict):
                    exception_list = [exception_list]

                for exc in exception_list:
                    code = exc.get('Code', 'N/A')
                    message = exc.get('Message', 'Unknown error')
                    print(f"  [Code {code}] {message}", file=sys.stderr)

                    # Provide helpful hints for common errors
                    if 'Badly formatted date' in message or 'date/time' in message.lower():
                        print("\nHint: Dates must be in format YYYY-MM-DD (e.g., 2024-01-15)", file=sys.stderr)
                        print("      or YYYY-MM-DDTHH:MM:SS (e.g., 2024-01-15T14:30:00)", file=sys.stderr)

                return []

            # Extract workunits from JSON response
            wu_response = data.get('WUQueryResponse', {})
            wu_list = wu_response.get('Workunits', {}).get('ECLWorkunit', [])

            # Handle case where single workunit is returned as dict instead of list
            if isinstance(wu_list, dict):
                wu_list = [wu_list]

            # Add workunits from this page
            for wu in wu_list:
                all_workunits.append({
                    'wuid': wu.get('Wuid', ''),
                    'state': wu.get('State', '')
                })

            # Check if there are more results
            num_wus = wu_response.get('NumWUs', 0)
            if not wu_list or len(all_workunits) >= num_wus:
                # No more results
                break

            # Move to next page
            page_start_from += len(wu_list)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print(f"ERROR: Authentication failed. Please check your credentials (-u user:password).", file=sys.stderr)
                sys.exit(1)
            print(f"HTTP error {e.response.status_code}: {e}", file=sys.stderr)
            return []
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to ESP server: {e}", file=sys.stderr)
            return []
        except (KeyError, ValueError) as e:
            print(f"Error parsing response: {e}", file=sys.stderr)
            return []

    return all_workunits

def main():
    parser = argparse.ArgumentParser(
        description='Fetch workunit IDs and states from ESP WsWorkunits service',
        usage='%(prog)s <espserver:port> <start-date> <end-date> [-s <state>] [-p <num>] [-u <user>:<pwd>]',
        epilog='''
Examples:
  %(prog)s localhost:8010 2024-01-01 2024-01-31
      Query workunits from January 1st through January 31st, 2024

  %(prog)s localhost:8010 2024-01-01T08:00:00 2024-01-01T17:00:00
      Query workunits from 8 AM to 5 PM on January 1st, 2024

  %(prog)s localhost:8010 2024-01-01 2024-01-31T12:00:00
      Query from start of January 1st through noon on January 31st

  %(prog)s localhost:8010 2024-01-01 2024-01-31 -s failed
      Query only failed workunits (outputs WUID only)

  %(prog)s localhost:8010 2024-01-01 2024-01-31 -p 1000
      Query workunits with page size of 1000 (default is 500)

  %(prog)s localhost:8010 2024-01-01 2024-01-31 -u myuser:mypassword
      Query workunits with authentication

Date/Time Formats:
  YYYY-MM-DD              Date only (start defaults to 00:00:00, end to 23:59:59)
  YYYY-MM-DDTHH:MM:SS     Date with specific time

State Filter:
  When -s <state> is used, only workunits matching that state are returned
  and only the WUID is output (no state column). Common states:
    completed, failed, aborted, blocked, submitted, compiled, running, etc.
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('espserver',
                       metavar='espserver:port',
                       help='ESP server address in format host:port (e.g., localhost:8010)')
    parser.add_argument('start_date',
                       metavar='start-date',
                       help='Start date/time: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS')
    parser.add_argument('end_date',
                       metavar='end-date',
                       help='End date/time: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS')
    parser.add_argument('-s', '--state',
                       metavar='<state>',
                       help='Filter by state (e.g., failed, completed). When specified, only WUIDs are output.')
    parser.add_argument('-p', '--page-size',
                       metavar='<num>',
                       type=int,
                       default=500,
                       help='Number of workunits to fetch per page (default: 500)')
    parser.add_argument('-u', '--user',
                       metavar='<user>:<pwd>',
                       help='HTTP basic authentication credentials in format user:password')

    args = parser.parse_args()

    # Parse authentication credentials if provided
    auth = None
    if args.user:
        if ':' not in args.user:
            print("Error: Authentication credentials must be in format user:password", file=sys.stderr)
            return 1
        username, password = args.user.split(':', 1)
        auth = HTTPBasicAuth(username, password)

    # Validate page size
    if args.page_size <= 0:
        print("Error: Page size must be a positive integer", file=sys.stderr)
        return 1

    # Query the workunits
    workunits = query_workunits(args.espserver, args.start_date, args.end_date, auth=auth, page_size=args.page_size)

    if not workunits:
        print("No workunits found or error occurred", file=sys.stderr)
        return 1

    # Filter by state if requested
    if args.state:
        state_filter = args.state.lower()
        filtered_workunits = [wu for wu in workunits if wu['state'].lower() == state_filter]

        if not filtered_workunits:
            print(f"No workunits found with state '{args.state}'", file=sys.stderr)
            return 1

        # When filtering by state, output only WUIDs
        try:
            for wu in filtered_workunits:
                print(wu['wuid'])
        except BrokenPipeError:
            # Handle pipe being closed (e.g., when piping to head)
            # Suppress the error and exit cleanly
            sys.stderr.close()
            pass
    else:
        # Normal output with WUID and state
        try:
            print(f"{'WUID':<20} {'State'}")
            print("-" * 50)
            for wu in workunits:
                print(f"{wu['wuid']:<20} {wu['state']}")

            print(f"\nTotal workunits: {len(workunits)}")
        except BrokenPipeError:
            # Handle pipe being closed (e.g., when piping to head)
            # Suppress the error and exit cleanly
            sys.stderr.close()
            pass

    return 0

if __name__ == '__main__':
    sys.exit(main())
