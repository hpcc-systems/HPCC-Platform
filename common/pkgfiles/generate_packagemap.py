#!/usr/bin/env python3
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
"""
Generate a Roxie Package Map for a deployed query.

This utility queries an HPCC ESP server to get information about a deployed
Roxie query and generates a package map XML file that can be used to recreate
the query's file mappings.

It assumes that filename aliases used in the query resolve to files with the
same name (i.e., the alias and the actual file have the same name).
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin
import json

try:
    import requests
except ImportError:
    print("Error: This script requires the 'requests' library.", file=sys.stderr)
    print("Install it with: pip install requests", file=sys.stderr)
    sys.exit(1)


class ESPClient:
    """Client for communicating with HPCC ESP services."""
    
    def __init__(self, server: str, username: Optional[str] = None, 
                 password: Optional[str] = None, verify_ssl: bool = True):
        """
        Initialize ESP client.
        
        Args:
            server: ESP server URL (e.g., http://localhost:8010)
            username: Optional username for authentication
            password: Optional password for authentication
            verify_ssl: Whether to verify SSL certificates
        """
        self.server = server.rstrip('/')
        self.auth = (username, password) if username and password else None
        self.verify_ssl = verify_ssl
        
    def _make_request(self, service: str, method: str, params: Dict) -> Dict:
        """
        Make a request to an ESP service.
        
        Args:
            service: Service name (e.g., 'WsWorkunits')
            method: Method name (e.g., 'WUQueryFiles')
            params: Request parameters
            
        Returns:
            Response dictionary
            
        Raises:
            requests.RequestException: If the request fails
        """
        url = f"{self.server}/{service}/{method}.json"
        
        try:
            response = requests.get(
                url,
                params=params,
                auth=self.auth,
                verify=self.verify_ssl,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error communicating with ESP server: {e}", file=sys.stderr)
            raise
    
    def get_query_files(self, target: str, query_id: str) -> Dict:
        """
        Get files used by a specific query.
        
        Args:
            target: Target cluster name
            query_id: Query ID or alias
            
        Returns:
            Dictionary with 'Files' and 'SuperFiles' lists
        """
        params = {
            'Target': target,
            'QueryId': query_id
        }
        
        result = self._make_request('WsWorkunits', 'WUQueryFiles', params)
        
        # Check for exceptions in response
        if 'Exceptions' in result.get('WUQueryFilesResponse', {}):
            exceptions = result['WUQueryFilesResponse']['Exceptions']
            if exceptions:
                raise Exception(f"ESP service error: {exceptions}")
        
        response = result.get('WUQueryFilesResponse', {})
        return {
            'Files': response.get('Files', {}).get('File', []),
            'SuperFiles': response.get('SuperFiles', {}).get('SuperFile', [])
        }
    
    def get_query_details(self, target: str, query_id: str) -> Dict:
        """
        Get details about a specific query.
        
        Args:
            target: Target cluster name
            query_id: Query ID or alias
            
        Returns:
            Dictionary with query details
        """
        params = {
            'QuerySet': target,
            'QueryId': query_id
        }
        
        result = self._make_request('WsWorkunits', 'WUQueryDetails', params)
        return result.get('WUQueryDetailsResponse', {})


class PackageMapGenerator:
    """Generates Roxie package map XML from query information."""
    
    def __init__(self, query_id: str, files: List[Dict], superfiles: List[Dict]):
        """
        Initialize the generator.
        
        Args:
            query_id: Query identifier
            files: List of regular files used by the query
            superfiles: List of superfiles used by the query
        """
        self.query_id = query_id
        self.files = files or []
        self.superfiles = superfiles or []
        
    def _normalize_filename(self, filename: str) -> str:
        """
        Normalize a filename by ensuring it has the tilde prefix.
        
        Args:
            filename: Input filename
            
        Returns:
            Normalized filename with ~ prefix
        """
        if not filename:
            return filename
        
        filename = filename.strip()
        if not filename.startswith('~'):
            return f'~{filename}'
        return filename
    
    def _create_package_element(self, parent: ET.Element, package_id: str) -> ET.Element:
        """
        Create a Package element.
        
        Args:
            parent: Parent XML element
            package_id: Package identifier
            
        Returns:
            Created Package element
        """
        package = ET.SubElement(parent, 'Package')
        package.set('id', package_id)
        return package
    
    def _add_base_reference(self, package: ET.Element, base_id: str):
        """
        Add a Base reference to a package.
        
        Args:
            package: Package XML element
            base_id: ID of the base package to reference
        """
        base = ET.SubElement(package, 'Base')
        base.set('id', base_id)
    
    def _add_superfile(self, package: ET.Element, superfile_name: str, 
                      subfiles: List[str]):
        """
        Add a SuperFile definition to a package.
        
        Args:
            package: Package XML element
            superfile_name: Name of the superfile
            subfiles: List of subfile names
        """
        if not subfiles:
            return
        
        superfile = ET.SubElement(package, 'SuperFile')
        superfile.set('id', self._normalize_filename(superfile_name))
        
        for subfile_name in subfiles:
            if subfile_name:  # Skip empty subfile names
                subfile = ET.SubElement(superfile, 'SubFile')
                subfile.set('value', self._normalize_filename(subfile_name))
    
    def _collect_all_files(self) -> Set[str]:
        """
        Collect all unique file names from regular files and superfiles.
        
        Returns:
            Set of all file names
        """
        all_files = set()
        
        # Add regular files
        for file_info in self.files:
            filename = file_info.get('FileName')
            if filename:
                all_files.add(filename)
        
        # Add subfiles from superfiles
        for sf in self.superfiles:
            subfiles = sf.get('SubFiles', {}).get('File', [])
            if isinstance(subfiles, str):
                subfiles = [subfiles]
            for subfile in subfiles:
                if subfile:
                    all_files.add(subfile)
        
        return all_files
    
    def _process_superfiles(self) -> Dict[str, List[str]]:
        """
        Process superfiles and their subfiles.
        
        Returns:
            Dictionary mapping superfile names to lists of subfile names
        """
        superfile_map = {}
        
        for sf in self.superfiles:
            sf_name = sf.get('Name')
            if not sf_name:
                continue
            
            # Get subfiles
            subfiles = sf.get('SubFiles', {}).get('File', [])
            if isinstance(subfiles, str):
                subfiles = [subfiles]
            
            # Assume aliases resolve to same-named files
            subfile_list = []
            for subfile in subfiles:
                if subfile:
                    # Use the subfile name as-is, assuming alias = actual file
                    subfile_list.append(subfile)
            
            if subfile_list:
                superfile_map[sf_name] = subfile_list
        
        return superfile_map
    
    def generate(self, include_file_comment: bool = True) -> str:
        """
        Generate the package map XML.
        
        Args:
            include_file_comment: Whether to include a comment about individual files
            
        Returns:
            XML string for the package map
        """
        # Create root element
        root = ET.Element('RoxiePackages')
        
        # Add comment
        comment_text = (
            f" Package map generated for query: {self.query_id} "
        )
        root.append(ET.Comment(comment_text))
        
        # Create query package that references data packages
        query_package = self._create_package_element(root, self.query_id)
        
        # Process superfiles
        superfile_map = self._process_superfiles()
        
        # If we have superfiles, create data packages for each
        data_package_ids = []
        
        for sf_name, subfiles in superfile_map.items():
            # Create a data package for this superfile
            # Use a simple naming scheme: normalize the superfile name for package id
            pkg_id = sf_name.replace('~', '').replace('::', '_')
            if not pkg_id.endswith('_data'):
                pkg_id = f"{pkg_id}_data"
            
            data_package_ids.append(pkg_id)
            
            # Add Base reference in query package
            self._add_base_reference(query_package, pkg_id)
            
            # Create the data package with SuperFile definition
            data_package = self._create_package_element(root, pkg_id)
            self._add_superfile(data_package, sf_name, subfiles)
        
        # Handle regular files (files not in superfiles)
        regular_files = []
        superfile_subfiles = set()
        for subfile_list in superfile_map.values():
            superfile_subfiles.update(subfile_list)
        
        for file_info in self.files:
            filename = file_info.get('FileName')
            if filename and filename not in superfile_subfiles:
                regular_files.append(filename)
        
        # If we have regular files, create a superfile for them or add comment
        if regular_files and include_file_comment:
            root.append(ET.Comment(
                f" Note: Query uses {len(regular_files)} individual file(s). "
                "These may be referenced directly or through superfiles not detected. "
                f"Files: {', '.join(regular_files[:5])}"
                f"{'...' if len(regular_files) > 5 else ''} "
            ))
        
        # Format the XML with indentation
        self._indent(root)
        
        # Generate XML string
        xml_declaration = '<?xml version="1.0" encoding="utf-8"?>\n'
        xml_string = ET.tostring(root, encoding='unicode')
        
        return xml_declaration + xml_string
    
    def _indent(self, elem: ET.Element, level: int = 0):
        """
        Add indentation to XML for pretty printing.
        
        Args:
            elem: XML element to indent
            level: Current indentation level
        """
        indent = "\n" + "  " * level
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = indent + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = indent
            for child in elem:
                self._indent(child, level + 1)
            if not child.tail or not child.tail.strip():
                child.tail = indent
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = indent


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Generate a Roxie package map for a deployed query',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate package map for a query
  %(prog)s --server http://localhost:8010 --target roxie --query MyQuery

  # Generate with authentication
  %(prog)s --server https://example.com:8010 --target roxie --query MyQuery \\
           --username admin --password secret

  # Output to file
  %(prog)s --server http://localhost:8010 --target roxie --query MyQuery \\
           --output myquery.xml

  # Skip SSL verification (not recommended for production)
  %(prog)s --server https://localhost:8010 --target roxie --query MyQuery \\
           --no-verify-ssl
        """
    )
    
    parser.add_argument(
        '--server',
        required=True,
        help='ESP server URL (e.g., http://localhost:8010)'
    )
    
    parser.add_argument(
        '--target',
        required=True,
        help='Target cluster/queryset name (e.g., roxie)'
    )
    
    parser.add_argument(
        '--query',
        required=True,
        help='Query ID or alias'
    )
    
    parser.add_argument(
        '--username',
        help='Username for authentication (optional)'
    )
    
    parser.add_argument(
        '--password',
        help='Password for authentication (optional)'
    )
    
    parser.add_argument(
        '--output',
        '-o',
        help='Output file path (default: stdout)'
    )
    
    parser.add_argument(
        '--no-verify-ssl',
        action='store_true',
        help='Disable SSL certificate verification (not recommended)'
    )
    
    parser.add_argument(
        '--no-file-comment',
        action='store_true',
        help='Do not include comment about individual files'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    try:
        # Create ESP client
        if args.verbose:
            print(f"Connecting to {args.server}...", file=sys.stderr)
        
        client = ESPClient(
            args.server,
            args.username,
            args.password,
            verify_ssl=not args.no_verify_ssl
        )
        
        # Get query files
        if args.verbose:
            print(f"Retrieving files for query '{args.query}' on target '{args.target}'...", 
                  file=sys.stderr)
        
        query_files = client.get_query_files(args.target, args.query)
        
        files = query_files.get('Files', [])
        superfiles = query_files.get('SuperFiles', [])
        
        if args.verbose:
            print(f"Found {len(files)} file(s) and {len(superfiles)} superfile(s)", 
                  file=sys.stderr)
        
        # Generate package map
        generator = PackageMapGenerator(args.query, files, superfiles)
        package_map_xml = generator.generate(include_file_comment=not args.no_file_comment)
        
        # Output the result
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(package_map_xml)
            if args.verbose:
                print(f"Package map written to {args.output}", file=sys.stderr)
        else:
            print(package_map_xml)
        
        if args.verbose:
            print("Package map generated successfully", file=sys.stderr)
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
