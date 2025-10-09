#!/usr/bin/env python3
"""
HPCC AKS Utility Script
A Python script to analyze AKS cluster resources and costs for HPCC workloads.
"""

import argparse
import subprocess
import json
import sys
import re
from typing import Dict, List, Tuple, Optional

class AKSUtility:
    def __init__(self, resource_group: str, cluster_name: str, namespace: str,
                 show_pods: bool = False, node_pool: str = None, ea_discount: float = 10,
                 ds_v4_family_discount: float = 10, use_azure_pricing: bool = False):
        self.resource_group = resource_group
        self.cluster_name = cluster_name
        self.namespace = namespace
        self.show_pods = show_pods
        self.node_pool = node_pool
        self.ea_discount = ea_discount
        self.ds_v4_family_discount = ds_v4_family_discount
        self.use_azure_pricing = use_azure_pricing

        # Auto-detect region when Azure pricing is enabled
        if use_azure_pricing:
            self.azure_region = self.get_cluster_location()
        else:
            self.azure_region = "eastus"  # Default for fallback scenarios

        self.messages = []

    def run_command(self, command: List[str]) -> str:
        """Execute a shell command and return the output."""
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Error running command {' '.join(command)}: {e}")
            sys.exit(1)

    def get_actual_node_cost(self, resource_id: str, days: int = 7) -> float:
        """Get actual cost for a node from Azure Cost Management API."""
        try:
            # This would require Azure SDK and proper authentication
            # Example implementation (commented out as it requires additional setup):
            """
            from azure.identity import DefaultAzureCredential
            from azure.mgmt.costmanagement import CostManagementClient
            
            credential = DefaultAzureCredential()
            cost_client = CostManagementClient(credential, subscription_id)
            
            # Query for actual costs in the last N days
            query = {
                "type": "ActualCost",
                "timeframe": "Custom",
                "timePeriod": {
                    "from": (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"),
                    "to": datetime.now().strftime("%Y-%m-%d")
                },
                "dataset": {
                    "granularity": "Daily",
                    "filter": {
                        "dimension": {
                            "name": "ResourceId",
                            "operator": "In",
                            "values": [resource_id]
                        }
                    }
                }
            }
            
            result = cost_client.query.usage(scope=f"/subscriptions/{subscription_id}", parameters=query)
            # Process result to get hourly cost
            """
            return 0.0  # Placeholder - would return actual cost
        except Exception:
            return 0.0

    def get_vm_hourly_cost(self, vm_size: str) -> float:
        """Get estimated hourly cost for Azure VM sizes with configurable discounts."""
        ea_multiplier = (100 - self.ea_discount) / 100
        ds_v4_family_multiplier = (100 - self.ds_v4_family_discount) / 100
        
        # VM pricing dictionary (East US pricing as of 2024)
        vm_prices = {
            # Standard D-series v5 (EA discount applied)
            "Standard_D2s_v5": 0.096 * ea_multiplier,
            "Standard_D4s_v5": 0.192 * ea_multiplier,
            "Standard_D8s_v5": 0.384 * ea_multiplier,
            "Standard_D16s_v5": 0.768 * ea_multiplier,
            "Standard_D32s_v5": 1.536 * ea_multiplier,
            "Standard_D48s_v5": 2.304 * ea_multiplier,
            "Standard_D64s_v5": 3.072 * ea_multiplier,

            # Standard D-series v4 (smaller sizes - EA discount applied)
            "Standard_D2s_v4": 0.096 * ea_multiplier,
            "Standard_D4s_v4": 0.192 * ea_multiplier,
            "Standard_D8s_v4": 0.384 * ea_multiplier,

            # Standard Dds_v4 family (D16ds v4 and larger, Linux) - Dds_v4 family discount
            "Standard_D16s_v4": 0.768 * ds_v4_family_multiplier,
            "Standard_D32s_v4": 1.536 * ds_v4_family_multiplier,
            "Standard_D48s_v4": 2.304 * ds_v4_family_multiplier,
            "Standard_D64s_v4": 3.072 * ds_v4_family_multiplier,

            # Standard E-series v5 (memory optimized - EA discount applied)
            "Standard_E2s_v5": 0.126 * ea_multiplier,
            "Standard_E4s_v5": 0.252 * ea_multiplier,
            "Standard_E8s_v5": 0.504 * ea_multiplier,
            "Standard_E16s_v5": 1.008 * ea_multiplier,
            "Standard_E32s_v5": 2.016 * ea_multiplier,
            "Standard_E48s_v5": 3.024 * ea_multiplier,
            "Standard_E64s_v5": 4.032 * ea_multiplier,

            # Standard F-series v2 (compute optimized - EA discount applied)
            "Standard_F2s_v2": 0.085 * ea_multiplier,
            "Standard_F4s_v2": 0.169 * ea_multiplier,
            "Standard_F8s_v2": 0.338 * ea_multiplier,
            "Standard_F16s_v2": 0.676 * ea_multiplier,
            "Standard_F32s_v2": 1.352 * ea_multiplier,
            "Standard_F48s_v2": 2.028 * ea_multiplier,
            "Standard_F64s_v2": 2.704 * ea_multiplier,
            "Standard_F72s_v2": 3.042 * ea_multiplier,

            # Standard B-series (burstable - EA discount applied)
            "Standard_B2s": 0.041 * ea_multiplier,
            "Standard_B2ms": 0.083 * ea_multiplier,
            "Standard_B4ms": 0.166 * ea_multiplier,
            "Standard_B8ms": 0.333 * ea_multiplier,
            "Standard_B12ms": 0.499 * ea_multiplier,
            "Standard_B16ms": 0.666 * ea_multiplier,
            "Standard_B20ms": 0.832 * ea_multiplier,
        }

        if vm_size in vm_prices:
            return vm_prices[vm_size]

        # Default fallback - estimate based on CPU count if available
        cpu_match = re.search(r'(\d+)', vm_size)
        if cpu_match:
            cpu_estimate = int(cpu_match.group(1))
            return cpu_estimate * 0.048 * ea_multiplier  # ~$0.048 per vCPU base estimate

        return 0.100 * ea_multiplier  # Default base estimate

    def get_azure_retail_price(self, vm_size: str, region: str = "eastus") -> float:
        """Get current Azure retail pricing using the Azure Retail Prices API."""
        try:
            import urllib.request
            import urllib.parse
            
            # Azure Retail Prices API endpoint
            base_url = "https://prices.azure.com/api/retail/prices"
            
            # Filter for the specific VM size and region
            filter_str = f"serviceName eq 'Virtual Machines' and armSkuName eq '{vm_size}' and armRegionName eq '{region}' and priceType eq 'Consumption'"
            params = {
                '$filter': filter_str
            }
            
            url = f"{base_url}?{urllib.parse.urlencode(params)}"
            with urllib.request.urlopen(url) as response:
                import json
                data = json.loads(response.read().decode())

                items = data.get('Items', [])
                if items:
                    # Get the retail price (per hour)
                    retail_price = items[0].get('retailPrice', 0)
                    return float(retail_price)

        except Exception as e:
            print(f"Warning: Could not fetch Azure retail price for {vm_size}: {e}")

        return 0.0

    def parse_cpu_value(self, cpu_str: str) -> float:
        """Parse CPU value from Kubernetes format."""
        if not cpu_str or cpu_str == "":
            return 0.0

        if cpu_str.endswith('m'):
            return float(cpu_str[:-1]) / 1000
        return float(cpu_str)

    def parse_memory_value(self, mem_str: str) -> float:
        """Parse memory value from Kubernetes format to GB."""
        if not mem_str or mem_str == "":
            return 0.0

        # Handle different memory units and convert to GB (Gigabytes)
        if mem_str.endswith('Ki'):
            return float(mem_str[:-2]) * 1024 / (1000 * 1000 * 1000)
        elif mem_str.endswith('Mi'):
            return float(mem_str[:-2]) * 1024 * 1024 / (1000 * 1000 * 1000)
        elif mem_str.endswith('Gi'):
            return float(mem_str[:-2]) * 1024 * 1024 * 1024 / (1000 * 1000 * 1000)
        elif mem_str.endswith('Ti'):
            return float(mem_str[:-2]) * 1024 * 1024 * 1024 * 1024 / (1000 * 1000 * 1000)
        elif mem_str.endswith('K'):
            # Decimal kilobytes to gigabytes
            return float(mem_str[:-1]) / (1000 * 1000)
        elif mem_str.endswith('M'):
            # Decimal megabytes to gigabytes
            return float(mem_str[:-1]) / 1000
        elif mem_str.endswith('G'):
            # Decimal gigabytes
            return float(mem_str[:-1])
        elif mem_str.endswith('T'):
            # Decimal terabytes to gigabytes
            return float(mem_str[:-1]) * 1000
        else:
            # Assume bytes if no unit specified
            try:
                return float(mem_str) / (1000 * 1000 * 1000)
            except ValueError:
                return 0.0

    def get_aks_credentials(self):
        """Get credentials for the AKS cluster."""
        print(f"Getting AKS credentials for cluster {self.cluster_name}...")
        self.run_command([
            'az', 'aks', 'get-credentials',
            '--resource-group', self.resource_group,
            '--name', self.cluster_name,
            '--overwrite-existing'
        ])

    def get_node_pools(self) -> List[str]:
        """Get list of node pool names."""
        output = self.run_command([
            'az', 'aks', 'nodepool', 'list',
            '--resource-group', self.resource_group,
            '--cluster-name', self.cluster_name,
            '--query', '[].name',
            '-o', 'tsv'
        ])
        return output.split('\n') if output else []

    def get_cluster_location(self) -> str:
        """Get the Azure region/location of the AKS cluster."""
        try:
            output = self.run_command([
                'az', 'aks', 'show',
                '--resource-group', self.resource_group,
                '--name', self.cluster_name,
                '--query', 'location',
                '-o', 'tsv'
            ])
            return output.lower() if output else "eastus"
        except:
            return "eastus"

    def get_vm_size_for_pool(self, pool_name: str) -> str:
        """Get VM size for a specific node pool."""
        try:
            output = self.run_command([
                'az', 'aks', 'nodepool', 'show',
                '--resource-group', self.resource_group,
                '--cluster-name', self.cluster_name,
                '--name', pool_name,
                '--query', 'vmSize',
                '-o', 'tsv'
            ])
            return output if output else "Unknown"
        except:
            return "Unknown"

    def get_nodes_for_pool(self, pool_name: str) -> List[Dict]:
        """Get node information for a specific pool."""
        output = self.run_command([
            'kubectl', 'get', 'nodes',
            '-l', f'agentpool={pool_name}',
            '-o', 'json'
        ])

        if not output:
            return []

        nodes_data = json.loads(output)
        nodes = []

        for node in nodes_data.get('items', []):
            node_name = node['metadata']['name']
            allocatable = node['status']['allocatable']
            capacity = node['status']['capacity']

            # Get node status
            conditions = node.get('status', {}).get('conditions', [])
            ready = False
            for condition in conditions:
                if condition.get('type') == 'Ready' and condition.get('status') == 'True':
                    ready = True
                    break

            # Extract Azure resource information
            provider_id = node['spec'].get('providerID', '')
            # Provider ID format: azure:///subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Compute/virtualMachineScaleSets/{vmss}/virtualMachines/{instance}
            resource_id = provider_id.replace('azure://', '') if provider_id.startswith('azure://') else ''

            nodes.append({
                'name': node_name,
                'cpu_allocatable': self.parse_cpu_value(allocatable.get('cpu', '0')),
                'cpu_capacity': self.parse_cpu_value(capacity.get('cpu', '0')),
                'memory_allocatable': self.parse_memory_value(allocatable.get('memory', '0')),
                'memory_capacity': self.parse_memory_value(capacity.get('memory', '0')),
                'ready': ready,
                'resource_id': resource_id
            })

        return nodes

    def get_node_usage(self, pool_name: str) -> Dict[str, Dict]:
        """Get CPU and memory usage for nodes in a pool."""
        try:
            output = self.run_command([
                'kubectl', 'top', 'nodes',
                '-l', f'agentpool={pool_name}',
                '--no-headers'
            ])

            usage_data = {}
            for line in output.split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 5:
                        node_name = parts[0]
                        cpu_used = self.parse_cpu_value(parts[1])
                        memory_used = self.parse_memory_value(parts[3])

                        usage_data[node_name] = {
                            'cpu_used': cpu_used,
                            'memory_used': memory_used
                        }

            return usage_data
        except:
            return {}

    def get_pod_usage(self, namespace: str = None) -> Dict[str, Dict]:
        """Get CPU and memory usage for pods."""
        try:
            cmd = ['kubectl', 'top', 'pods', '--no-headers']
            if namespace:
                cmd.extend(['--namespace', namespace])
            else:
                cmd.append('--all-namespaces')

            output = self.run_command(cmd)

            usage_data = {}
            for line in output.split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 3:
                        if namespace:
                            # Format: POD_NAME CPU MEMORY
                            pod_name = parts[0]
                            cpu_used = self.parse_cpu_value(parts[1])
                            memory_used = self.parse_memory_value(parts[2])
                        else:
                            # Format: NAMESPACE POD_NAME CPU MEMORY
                            if len(parts) >= 4:
                                pod_name = parts[1]
                                cpu_used = self.parse_cpu_value(parts[2])
                                memory_used = self.parse_memory_value(parts[3])
                            else:
                                continue

                        usage_data[pod_name] = {
                            'cpu_used': cpu_used,
                            'memory_used': memory_used
                        }

            return usage_data
        except:
            return {}

    def get_pods_data(self, namespace: str = None) -> List[Dict]:
        """Get pod information with resource requests."""
        cmd = ['kubectl', 'get', 'pods']
        if namespace:
            cmd.extend(['--namespace', namespace])
        else:
            cmd.append('--all-namespaces')
        cmd.extend(['-o', 'json'])

        output = self.run_command(cmd)
        if not output:
            return []

        pods_data = json.loads(output)
        pods = []

        for pod in pods_data.get('items', []):
            pod_name = pod['metadata']['name']
            pod_namespace = pod['metadata'].get('namespace', '')
            node_name = pod['spec'].get('nodeName', '')

            # Calculate total CPU and memory requests for all containers
            total_cpu_request = 0.0
            total_memory_request = 0.0
            total_cpu_limit = 0.0
            total_memory_limit = 0.0
            containers = pod['spec'].get('containers', [])
            for container in containers:
                resources = container.get('resources', {})
                requests = resources.get('requests', {})
                limits = resources.get('limits', {})
                cpu_request = requests.get('cpu', '0')
                memory_request = requests.get('memory', '0')
                cpu_limit = limits.get('cpu', '0')
                memory_limit = limits.get('memory', '0')
                total_cpu_request += self.parse_cpu_value(cpu_request)
                total_memory_request += self.parse_memory_value(memory_request)
                total_cpu_limit += self.parse_cpu_value(cpu_limit)
                total_memory_limit += self.parse_memory_value(memory_limit)

            # Get pod status information
            status = pod.get('status', {})
            phase = status.get('phase', '')

            # Check readiness
            ready = True
            ready_reason = ""
            conditions = status.get('conditions', [])
            for condition in conditions:
                if condition.get('type') == 'Ready' and condition.get('status') != 'True':
                    ready = False
                    break

            # Get container status reasons if not ready
            if not ready:
                container_statuses = status.get('containerStatuses', []) + status.get('initContainerStatuses', [])
                reasons = []
                for container_status in container_statuses:
                    state = container_status.get('state', {})
                    if 'waiting' in state and state['waiting'].get('reason'):
                        reasons.append(state['waiting']['reason'])
                    elif 'terminated' in state and state['terminated'].get('reason'):
                        reasons.append(state['terminated']['reason'])

                # Prioritize reasons
                priority_reasons = ['ImagePullBackOff', 'ErrImagePull', 'CrashLoopBackOff', 'PodInitializing']
                ready_reason = ""
                for priority in priority_reasons:
                    if priority in reasons:
                        ready_reason = priority
                        break

                if not ready_reason and reasons:
                    ready_reason = ", ".join(set(reasons))
                elif not ready_reason:
                    ready_reason = f"Pod phase: {phase}" if phase != "Running" else "Not Ready"

            pods.append({
                'name': pod_name,
                'namespace': pod_namespace,
                'node_name': node_name,
                'cpu_request': total_cpu_request,
                'memory_request': total_memory_request,
                'cpu_limit': total_cpu_limit,
                'memory_limit': total_memory_limit,
                'ready': ready,
                'ready_reason': ready_reason,
                'phase': phase
            })

        return pods

    def analyze_pool(self, pool_name: str) -> Dict:
        """Analyze a single node pool."""
        vm_size = self.get_vm_size_for_pool(pool_name)

        # Get cost - either from Azure API or static pricing
        if self.use_azure_pricing:
            vm_hourly_cost = self.get_azure_retail_price(vm_size, self.azure_region)
            if vm_hourly_cost == 0.0:
                # Fallback to static pricing if API fails
                vm_hourly_cost = self.get_vm_hourly_cost(vm_size)
                self.messages.append(f"{pool_name}: Using fallback pricing - Azure API unavailable for {vm_size} in {self.azure_region}")
            else:
                # Apply discounts to Azure retail price
                ea_multiplier = (100 - self.ea_discount) / 100
                vm_hourly_cost *= ea_multiplier
        else:
            vm_hourly_cost = self.get_vm_hourly_cost(vm_size)

        nodes = self.get_nodes_for_pool(pool_name)
        node_usage = self.get_node_usage(pool_name)

        # Get pods in the target namespace
        namespace_pods = self.get_pods_data(self.namespace)
        # Get all pods for total resource calculation
        all_pods = self.get_pods_data()

        # Initialize counters
        node_count = len(nodes)
        total_cpu_capacity = sum(node['cpu_capacity'] for node in nodes)
        total_cpu_allocatable = sum(node['cpu_allocatable'] for node in nodes)
        total_memory_capacity = sum(node['memory_capacity'] for node in nodes)
        total_memory_allocatable = sum(node['memory_allocatable'] for node in nodes)
        total_cpu_used = 0.0
        total_memory_used = 0.0
        namespace_cpu_requested = 0.0
        namespace_memory_requested = 0.0
        all_cpu_requested = 0.0
        all_memory_requested = 0.0

        pod_list = []
        not_ready_pods = []
        thor_agents = []
        thor_workers_managers = []

        # Get node names for this pool
        pool_node_names = [node['name'] for node in nodes]

        # Process usage data
        for node_name in pool_node_names:
            if node_name in node_usage:
                total_cpu_used += node_usage[node_name]['cpu_used']
                total_memory_used += node_usage[node_name]['memory_used']

        # Get pod usage data
        pod_usage = self.get_pod_usage(self.namespace)

        # Organize pods by node
        pods_by_node = {}
        for node in nodes:
            pods_by_node[node['name']] = []

        # Collect pods for this pool and organize by node
        pool_pods = []

        # Process pods
        for pod in namespace_pods:
            if pod['node_name'] in pool_node_names:
                namespace_cpu_requested += pod['cpu_request']
                namespace_memory_requested += pod['memory_request']

                # Add pod usage data
                pod_info = pod.copy()
                if pod['name'] in pod_usage:
                    pod_info['cpu_used'] = pod_usage[pod['name']]['cpu_used']
                    pod_info['memory_used'] = pod_usage[pod['name']]['memory_used']
                else:
                    pod_info['cpu_used'] = 0.0
                    pod_info['memory_used'] = 0.0

                pool_pods.append(pod_info)

                # Add to node-specific list
                if pod['node_name'] in pods_by_node:
                    pods_by_node[pod['node_name']].append(pod_info)

                if self.show_pods:
                    pod_list.append(pod['name'])

                if not pod['ready']:
                    not_ready_pods.append(f"{pod['name']} ({pod['ready_reason']})")

                # Check for thor conflicts
                if 'eclagent' in pod['name'] or 'thoragent' in pod['name']:
                    thor_agents.append(pod['name'])
                elif 'thorworker' in pod['name'] or 'thormanager' in pod['name']:
                    thor_workers_managers.append(pod['name'])

        # Add node usage data to nodes
        nodes_with_usage = []
        for node in nodes:
            node_info = node.copy()
            if node['name'] in node_usage:
                node_info['cpu_used'] = node_usage[node['name']]['cpu_used']
                node_info['memory_used'] = node_usage[node['name']]['memory_used']
            else:
                node_info['cpu_used'] = 0.0
                node_info['memory_used'] = 0.0
            node_info['pods'] = pods_by_node[node['name']]
            nodes_with_usage.append(node_info)

        # Process all pods for total resource calculation
        for pod in all_pods:
            if pod['node_name'] in pool_node_names:
                all_cpu_requested += pod['cpu_request']
                all_memory_requested += pod['memory_request']

        # Calculate percentages
        cpu_allocated_percent = int((total_cpu_allocatable / total_cpu_capacity * 100)) if total_cpu_capacity > 0 else 0
        cpu_inuse_percent = int((total_cpu_used / total_cpu_allocatable * 100)) if total_cpu_allocatable > 0 else 0
        hpcc_percent = int((namespace_cpu_requested / total_cpu_allocatable * 100)) if total_cpu_allocatable > 0 else 0
        memory_used_percent = int((total_memory_used / total_memory_allocatable * 100)) if total_memory_allocatable > 0 else 0

        total_hourly_cost = node_count * vm_hourly_cost

        # Generate messages
        pool_messages = []

        if namespace_cpu_requested < total_cpu_allocatable and namespace_cpu_requested > 0:
            pool_messages.append(f"{pool_name}: Some pods lack CPU resource requests - this prevents proper scheduling and resource guarantees")

        if not_ready_pods:
            not_ready_list = '; '.join(not_ready_pods)
            pool_messages.append(f"{pool_name}: Not ready pods: {not_ready_list}")

        if thor_agents and thor_workers_managers:
            pool_messages.append(f"{pool_name}: Thor agents and workers/managers running in same node pool - this prevents the node pool from scaling to 0")

        # Check for wasted resources
        if any(pool_type in pool_name for pool_type in ['thorpool', 'miscpool', 'roxiepool', 'spraypool']):
            if hpcc_percent == 0 and cpu_inuse_percent > 0:
                pool_messages.append(f"{pool_name}: This node pool has no active HPCC workloads but nodes are still consuming {cpu_inuse_percent}% CPU. Consider scaling down or investigating system processes to reduce unnecessary costs (${total_hourly_cost:.2f}/hr)")

        # Calculate cost efficiency metrics
        cost_per_allocated_cpu = total_hourly_cost / total_cpu_allocatable if total_cpu_allocatable > 0 else 0
        cost_per_used_cpu = total_hourly_cost / total_cpu_used if total_cpu_used > 0 else 0
        cost_per_allocated_gb = total_hourly_cost / total_memory_allocatable if total_memory_allocatable > 0 else 0

        # Calculate waste metrics
        cpu_waste_percent = ((total_cpu_allocatable - namespace_cpu_requested) / total_cpu_allocatable * 100) if total_cpu_allocatable > 0 else 0
        memory_waste_percent = ((total_memory_allocatable - namespace_memory_requested) / total_memory_allocatable * 100) if total_memory_allocatable > 0 else 0

        # Calculate potential savings
        if cpu_waste_percent > 20:  # If more than 20% CPU waste
            potential_savings = total_hourly_cost * (cpu_waste_percent / 100) * 0.5  # Conservative 50% of waste
            pool_messages.append(f"{pool_name}: High CPU waste ({cpu_waste_percent:.0f}%) - potential savings: ${potential_savings:.2f}/hr by rightsizing")

        return {
            'pool_name': pool_name,
            'node_count': node_count,
            'cpu_requested': namespace_cpu_requested,
            'cpu_capacity': total_cpu_capacity,
            'cpu_allocatable': total_cpu_allocatable,
            'cpu_allocated_percent': cpu_allocated_percent,
            'cpu_used': total_cpu_used,
            'cpu_inuse_percent': cpu_inuse_percent,
            'hpcc_percent': hpcc_percent,
            'memory_requested': namespace_memory_requested,
            'memory_capacity': total_memory_capacity,
            'memory_used': total_memory_used,
            'memory_used_percent': memory_used_percent,
            'total_hourly_cost': total_hourly_cost,
            'cost_per_allocated_cpu': cost_per_allocated_cpu,
            'cost_per_used_cpu': cost_per_used_cpu,
            'cost_per_allocated_gb': cost_per_allocated_gb,
            'cpu_waste_percent': cpu_waste_percent,
            'memory_waste_percent': memory_waste_percent,
            'pod_list': pod_list,
            'pods': pool_pods,
            'nodes': nodes_with_usage,
            'messages': pool_messages
        }

    def print_header(self):
        """Print the table header."""
        print(f"{'Node Pool':<15} {'NumNodes':>10} {'CPUs':>10} {'CPU':>10} {'CPUs':>10} {'CPUs':>10} {'CPUs':>10} {'CPUs':>10} {'CPUs':>10} {'Mem':>10} {'Mem':>10} {'Mem':>10}     {'Cost/Hr':>10}")
        print(f"{'':>26} {'Requested':>10} {'Capacity':>10} {'Allocated':>10} {'Alloc(%)':>10} {'In Use':>10} {'InUse(%)':>10} {'HPCC(%)':>10} {'Req(GB)':>10} {'Cap(GB)':>10} {'Used(%)':>10}     {'USD':>10}")
        print("-" * 185)

    def print_node_header(self):
        """Print the node details header."""
        print(f"  {'Node Name':<35} {'Status':>10} {'CPU':>10} {'CPU':>10} {'CPU':>10} {'Mem':>10} {'Mem':>10} {'Mem':>10}")
        print(f"  {'':>46} {'Cap':>10} {'Alloc':>10} {'Used':>10} {'Cap(GB)':>10} {'Alloc(GB)':>10} {'Used(GB)':>10}")
        print("  " + "-" * 130)

    def print_pod_subheader(self):
        """Print the pod details subheader under nodes."""
        print(f"    {'Pod Name':<45} {'Status':>10} {'CPU':>8} {'CPU':>8} {'CPU':>8} {'Mem':>8} {'Mem':>8} {'Mem':>8}")
        print(f"    {'':>56} {'Req':>8} {'Limit':>8} {'Used':>8} {'Req(GB)':>8} {'Limit(GB)':>8} {'Used(GB)':>8}")
        print("    " + "-" * 140)

    def run(self):
        """Main execution method."""
        # Get AKS credentials
        self.get_aks_credentials()

        # Show pricing information if using Azure pricing
        if self.use_azure_pricing:
            print(f"Using Azure retail pricing for region: {self.azure_region}")

        # Get node pools
        node_pools = self.get_node_pools()
        if not node_pools:
            print("No node pools found.")
            return

        # Filter by specific node pool if specified
        if self.node_pool:
            if self.node_pool in node_pools:
                node_pools = [self.node_pool]
                print(f"Filtering to show only node pool: {self.node_pool}")
            else:
                print(f"Error: Node pool '{self.node_pool}' not found. Available node pools: {', '.join(node_pools)}")
                return

        # Print header
        self.print_header()

        # Analyze each pool and collect data
        pool_data_list = []
        for pool in node_pools:
            if not pool.strip():
                continue

            pool_data = self.analyze_pool(pool)
            pool_data_list.append(pool_data)
            self.messages.extend(pool_data['messages'])

            # Print pool data
            print(f"{pool_data['pool_name']:<15} {pool_data['node_count']:>10} {pool_data['cpu_requested']:>10.1f} {pool_data['cpu_capacity']:>10.1f} {pool_data['cpu_allocatable']:>10.1f} {pool_data['cpu_allocated_percent']:>9}% {pool_data['cpu_used']:>10.1f} {pool_data['cpu_inuse_percent']:>9}% {pool_data['hpcc_percent']:>9}% {pool_data['memory_requested']:>10.1f} {pool_data['memory_capacity']:>10.1f} {pool_data['memory_used_percent']:>9}%     ${pool_data['total_hourly_cost']:>9.2f}")

            # Print node and pod details if show_pods is enabled
            if self.show_pods and pool_data['nodes']:
                self.print_node_header()

                for node in pool_data['nodes']:
                    node_status = "Ready" if node['ready'] else "NotReady"

                    print(f"  {node['name']:<35} {node_status:>10} {node['cpu_capacity']:>10.1f} {node['cpu_allocatable']:>10.1f} {node['cpu_used']:>10.1f} {node['memory_capacity']:>10.1f} {node['memory_allocatable']:>10.1f} {node['memory_used']:>10.2f}")

                    # Print pods for this node
                    if node['pods']:
                        self.print_pod_subheader()
                        for pod in node['pods']:
                            status = "Ready" if pod['ready'] else pod['ready_reason']
                            # Truncate status if too long
                            if len(status) > 9:
                                status = status[:6] + "..."

                            print(f"    {pod['name']:<45} {status:>10} {pod['cpu_request']:>8.1f} {pod['cpu_limit']:>8.1f} {pod['cpu_used']:>8.1f} {pod['memory_request']:>8.2f} {pod['memory_limit']:>8.2f} {pod['memory_used']:>8.3f}")
                        print()  # Add blank line after node's pods
                print()  # Add blank line after all nodes

        # Print cost analysis summary
        self.print_cost_analysis(pool_data_list)

        # Print messages
        if self.messages:
            print("\nMessages:")
            for message in self.messages:
                print(f"  - {message}")

    def print_cost_analysis(self, pool_data_list: List[Dict]):
        """Print comprehensive cost analysis."""
        if not pool_data_list:
            return
            
        print("\n" + "="*80)
        print("COST ANALYSIS SUMMARY")
        print("="*80)
        
        # Calculate totals
        total_cost = sum(pool['total_hourly_cost'] for pool in pool_data_list)
        total_nodes = sum(pool['node_count'] for pool in pool_data_list)
        total_cpu_capacity = sum(pool['cpu_capacity'] for pool in pool_data_list)
        total_cpu_used = sum(pool['cpu_used'] for pool in pool_data_list)
        total_memory_capacity = sum(pool['memory_capacity'] for pool in pool_data_list)
        total_memory_used = sum(pool['memory_used'] for pool in pool_data_list)
        
        # Overall metrics
        print(f"Total Cluster Cost: ${total_cost:.2f}/hour (${total_cost * 24:.2f}/day, ${total_cost * 24 * 30:.0f}/month)")
        print(f"Total Nodes: {total_nodes}")
        print(f"Average Cost per Node: ${total_cost/total_nodes:.2f}/hour")
        
        # Utilization efficiency
        cpu_utilization = (total_cpu_used / total_cpu_capacity * 100) if total_cpu_capacity > 0 else 0
        memory_utilization = (total_memory_used / total_memory_capacity * 100) if total_memory_capacity > 0 else 0
        
        print(f"\nResource Utilization:")
        print(f"  CPU Utilization: {cpu_utilization:.1f}% ({total_cpu_used:.1f}/{total_cpu_capacity:.1f} cores)")
        print(f"  Memory Utilization: {memory_utilization:.1f}% ({total_memory_used:.1f}/{total_memory_capacity:.1f} GB)")
        
        # Cost efficiency by pool
        print(f"\nCost Efficiency by Pool:")
        print(f"{'Pool':<15} {'Cost/Hr':>10} {'$/CPU':>8} {'$/GB':>8} {'CPU Waste':>10} {'Mem Waste':>10}")
        print("-" * 75)
        
        for pool in pool_data_list:
            print(f"{pool['pool_name']:<15} "
                  f"${pool['total_hourly_cost']:>9.2f} "
                  f"${pool['cost_per_allocated_cpu']:>7.2f} "
                  f"${pool['cost_per_allocated_gb']:>7.2f} "
                  f"{pool['cpu_waste_percent']:>9.1f}% "
                  f"{pool['memory_waste_percent']:>9.1f}%")
        
        # Rightsizing recommendations
        print(f"\nRightsizing Opportunities:")
        high_waste_pools = [p for p in pool_data_list if p['cpu_waste_percent'] > 30 or p['memory_waste_percent'] > 50]
        if high_waste_pools:
            for pool in high_waste_pools:
                if pool['cpu_waste_percent'] > 30:
                    potential_savings = pool['total_hourly_cost'] * 0.3  # Conservative estimate
                    print(f"  - {pool['pool_name']}: {pool['cpu_waste_percent']:.0f}% CPU waste - consider reducing node size/count (potential savings: ${potential_savings:.2f}/hr)")
                if pool['memory_waste_percent'] > 50:
                    potential_savings = pool['total_hourly_cost'] * 0.2  # Conservative estimate  
                    print(f"  - {pool['pool_name']}: {pool['memory_waste_percent']:.0f}% memory waste - consider memory-optimized instances")
        else:
            print("  - No significant rightsizing opportunities detected")
        
        # Scaling recommendations
        print(f"\nScaling Recommendations:")
        idle_pools = [p for p in pool_data_list if p['hpcc_percent'] == 0 and 'system' not in p['pool_name']]
        if idle_pools:
            idle_cost = sum(p['total_hourly_cost'] for p in idle_pools)
            print(f"  - Idle pools detected: {', '.join(p['pool_name'] for p in idle_pools)}")
            print(f"    Cost impact: ${idle_cost:.2f}/hr (${idle_cost * 24 * 30:.0f}/month)")
            print(f"    Recommendation: Scale to zero or use spot instances")
        
        underutilized_pools = [p for p in pool_data_list if 0 < p['hpcc_percent'] < 25 and 'system' not in p['pool_name']]
        if underutilized_pools:
            print(f"  - Underutilized pools: {', '.join(p['pool_name'] for p in underutilized_pools)}")
            print(f"    Recommendation: Consider consolidating workloads or using smaller instances")
        
        print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description="HPCC AKS Utility - Analyze AKS cluster resources and costs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -g RESOURCE_GROUP --name CLUSTER_NAME --namespace NAMESPACE
  %(prog)s --resource-group RESOURCE_GROUP --cluster-name CLUSTER_NAME --namespace NAMESPACE --show-pods
  %(prog)s -g RESOURCE_GROUP --name CLUSTER_NAME --namespace NAMESPACE --node-pool servpool0
  %(prog)s -g RESOURCE_GROUP --name CLUSTER_NAME --namespace NAMESPACE --node-pool thorpool0 --show-pods
  %(prog)s -g RESOURCE_GROUP --name CLUSTER_NAME --namespace NAMESPACE --ea-discount 25 --ds_v4_family_discount 75
  %(prog)s -g RESOURCE_GROUP --name CLUSTER_NAME --namespace NAMESPACE --use-azure-pricing
        """
    )
    
    # Required arguments
    parser.add_argument('-g', '--resource-group', required=True, 
                       help='Azure resource group name')
    parser.add_argument('--name', '--cluster-name', dest='cluster_name', required=True,
                       help='AKS cluster name')
    parser.add_argument('--namespace', required=True,
                       help='Kubernetes namespace')
    
    # Optional arguments
    parser.add_argument('--show-pods', action='store_true',
                       help='Show pods in the output')
    parser.add_argument('--node-pool', type=str,
                       help='Filter to show only a specific node pool')
    parser.add_argument('--ea-discount', type=float, default=10,
                       help='Enterprise Agreement discount percentage (default: 10)')
    parser.add_argument('--ds_v4_family_discount', type=float, default=10,
                       help='Dds_v4 family discount percentage for D16ds v4 and larger (default: 10)')
    parser.add_argument('--use-azure-pricing', action='store_true',
                       help='Use live Azure Retail Prices API instead of static pricing (auto-detects region from cluster). Fetches current retail prices then applies EA discount specified by --ea-discount')
    
    args = parser.parse_args()
    
    # Create and run the utility
    utility = AKSUtility(
        resource_group=args.resource_group,
        cluster_name=args.cluster_name,
        namespace=args.namespace,
        show_pods=args.show_pods,
        node_pool=args.node_pool,
        ea_discount=args.ea_discount,
        ds_v4_family_discount=args.ds_v4_family_discount,
        use_azure_pricing=args.use_azure_pricing
    )
    
    utility.run()


if __name__ == '__main__':
    main()
