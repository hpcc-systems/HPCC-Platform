#!/usr/bin/env python3
"""
HPCC AKS Cluster Insights & Cost Optimization Script

This enhanced script provides comprehensive insights on AKS cluster utilization, costs, and health metrics
with a focus on identifying cost-saving opportunities and resource optimization.

Key Features:
- Real-time node utilization analysis with efficiency scoring
- Historical resource utilization tracking
- Idle and underutilized resource detection
- Cost optimization recommendations and scenarios
- Potential cost savings calculations
- Resource waste identification and quantification
- Actionable recommendations for cost reduction

Cost Optimization Capabilities:
- Identifies idle nodes (< 10% CPU & memory usage)
- Detects underutilized nodes (< 20% CPU or < 30% memory)
- Calculates potential savings from right-sizing and spot instances
- Provides efficiency grading (A-F) for the cluster
- Suggests specific optimization actions with risk assessment

Usage Examples:
    # Standard comprehensive report
    python hpcc-aks-cluster-insights.py --cluster my-cluster --subscription my-sub

    # Focused cost optimization report
    python hpcc-aks-cluster-insights.py --cluster my-cluster --subscription my-sub --cost-optimization

    # JSON output for automation
    python hpcc-aks-cluster-insights.py --cluster my-cluster --subscription my-sub --json

    # With enterprise discount and alternative D-series discount (D-series VMs get 30%, others get 25%)
    python hpcc-aks-cluster-insights.py --cluster my-cluster --subscription my-sub --enterprise-discount 25 --d-series-discount 30

    # Cost optimization with discounts for accurate savings calculations
    python hpcc-aks-cluster-insights.py --cluster my-cluster --subscription my-sub --cost-optimization --enterprise-discount 20 --d-series-discount 25

Prerequisites:
- Azure CLI installed and authenticated
- kubectl installed and configured
- Appropriate Azure permissions for the cluster and cost management
- Kubernetes metrics server enabled in the cluster
- Optional: Azure Cost Management extension (az extension add --name costmanagement) for detailed cost analysis

"""

import json
import subprocess
import sys
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
import argparse
import statistics
import re
from collections import defaultdict
import urllib.request
import urllib.error
import urllib.parse
import base64
import getpass


class AKSClusterInsights:
    def __init__(self, cluster_name: str, subscription: str, resource_group: Optional[str] = None,
                 enterprise_discount: float = 0.0, d_series_discount: float = 0.0, ecl_watch_url: Optional[str] = None,
                 ecl_watch_username: Optional[str] = None, ecl_watch_password: Optional[str] = None,
                 enable_timeout_protection: bool = False):
        self.cluster_name = cluster_name
        self.subscription = subscription
        self.resource_group = resource_group
        self.cluster_data = None
        self.history_file = f".aks-idle-history-{cluster_name}.json"
        self.enterprise_discount = max(0.0, min(100.0, enterprise_discount))  # Clamp between 0-100
        self.d_series_discount = max(0.0, min(100.0, d_series_discount))  # Clamp between 0-100
        self.ecl_watch_url = ecl_watch_url

        # ECL Watch credentials - will be resolved later with interactive prompts if needed
        self._ecl_watch_username_param = ecl_watch_username
        self._ecl_watch_password_param = ecl_watch_password
        self.ecl_watch_username = None
        self.ecl_watch_password = None
        self._credentials_resolved = False

        # ECL Watch job analysis caching
        self._ecl_watch_job_analysis = None
        self._ecl_watch_query_attempted = False

        # Cost API availability tracking
        self._cost_api_tested = False
        self._cost_api_available = False
        self._cost_api_message_shown = False

        # Timeout protection setting
        self.enable_timeout_protection = enable_timeout_protection

    def display_discount_info(self):
        """Display current discount settings and warn if using defaults."""
        print("💰 Cost Analysis Configuration:")

        if self.enterprise_discount == 0.0 and self.d_series_discount == 0.0:
            print("   ⚠️  WARNING: Using default pricing (no discounts applied)")
            print("   📝 Consider using --enterprise-discount and --d-series-discount flags")
            print("      for more accurate cost estimates with your negotiated Azure rates")
        else:
            print("   ✅ Discount configuration:")
            if self.enterprise_discount > 0:
                print(f"      • Enterprise discount: {self.enterprise_discount}% (applied to all VMs)")
            if self.d_series_discount > 0:
                print(f"      • D-series discount: {self.d_series_discount}% (alternative rate for D-series VMs)")

            # Show effective discount for D-series VMs
            if self.enterprise_discount > 0 and self.d_series_discount > 0:
                effective_d_discount = max(self.enterprise_discount, self.d_series_discount)
                print(f"      • Effective D-series discount: {effective_d_discount}% (higher of the two rates)")

        # Show timeout protection status
        if self.enable_timeout_protection:
            print("   ⏱️  Timeout protection: ENABLED (30 second limit for consumption API)")
        else:
            print("   ⏱️  Timeout protection: DISABLED (allows full cost data retrieval)")

        print()  # Add spacing

    def run_az_command(self, command: List[str], timeout: int = 60) -> Dict[str, Any]:
        """Run Azure CLI command and return JSON output with timeout protection."""
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True,
                timeout=timeout
            )
            return json.loads(result.stdout) if result.stdout else {}
        except subprocess.TimeoutExpired:
            print(f"Command timed out after {timeout} seconds: {' '.join(command)}", file=sys.stderr)
            return {"error": f"Command timed out after {timeout} seconds"}
        except subprocess.CalledProcessError as e:
            print(f"Error running command: {' '.join(command)}", file=sys.stderr)
            print(f"Error output: {e.stderr}", file=sys.stderr)
            return {}
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}", file=sys.stderr)
            return {}

    def set_subscription(self):
        """Set the active Azure subscription."""
        print(f"Setting subscription to: {self.subscription}")
        subprocess.run(
            ["az", "account", "set", "--subscription", self.subscription],
            check=True
        )

    def get_cluster_details(self) -> Dict[str, Any]:
        """Fetch AKS cluster details."""
        if not self.resource_group:
            # Find resource group
            clusters = self.run_az_command([
                "az", "aks", "list",
                "--subscription", self.subscription,
                "--query", f"[?name=='{self.cluster_name}']",
                "-o", "json"
            ])
            if clusters and len(clusters) > 0:
                self.resource_group = clusters[0].get("resourceGroup")

        if not self.resource_group:
            print(f"Error: Could not find resource group for cluster {self.cluster_name}", file=sys.stderr)
            return {}

        print(f"Fetching cluster details for {self.cluster_name}...")
        self.cluster_data = self.run_az_command([
            "az", "aks", "show",
            "--name", self.cluster_name,
            "--resource-group", self.resource_group,
            "-o", "json"
        ])
        return self.cluster_data

    def get_node_pool_summary(self) -> List[Dict[str, Any]]:
        """Summarize node pool information."""
        if not self.cluster_data:
            return []

        node_pools = self.cluster_data.get("agentPoolProfiles", [])
        summary = []

        for pool in node_pools:
            pool_name = pool.get("name", "").lower()

            # Determine pool purpose based on name patterns
            if "thor" in pool_name:
                if "d" in pool_name and any(size in pool_name for size in ["32", "48", "64"]):
                    thor_cluster = f"thor-{pool_name}"
                    purpose = "Thor Workers"
                    # Estimate workers from pool name if possible
                    if "400" in pool_name or "d48" in pool_name or "d64" in pool_name:
                        workers = 400
                    elif "100" in pool_name:
                        workers = 100
                    elif "40" in pool_name:
                        workers = 40
                    else:
                        workers = 0  # Unknown
                    description = f"Thor worker cluster ({workers} workers estimated)" if workers > 0 else "Thor worker cluster"
                elif "l" in pool_name:
                    thor_cluster = f"thor-{pool_name}"
                    purpose = "Thor Spill Workers"
                    workers = 400 if "400" in pool_name else 0
                    description = f"Thor spill worker cluster ({workers} workers estimated)" if workers > 0 else "Thor spill worker cluster"
                else:
                    thor_cluster = f"thor-{pool_name}"
                    purpose = "Thor Workers"
                    workers = 0
                    description = "Thor worker cluster"
            elif "serv" in pool_name:
                thor_cluster = "N/A"
                purpose = "Services & Admin"
                workers = 0
                description = "Admin and service pods (dali, esp, eclcc, etc.)"
            elif "dali" in pool_name or "file" in pool_name or "data" in pool_name:
                thor_cluster = "N/A"
                purpose = "Data File Services"
                workers = 0
                description = "Data file services (spray, rowservice, direct-access)"
            elif "system" in pool_name:
                thor_cluster = "N/A"
                purpose = "System Services"
                workers = 0
                description = "Kubernetes system services and infrastructure"
            elif "ops" in pool_name:
                thor_cluster = "N/A"
                purpose = "Operations"
                workers = 0
                description = "Operations and management services"
            else:
                # Default for unknown pools
                thor_cluster = "Unknown"
                purpose = "Unknown"
                workers = 0
                description = "Purpose not identified"

            pool_info = {
                "name": pool.get("name"),
                "vm_size": pool.get("vmSize"),
                "current_count": pool.get("count", 0),
                "min_count": pool.get("minCount", 0),
                "max_count": pool.get("maxCount", 0),
                "autoscaling": pool.get("enableAutoScaling", False),
                "mode": pool.get("mode"),
                "os_type": pool.get("osType"),
                "orchestrator_version": pool.get("orchestratorVersion"),
                "provisioning_state": pool.get("provisioningState"),
                "power_state": pool.get("powerState", {}).get("code", "Unknown"),
                "labels": pool.get("nodeLabels", {}),
                "thor_cluster": thor_cluster,
                "thor_purpose": purpose,
                "thor_workers": workers,
                "thor_description": description
            }
            summary.append(pool_info)

        return summary

    def _ensure_kubectl_access(self) -> bool:
        """Ensure kubectl access to the AKS cluster. Returns True if successful."""
        if not self.cluster_name or not self.resource_group:
            return False

        try:
            # Check if kubectl is available
            kubectl_check = subprocess.run(
                ["kubectl", "version", "--client"],
                capture_output=True,
                timeout=5,
                check=False
            )

            if kubectl_check.returncode != 0:
                return False

            # Get kubectl credentials
            cred_result = subprocess.run([
                "az", "aks", "get-credentials",
                "--name", self.cluster_name,
                "--resource-group", self.resource_group,
                "--overwrite-existing",
                "--only-show-errors"
            ], capture_output=True, timeout=60, check=False)

            if cred_result.returncode != 0:
                return False

            # Test connectivity with a quick cluster-info call
            kubectl_test = subprocess.run(
                ["kubectl", "cluster-info", "--request-timeout=15s"],
                capture_output=True,
                timeout=20,
                check=False
            )

            return kubectl_test.returncode == 0

        except Exception:
            return False

    def get_node_utilization(self) -> Dict[str, Any]:
        """Get node utilization metrics using kubectl, with Azure API fallback."""
        print("Fetching node utilization metrics...")

        # First ensure we have the required parameters
        if not self.cluster_name or not self.resource_group:
            print("Warning: Missing cluster name or resource group for kubectl authentication", file=sys.stderr)
            return self._get_node_utilization_fallback()

        try:
            # Check if kubectl is available
            kubectl_check = subprocess.run(
                ["kubectl", "version", "--client"],
                capture_output=True,
                timeout=10
            )

            if kubectl_check.returncode != 0:
                print("Warning: kubectl not available or not working", file=sys.stderr)
                return self._get_node_utilization_fallback()

            # Get kubectl credentials with better error handling
            print(f"Getting kubectl credentials for cluster {self.cluster_name}...")
            cred_result = subprocess.run([
                "az", "aks", "get-credentials",
                "--name", self.cluster_name,
                "--resource-group", self.resource_group,
                "--overwrite-existing",
                "--only-show-errors"  # Reduce noise
            ], capture_output=True, timeout=60, check=False)

            if cred_result.returncode != 0:
                error_msg = cred_result.stderr.decode() if cred_result.stderr else 'Unknown authentication error'
                print(f"Warning: Could not authenticate to AKS cluster: {error_msg}", file=sys.stderr)
                print("Possible causes:", file=sys.stderr)
                print("  - Insufficient permissions (need AKS Cluster User role)", file=sys.stderr)
                print("  - Network connectivity issues", file=sys.stderr)
                print("  - Cluster may be stopped or unavailable", file=sys.stderr)
                return self._get_node_utilization_fallback()

            # Test kubectl connectivity
            print("Testing kubectl connectivity...")
            kubectl_test = subprocess.run(
                ["kubectl", "cluster-info", "--request-timeout=30s"],
                capture_output=True,
                timeout=35,
                check=False
            )

            if kubectl_test.returncode != 0:
                error_msg = kubectl_test.stderr.decode() if kubectl_test.stderr else 'Connection test failed'
                print(f"Warning: kubectl connectivity test failed: {error_msg}", file=sys.stderr)
                return self._get_node_utilization_fallback()

            # Get node metrics
            print("Fetching node list...")
            nodes_result = subprocess.run(
                ["kubectl", "get", "nodes", "-o", "json", "--request-timeout=30s"],
                capture_output=True,
                text=True,
                timeout=35,
                check=False
            )

            if nodes_result.returncode != 0:
                error_msg = nodes_result.stderr if nodes_result.stderr else 'Failed to get nodes'
                print(f"Warning: Could not fetch nodes via kubectl: {error_msg}", file=sys.stderr)
                return self._get_node_utilization_fallback()

            if not nodes_result.stdout.strip():
                print("Warning: Empty response from kubectl get nodes", file=sys.stderr)
                return self._get_node_utilization_fallback()

            try:
                nodes = json.loads(nodes_result.stdout)
            except json.JSONDecodeError as e:
                print(f"Warning: Invalid JSON response from kubectl: {e}", file=sys.stderr)
                return self._get_node_utilization_fallback()

            # Create a mapping of node names to creation times
            node_ages = {}
            for node in nodes.get("items", []):
                node_name = node.get("metadata", {}).get("name", "")
                creation_time = node.get("metadata", {}).get("creationTimestamp", "")
                if creation_time and node_name:
                    try:
                        # Parse the creation timestamp
                        creation_dt = datetime.fromisoformat(creation_time.replace('Z', '+00:00'))
                        age_hours = (datetime.now(creation_dt.tzinfo) - creation_dt).total_seconds() / 3600
                        age_days = age_hours / 24
                        node_ages[node_name] = {
                            "age_hours": age_hours,
                            "age_days": age_days,
                            "creation_time": creation_time
                        }
                    except Exception:
                        node_ages[node_name] = {"age_hours": 0, "age_days": 0, "creation_time": "unknown"}

            # Get node utilization metrics (requires metrics server)
            print("Fetching node utilization metrics...")
            top_result = subprocess.run(
                ["kubectl", "top", "nodes", "--no-headers", "--request-timeout=30s"],
                capture_output=True,
                text=True,
                timeout=35,
                check=False
            )

            node_utilization = []
            if top_result.returncode == 0 and top_result.stdout.strip():
                for line in top_result.stdout.strip().split('\n'):
                    if line:
                        parts = line.split()
                        if len(parts) >= 5:
                            # Parse CPU and memory percentages with better error handling
                            cpu_percent_str = parts[2].rstrip('%')
                            memory_percent_str = parts[4].rstrip('%')

                            try:
                                # Handle cases where values might be '<unknown>' or empty
                                if cpu_percent_str == '<unknown>' or cpu_percent_str == '' or cpu_percent_str == 'unknown':
                                    cpu_percent = 0.0
                                else:
                                    cpu_percent = float(cpu_percent_str)
                            except (ValueError, TypeError):
                                cpu_percent = 0.0

                            try:
                                if memory_percent_str == '<unknown>' or memory_percent_str == '' or memory_percent_str == 'unknown':
                                    memory_percent = 0.0
                                else:
                                    memory_percent = float(memory_percent_str)
                            except (ValueError, TypeError):
                                memory_percent = 0.0

                            # Get age information for this node
                            node_name = parts[0]
                            age_info = node_ages.get(node_name, {"age_hours": 0, "age_days": 0, "creation_time": "unknown"})

                            node_utilization.append({
                                "name": parts[0],
                                "cpu_cores": parts[1],
                                "cpu_percent": parts[2],
                                "cpu_percent_numeric": cpu_percent,
                                "memory": parts[3],
                                "memory_percent": parts[4],
                                "memory_percent_numeric": memory_percent,
                                "age_hours": age_info["age_hours"],
                                "age_days": age_info["age_days"],
                                "creation_time": age_info["creation_time"]
                            })
            else:
                # kubectl top nodes failed - likely metrics server not available
                if top_result.returncode != 0:
                    error_msg = top_result.stderr if top_result.stderr else 'Metrics server may not be installed or accessible'
                    print(f"Warning: kubectl top nodes failed: {error_msg}", file=sys.stderr)
                    print("Note: Kubernetes metrics server is required for utilization data", file=sys.stderr)
                else:
                    print("Warning: No utilization data returned from metrics server", file=sys.stderr)

                # Create basic node entries without utilization metrics
                for node in nodes.get("items", []):
                    node_name = node.get("metadata", {}).get("name", "")
                    if node_name:
                        age_info = node_ages.get(node_name, {"age_hours": 0, "age_days": 0, "creation_time": "unknown"})
                        node_utilization.append({
                            "name": node_name,
                            "cpu_cores": "unknown",
                            "cpu_percent": "0%",
                            "cpu_percent_numeric": 0.0,  # Default to 0 since we can't measure
                            "memory": "unknown", 
                            "memory_percent": "0%",
                            "memory_percent_numeric": 0.0,  # Default to 0 since we can't measure
                            "age_hours": age_info["age_hours"],
                            "age_days": age_info["age_days"],
                            "creation_time": age_info["creation_time"],
                            "metrics_unavailable": True  # Flag to indicate missing metrics
                        })

            # Check if we got any utilization data
            has_real_metrics = any(not node.get("metrics_unavailable", False) for node in node_utilization)

            return {
                "total_nodes": len(nodes.get("items", [])),
                "node_metrics": node_utilization,
                "has_real_metrics": has_real_metrics,
                "warning": "Some utilization metrics unavailable - metrics server may not be installed" if not has_real_metrics else ""
            }
        except Exception as e:
            print(f"Warning: Could not fetch node utilization: {e}", file=sys.stderr)
            return self._get_node_utilization_fallback()

    def _get_node_utilization_fallback(self) -> Dict[str, Any]:
        """Fallback method to get basic node information when kubectl is not available."""
        print("Using Azure API fallback for node information (no utilization metrics available)")

        if not self.cluster_data:
            return {"error": "No cluster data available and kubectl access failed"}

        # Get basic node information from Azure API
        node_pools = self.cluster_data.get("agentPoolProfiles", [])
        total_nodes = sum(pool.get("count", 0) for pool in node_pools)

        # Create mock node metrics with zero utilization (since we can't get real metrics)
        node_metrics = []
        node_counter = 1

        for pool in node_pools:
            pool_name = pool.get("name", "unknown")
            vm_size = pool.get("vmSize", "unknown")
            node_count = pool.get("count", 0)

            # Create entries for each node in the pool
            for i in range(node_count):
                node_name = f"aks-{pool_name}-{node_counter:08d}-vmss000000"  # Typical AKS node naming
                node_metrics.append({
                    "name": node_name,
                    "cpu_cores": "unknown",
                    "cpu_percent": "0%",
                    "cpu_percent_numeric": 0.0,  # Set to 0 since we can't measure
                    "memory": "unknown",
                    "memory_percent": "0%", 
                    "memory_percent_numeric": 0.0,  # Set to 0 since we can't measure
                    "age_hours": 0.0,
                    "age_days": 0.0,
                    "creation_time": "unknown",
                    "vm_size": vm_size,
                    "node_pool": pool_name,
                    "fallback_mode": True  # Flag to indicate this is estimated data
                })
                node_counter += 1

        return {
            "total_nodes": total_nodes,
            "node_metrics": node_metrics,
            "fallback_mode": True,
            "warning": "Utilization data unavailable - kubectl access required for real metrics"
        }

    def load_idle_history(self) -> Dict[str, Any]:
        """Load historical idle tracking data from local file."""
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load idle history: {e}", file=sys.stderr)
        return {"nodes": {}, "last_updated": None}

    def save_idle_history(self, history_data: Dict[str, Any]) -> None:
        """Save idle tracking data to local file."""
        try:
            with open(self.history_file, 'w') as f:
                json.dump(history_data, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save idle history: {e}", file=sys.stderr)

    def update_idle_tracking(self, utilization_data: Dict[str, Any]) -> Dict[str, Any]:
        """Track idle duration for nodes and update historical data."""
        if "error" in utilization_data or not utilization_data.get("node_metrics"):
            return {}

        current_time = datetime.now(timezone.utc).isoformat()
        history = self.load_idle_history()

        IDLE_CPU_THRESHOLD = 5.0
        IDLE_MEMORY_THRESHOLD = 10.0

        updated_nodes = {}

        for node in utilization_data["node_metrics"]:
            node_name = node["name"]
            cpu_percent = node.get("cpu_percent_numeric", 0)
            memory_percent = node.get("memory_percent_numeric", 0)

            is_currently_idle = (cpu_percent <= IDLE_CPU_THRESHOLD and
                               memory_percent <= IDLE_MEMORY_THRESHOLD)

            node_history = history["nodes"].get(node_name, {})

            if is_currently_idle:
                if node_history.get("idle_since"):
                    # Still idle - keep existing idle_since timestamp
                    updated_nodes[node_name] = {
                        "idle_since": node_history["idle_since"],
                        "last_seen": current_time,
                        "current_cpu": cpu_percent,
                        "current_memory": memory_percent,
                        "status": "idle"
                    }
                else:
                    # Just became idle - record the start time
                    updated_nodes[node_name] = {
                        "idle_since": current_time,
                        "last_seen": current_time,
                        "current_cpu": cpu_percent,
                        "current_memory": memory_percent,
                        "status": "idle"
                    }
            else:
                # Node is active - clear idle tracking but keep last seen
                if node_history.get("idle_since"):
                    # Was previously idle, now active
                    updated_nodes[node_name] = {
                        "idle_since": None,
                        "last_seen": current_time,
                        "current_cpu": cpu_percent,
                        "current_memory": memory_percent,
                        "status": "active",
                        "last_idle_duration": self.calculate_duration(node_history["idle_since"], current_time) if node_history.get("idle_since") else None
                    }
                else:
                    # Was already active
                    updated_nodes[node_name] = {
                        "idle_since": None,
                        "last_seen": current_time,
                        "current_cpu": cpu_percent,
                        "current_memory": memory_percent,
                        "status": "active"
                    }

        # Update history
        history["nodes"] = updated_nodes
        history["last_updated"] = current_time
        self.save_idle_history(history)

        return self.calculate_idle_durations(updated_nodes)

    def calculate_duration(self, start_time: str, end_time: str) -> Dict[str, Any]:
        """Calculate duration between two ISO timestamps."""
        try:
            start = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            duration = end - start

            total_hours = duration.total_seconds() / 3600
            days = int(total_hours // 24)
            hours = int(total_hours % 24)

            return {
                "total_hours": total_hours,
                "days": days,
                "hours": hours,
                "display": f"{days}d" if days > 0 else f"{hours}h"
            }
        except Exception:
            return {"total_hours": 0, "days": 0, "hours": 0, "display": "0h"}

    def calculate_idle_durations(self, nodes_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate current idle durations for all nodes."""
        current_time = datetime.now(timezone.utc).isoformat()
        idle_durations = {}

        for node_name, node_data in nodes_data.items():
            if node_data["status"] == "idle" and node_data.get("idle_since"):
                duration = self.calculate_duration(node_data["idle_since"], current_time)
                idle_durations[node_name] = {
                    "idle_since": node_data["idle_since"],
                    "idle_duration": duration,
                    "current_cpu": node_data.get("current_cpu", 0),
                    "current_memory": node_data.get("current_memory", 0)
                }

        return idle_durations

    def analyze_resource_efficiency(self, utilization_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze resource efficiency and identify optimization opportunities."""
        if "error" in utilization_data or not utilization_data.get("node_metrics"):
            return {"error": "No utilization data available"}

        # Check if we're in fallback mode (no real utilization data)
        is_fallback_mode = utilization_data.get("fallback_mode", False)

        # Update idle duration tracking (skip if in fallback mode)
        idle_durations = {}
        if not is_fallback_mode:
            idle_durations = self.update_idle_tracking(utilization_data)

        node_metrics = utilization_data["node_metrics"]

        # Define thresholds for analysis
        LOW_CPU_THRESHOLD = 15.0  # CPU usage below this is considered underutilized
        LOW_MEMORY_THRESHOLD = 30.0  # Memory usage below this is considered underutilized
        IDLE_CPU_THRESHOLD = 5.0  # CPU usage below this is considered idle
        IDLE_MEMORY_THRESHOLD = 10.0  # Memory usage below this is considered idle

        idle_nodes = []
        underutilized_nodes = []
        well_utilized_nodes = []

        cpu_usage_list = []
        memory_usage_list = []

        for node in node_metrics:
            cpu_pct = node.get("cpu_percent_numeric", 0)
            mem_pct = node.get("memory_percent_numeric", 0)

            cpu_usage_list.append(cpu_pct)
            memory_usage_list.append(mem_pct)

            # Add node pool information to each node for detailed analysis
            node_name = node.get("name", "")
            node_pool = self._extract_node_pool_from_name(node_name)
            node["node_pool"] = node_pool

            # Ensure we have valid numeric values for calculations
            cpu_numeric = max(0.0, cpu_pct) if cpu_pct is not None else 0.0
            mem_numeric = max(0.0, mem_pct) if mem_pct is not None else 0.0
            node["waste_level"] = cpu_numeric + mem_numeric  # Combined utilization for sorting

            # Add age-based priority scoring (older idle nodes have higher priority for removal)
            age_days = node.get("age_days", 0)
            if cpu_numeric <= IDLE_CPU_THRESHOLD and mem_numeric <= IDLE_MEMORY_THRESHOLD:
                # For idle nodes, prioritize older nodes for removal
                node["removal_priority"] = age_days * 10 + (10 - cpu_numeric - mem_numeric)
            else:
                node["removal_priority"] = 0

            # Ensure we have string representations for display
            if not node.get("cpu_percent") or node["cpu_percent"] == "<unknown>":
                node["cpu_percent"] = f"{cpu_numeric:.0f}%"
            if not node.get("memory_percent") or node["memory_percent"] == "<unknown>":
                node["memory_percent"] = f"{mem_numeric:.0f}%"

            # Add formatted age display
            if age_days >= 1:
                node["age_display"] = f"{age_days:.0f}d"
            elif age_days >= 0.04:  # More than ~1 hour
                hours = age_days * 24
                node["age_display"] = f"{hours:.0f}h"
            else:
                node["age_display"] = "<1h"

            # Add idle duration information
            node_idle_info = idle_durations.get(node_name, {})
            if node_idle_info:
                idle_duration = node_idle_info.get("idle_duration", {})
                node["idle_duration_hours"] = idle_duration.get("total_hours", 0)
                node["idle_duration_display"] = idle_duration.get("display", "0h")
                node["idle_since"] = node_idle_info.get("idle_since")

                # Update removal priority for idle nodes to include idle duration
                if cpu_numeric <= IDLE_CPU_THRESHOLD and mem_numeric <= IDLE_MEMORY_THRESHOLD:
                    # Prioritize nodes that have been idle longer
                    idle_hours = idle_duration.get("total_hours", 0)
                    node["removal_priority"] = idle_hours * 100 + age_days * 10 + (10 - cpu_numeric - mem_numeric)
            else:
                node["idle_duration_hours"] = 0
                node["idle_duration_display"] = "0h"
                node["idle_since"] = None

            # Categorize nodes
            if cpu_numeric <= IDLE_CPU_THRESHOLD and mem_numeric <= IDLE_MEMORY_THRESHOLD:
                idle_nodes.append(node)
            elif not (cpu_numeric <= IDLE_CPU_THRESHOLD and mem_numeric <= IDLE_MEMORY_THRESHOLD) and ((cpu_numeric > IDLE_CPU_THRESHOLD and cpu_numeric <= LOW_CPU_THRESHOLD) or mem_numeric <= LOW_MEMORY_THRESHOLD):
                underutilized_nodes.append(node)
            else:
                well_utilized_nodes.append(node)

        # Sort nodes by removal priority (highest priority first) for idle nodes
        # and by waste level for underutilized nodes
        idle_nodes.sort(key=lambda x: x["removal_priority"], reverse=True)
        underutilized_nodes.sort(key=lambda x: x["waste_level"])

        # Calculate statistics
        avg_cpu = statistics.mean(cpu_usage_list) if cpu_usage_list else 0
        avg_memory = statistics.mean(memory_usage_list) if memory_usage_list else 0

        # Check for suspicious metrics - warn if all nodes show 0% utilization
        zero_cpu_nodes = len([cpu for cpu in cpu_usage_list if cpu == 0.0])
        zero_mem_nodes = len([mem for mem in memory_usage_list if mem == 0.0])
        total_nodes = len(cpu_usage_list)

        metrics_warning = ""
        metrics_unreliable = False

        if total_nodes > 0:
            zero_cpu_percent = (zero_cpu_nodes / total_nodes) * 100
            zero_mem_percent = (zero_mem_nodes / total_nodes) * 100

            # If 100% of nodes show 0% utilization, the metrics are likely incorrect
            if zero_cpu_percent == 100 and zero_mem_percent == 100 and not is_fallback_mode:
                metrics_unreliable = True
                metrics_warning = (
                    f"⚠️  METRICS WARNING: All nodes show 0% CPU and memory usage. "
                    "This indicates metrics collection issues: (1) Metrics server problems, "
                    "(2) Insufficient permissions, (3) kubectl connectivity issues, or "
                    "(4) Data collection problems. Verify metrics server status and kubectl access."
                )
                print(f"Warning: {metrics_warning}", file=sys.stderr)
                print("Warning: Skipping efficiency analysis due to unreliable metrics", file=sys.stderr)
            elif avg_cpu == 0.0 and avg_memory == 0.0 and total_nodes > 5 and not is_fallback_mode:
                metrics_unreliable = True
                metrics_warning = (
                    "⚠️  METRICS WARNING: All nodes show 0% utilization across both CPU and memory. "
                    "This is highly unusual and likely indicates metrics collection issues. "
                    "Check: kubectl access, metrics server installation, and node readiness."
                )
                print(f"Warning: {metrics_warning}", file=sys.stderr)
                print("Warning: Skipping efficiency analysis due to unreliable metrics", file=sys.stderr)

        # Calculate idle duration statistics
        idle_duration_stats = {}
        if idle_nodes:
            idle_hours_list = [node.get("idle_duration_hours", 0) for node in idle_nodes if node.get("idle_duration_hours", 0) > 0]
            if idle_hours_list:
                idle_duration_stats = {
                    "nodes_with_duration": len(idle_hours_list),
                    "avg_idle_hours": round(statistics.mean(idle_hours_list), 1),
                    "max_idle_hours": round(max(idle_hours_list), 1),
                    "min_idle_hours": round(min(idle_hours_list), 1)
                }

        # Integrate ECL Watch job analysis if available - use single query approach
        job_analysis = None
        workload_context = {}
        if self.ecl_watch_url:
            try:
                # Use single ECL Watch query for all job analysis
                job_analysis = self._get_unified_ecl_watch_data()
                if "error" not in job_analysis:
                    workload_context = job_analysis.get("workload_assessment", {})
            except Exception as e:
                print(f"Warning: ECL Watch job analysis failed: {e}", file=sys.stderr)

        # Check if we should skip efficiency analysis due to unreliable or missing metrics
        # Also skip if we're in fallback mode with active workloads (conflicting signals)
        active_jobs_detected = False
        if job_analysis and "error" not in job_analysis:
            job_stats = job_analysis.get("job_statistics", {})
            active_jobs_detected = (
                job_stats.get("currently_running", 0) > 0 or 
                job_stats.get("total_active_jobs", 0) > 0 or
                job_stats.get("recent_jobs_2h", 0) > 5  # More than 5 jobs in last 2 hours suggests active workload
            )

        # Skip detailed analysis if metrics are unreliable OR if we're in fallback mode with suspicious data
        # In fallback mode, if we have a large cluster (>50 nodes) with all 0% utilization, it's likely unreliable
        large_cluster_fallback = (
            is_fallback_mode and 
            len(node_metrics) > 50 and 
            avg_cpu == 0.0 and 
            avg_memory == 0.0
        )

        skip_detailed_analysis = (
            metrics_unreliable or 
            (is_fallback_mode and active_jobs_detected and avg_cpu == 0.0 and avg_memory == 0.0) or
            large_cluster_fallback
        )



        if skip_detailed_analysis:
            efficiency_analysis = {
                "summary": {
                    "total_nodes": len(node_metrics),
                    "idle_nodes": 0,  # Don't categorize when metrics are unreliable
                    "underutilized_nodes": 0,
                    "well_utilized_nodes": 0,
                    "average_cpu_usage": 0.0,  # Don't use unreliable averages
                    "average_memory_usage": 0.0,
                    "efficiency_score": 0.0,  # Can't calculate efficiency with bad metrics
                    "idle_duration_stats": {},
                    "workload_context": workload_context,
                    "fallback_mode": is_fallback_mode,
                    "utilization_warning": utilization_data.get("warning", "") if is_fallback_mode else "",
                    "metrics_warning": metrics_warning if metrics_unreliable else (
                        "⚠️ CONFLICTING SIGNALS: Active jobs detected but all nodes show 0% utilization. "
                        "This suggests kubectl/metrics server issues. Skipping utilization-based analysis." 
                        if is_fallback_mode and active_jobs_detected else ""
                    ),
                    "metrics_unreliable": True,
                    "zero_cpu_nodes": zero_cpu_nodes,
                    "zero_mem_nodes": zero_mem_nodes
                },
                "idle_nodes": [],  # Empty lists when metrics are unreliable
                "underutilized_nodes": [],
                "well_utilized_nodes": [],
                "idle_durations": {},
                "thresholds": {
                    "low_cpu": LOW_CPU_THRESHOLD,
                    "low_memory": LOW_MEMORY_THRESHOLD,
                    "idle_cpu": IDLE_CPU_THRESHOLD,
                    "idle_memory": IDLE_MEMORY_THRESHOLD
                },
                "job_analysis": job_analysis,
                "fallback_mode": is_fallback_mode,
                "metrics_unreliable": True
            }
        else:
            # Normal efficiency analysis with reliable metrics
            efficiency_analysis = {
                "summary": {
                    "total_nodes": len(node_metrics),
                    "idle_nodes": len(idle_nodes),
                    "underutilized_nodes": len(underutilized_nodes),
                    "well_utilized_nodes": len(well_utilized_nodes),
                    "average_cpu_usage": round(avg_cpu, 1),
                    "average_memory_usage": round(avg_memory, 1),
                    "efficiency_score": round((avg_cpu + avg_memory) / 2, 1),
                    "idle_duration_stats": idle_duration_stats,
                    "workload_context": workload_context,
                    "fallback_mode": is_fallback_mode,
                    "utilization_warning": utilization_data.get("warning", "") if is_fallback_mode else "",
                    "metrics_warning": metrics_warning,
                    "metrics_unreliable": False,
                    "zero_cpu_nodes": zero_cpu_nodes,
                    "zero_mem_nodes": zero_mem_nodes
                },
                "idle_nodes": idle_nodes,
                "underutilized_nodes": underutilized_nodes,
                "well_utilized_nodes": well_utilized_nodes,
                "idle_durations": idle_durations,
                "thresholds": {
                    "low_cpu": LOW_CPU_THRESHOLD,
                    "low_memory": LOW_MEMORY_THRESHOLD,
                    "idle_cpu": IDLE_CPU_THRESHOLD,
                    "idle_memory": IDLE_MEMORY_THRESHOLD
                },
                "job_analysis": job_analysis,
                "fallback_mode": is_fallback_mode,
                "metrics_unreliable": False
            }

        return efficiency_analysis

    def _extract_node_pool_from_name(self, node_name: str) -> str:
        """Extract node pool name from node name."""
        if "aks-" in node_name:
            parts = node_name.split("-")
            if len(parts) >= 2:
                return parts[1]  # Usually the node pool name is the second part
        return "unknown"

    def get_detailed_node_analysis(self, efficiency_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get detailed analysis of idle and underutilized nodes by node pool."""
        if "error" in efficiency_data:
            return {"error": "No efficiency data available"}

        idle_nodes = efficiency_data.get("idle_nodes", [])
        underutilized_nodes = efficiency_data.get("underutilized_nodes", [])

        # Group by node pool
        idle_by_pool = defaultdict(list)
        underutilized_by_pool = defaultdict(list)

        for node in idle_nodes:
            pool = node.get("node_pool", "unknown")
            idle_by_pool[pool].append(node)

        for node in underutilized_nodes:
            pool = node.get("node_pool", "unknown")
            underutilized_by_pool[pool].append(node)

        # Calculate pool-level statistics
        pool_analysis = {}

        for pool in set(list(idle_by_pool.keys()) + list(underutilized_by_pool.keys())):
            idle_count = len(idle_by_pool[pool])
            underutil_count = len(underutilized_by_pool[pool])

            pool_analysis[pool] = {
                "idle_nodes": idle_count,
                "underutilized_nodes": underutil_count,
                "total_waste": idle_count + underutil_count,
                "idle_node_details": idle_by_pool[pool][:10],  # Top 10 worst
                "underutilized_node_details": underutilized_by_pool[pool][:10]  # Top 10 worst
            }

        return {
            "pool_analysis": pool_analysis,
            "worst_idle_nodes": idle_nodes[:20],  # Top 20 worst idle nodes overall
            "worst_underutilized_nodes": underutilized_nodes[:20],  # Top 20 worst underutilized nodes
            "summary": {
                "pools_with_idle_nodes": len(idle_by_pool),
                "pools_with_underutilized_nodes": len(underutilized_by_pool),
                "total_pools_affected": len(pool_analysis)
            }
        }

    def get_historical_utilization_metrics(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get historical utilization metrics from Azure Monitor."""
        print(f"Fetching historical utilization metrics for last {hours_back} hours...")

        if not self.cluster_data:
            return {"error": "Cluster data not available"}

        cluster_resource_id = self.cluster_data.get("id")
        if not cluster_resource_id:
            return {"error": "Cluster resource ID not found"}

        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)

        # Common metrics to fetch
        metrics = [
            "node_cpu_usage_percentage",
            "node_memory_working_set_percentage",
            "cluster_autoscaler_unneeded_nodes_count"
        ]

        historical_data = {}

        for metric in metrics:
            try:
                metric_result = self.run_az_command([
                    "az", "monitor", "metrics", "list",
                    "--resource", cluster_resource_id,
                    "--metric", metric,
                    "--start-time", start_time.isoformat(),
                    "--end-time", end_time.isoformat(),
                    "--aggregation", "Average",
                    "--interval", "PT1H",  # 1 hour intervals
                    "-o", "json"
                ])

                if metric_result and "value" in metric_result:
                    historical_data[metric] = metric_result["value"]

            except Exception as e:
                print(f"Warning: Could not fetch metric {metric}: {e}", file=sys.stderr)
                historical_data[metric] = {"error": str(e)}

        return {
            "period": f"{start_time.isoformat()} to {end_time.isoformat()}",
            "metrics": historical_data
        }

    def calculate_cost_optimization_opportunities(self) -> Dict[str, Any]:
        """Calculate potential cost savings from various optimization strategies using current and historical data."""
        if not self.cluster_data:
            return {"error": "Cluster data not available"}

        node_pools = self.cluster_data.get("agentPoolProfiles", [])
        costs = self.estimate_costs()
        utilization = self.get_node_utilization()
        efficiency = self.analyze_resource_efficiency(utilization)

        # Get historical cost data for enhanced waste analysis
        historical_costs = self.get_cost_analysis(days_back=14)  # 2 weeks of historical data
        historical_trends = self._calculate_historical_waste_trends(historical_costs)

        if "error" in efficiency:
            return {"error": "Cannot calculate optimizations without utilization data"}

        # Check if metrics are unreliable - if so, skip utilization-based optimizations
        if efficiency.get("metrics_unreliable", False):
            return {
                "current_monthly_cost": costs.get("estimated_monthly", 0),
                "optimization_scenarios": [],
                "total_potential_savings": 0,
                "recommendations": [
                    {
                        "priority": "High",
                        "category": "Metrics",
                        "recommendation": "Fix metrics collection issues before analyzing utilization",
                        "rationale": efficiency["summary"].get("metrics_warning", "Current utilization metrics appear unreliable"),
                        "action": "Verify kubectl access, metrics server installation, and cluster health"
                    }
                ],
                "historical_analysis": historical_trends,
                "metrics_unreliable": True,
                "note": "Utilization-based optimization skipped due to unreliable metrics"
            }

        opportunities = {
            "current_monthly_cost": costs.get("estimated_monthly", 0),
            "optimization_scenarios": [],
            "total_potential_savings": 0,
            "recommendations": [],
            "historical_analysis": historical_trends,
            "metrics_unreliable": False
        }

        # Scenario 1: Right-sizing based on utilization
        idle_nodes_count = efficiency["summary"]["idle_nodes"]
        underutilized_count = efficiency["summary"]["underutilized_nodes"]

        # Initialize savings variables
        idle_cost_savings = 0
        downsize_savings = 0

        if idle_nodes_count > 0:
            # Calculate savings from removing idle nodes with historical validation
            avg_node_cost = costs.get("estimated_monthly", 0) / max(costs.get("total_nodes", 1), 1)
            idle_cost_savings = idle_nodes_count * avg_node_cost

            # Enhance with historical data
            confidence_level = "Medium"
            description = f"Remove {idle_nodes_count} idle nodes with <10% CPU and memory usage"

            if historical_trends.get("idle_trend_days", 0) >= 7:
                confidence_level = "High"
                historical_waste = historical_trends.get("avg_daily_idle_waste", 0) * 30
                if historical_waste > 0:
                    idle_cost_savings = max(idle_cost_savings, historical_waste)
                    description += f" (Historical waste: ${historical_waste:,.2f}/month)"

            if historical_trends.get("idle_consistent", False):
                description += " - Consistently idle for multiple days"

            opportunities["optimization_scenarios"].append({
                "name": "Remove Idle Nodes",
                "description": description,
                "monthly_savings": round(idle_cost_savings, 2),
                "impact": "Low risk - nodes are barely used",
                "action": f"Scale down node pools or enable aggressive autoscaling",
                "confidence": confidence_level,
                "historical_validation": historical_trends.get("historical_waste_confidence", "low")
            })

        if underutilized_count > 0:
            # Calculate savings from downsizing underutilized nodes with historical validation
            avg_node_cost = costs.get("estimated_monthly", 0) / max(costs.get("total_nodes", 1), 1)
            downsize_savings = underutilized_count * avg_node_cost * 0.3  # 30% savings from smaller VMs

            # Enhance with historical utilization patterns
            confidence_level = "Medium"
            description = f"Consider smaller VM sizes for {underutilized_count} underutilized nodes"

            if historical_trends.get("utilization_trend_days", 0) >= 7:
                confidence_level = "High"
                description += f" (Based on {historical_trends['utilization_trend_days']} days of data)"
                if historical_trends.get("avg_cpu_utilization", 0) < 25:
                    downsize_savings *= 1.2  # Increase savings estimate for consistently low utilization
                    description += " - Consistently low CPU utilization"

            opportunities["optimization_scenarios"].append({
                "name": "Downsize Underutilized Nodes",
                "description": description,
                "monthly_savings": round(downsize_savings, 2),
                "impact": "Medium risk - requires testing workload performance",
                "action": "Test workloads on smaller VM sizes (e.g., move from D8 to D4)",
                "confidence": confidence_level,
                "historical_validation": historical_trends.get("historical_waste_confidence", "low")
            })

        # Scenario 2: Spot instances opportunities (mutually exclusive with removal/downsizing)
        system_pools = [p for p in node_pools if p.get("mode") == "System"]
        user_pools = [p for p in node_pools if p.get("mode") == "User"]

        if user_pools:
            user_pool_cost = sum(costs.get("by_pool", [{}])[i].get("monthly_cost", 0)
                               for i, p in enumerate(node_pools) if p.get("mode") == "User")
            # Only calculate spot savings on non-idle, efficiently used nodes
            remaining_user_cost = user_pool_cost - (idle_cost_savings + downsize_savings)
            remaining_user_cost = max(0, remaining_user_cost)  # Ensure non-negative
            spot_savings = remaining_user_cost * 0.7  # Up to 70% savings with spot instances

            if spot_savings > 100:  # Only recommend if meaningful savings
                opportunities["optimization_scenarios"].append({
                    "name": "Implement Spot Instances",
                    "description": f"Use spot instances for fault-tolerant user workloads",
                    "monthly_savings": round(spot_savings, 2),
                    "impact": "Medium risk - workloads must handle interruptions",
                    "action": "Create spot node pools for batch jobs and stateless applications"
                })

        # Generate specific recommendations
        opportunities["recommendations"] = self.generate_optimization_recommendations(
            efficiency, costs, node_pools
        )

        # Calculate realistic total potential savings (avoid double counting)
        total_monthly_cost = costs.get("estimated_monthly", 0)

        # Primary savings: idle nodes (immediate, non-overlapping)
        primary_savings = idle_cost_savings + downsize_savings

        # Alternative savings scenarios (mutually exclusive)
        scenario_savings = []
        for scenario in opportunities["optimization_scenarios"]:
            if scenario["name"] in ["Implement Spot Instances"]:
                scenario_savings.append(scenario["monthly_savings"])

        # Take the maximum of alternative scenarios, don't add them
        max_alternative_savings = max(scenario_savings) if scenario_savings else 0

        # Total is primary savings plus best alternative
        realistic_total_savings = primary_savings + max_alternative_savings

        # Ensure savings never exceed total cost
        realistic_total_savings = min(realistic_total_savings, total_monthly_cost * 0.95)  # Cap at 95% of total cost

        opportunities["total_potential_savings"] = round(realistic_total_savings, 2)

        # Add a note about calculation method
        opportunities["savings_calculation_note"] = (
            f"Total savings calculated as: immediate optimizations (${primary_savings:,.2f}) "
            f"plus best alternative scenario (${max_alternative_savings:,.2f}), "
            f"capped at 95% of total monthly cost"
        )

        # Add historical waste summary
        if historical_trends.get("idle_trend_days", 0) > 0:
            opportunities["historical_waste_summary"] = {
                "analysis_period_days": historical_trends.get("idle_trend_days", 0),
                "avg_daily_idle_waste": historical_trends.get("avg_daily_idle_waste", 0),
                "confidence_level": historical_trends.get("historical_waste_confidence", "low"),
                "idle_pattern_consistent": historical_trends.get("idle_consistent", False),
                "utilization_pattern_consistent": historical_trends.get("utilization_consistent", False)
            }

        return opportunities

    def generate_optimization_recommendations(self, efficiency: Dict[str, Any],
                                           costs: Dict[str, Any],
                                           node_pools: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Generate specific actionable recommendations."""
        recommendations = []

        # Check for autoscaling
        non_autoscaling_pools = [p for p in node_pools if not p.get("enableAutoScaling", False)]
        if non_autoscaling_pools:
            recommendations.append({
                "priority": "High",
                "category": "Autoscaling",
                "recommendation": f"Enable autoscaling on {len(non_autoscaling_pools)} node pools",
                "rationale": "Automatically scale nodes based on demand to reduce idle capacity",
                "action": "Configure min/max node counts with appropriate scaling policies"
            })

        # Check for oversized VMs
        efficiency_score = efficiency["summary"].get("efficiency_score", 0)
        if efficiency_score < 30:
            recommendations.append({
                "priority": "High",
                "category": "Right-sizing",
                "recommendation": "Consider smaller VM sizes for current workloads",
                "rationale": f"Average resource utilization is only {efficiency_score}%",
                "action": "Test workloads on VM sizes with 25-50% fewer resources"
            })

        # Check for multiple small pools
        small_pools = [p for p in node_pools if p.get("count", 0) <= 2 and p.get("count", 0) > 0]
        if len(small_pools) > 2:
            recommendations.append({
                "priority": "Medium",
                "category": "Consolidation",
                "recommendation": f"Consider consolidating {len(small_pools)} small node pools",
                "rationale": "Multiple small pools reduce scheduling efficiency and increase management overhead",
                "action": "Combine compatible workloads into fewer, larger node pools"
            })

        # Check for cost threshold
        monthly_cost = costs.get("estimated_monthly", 0)
        if monthly_cost > 1000:
            recommendations.append({
                "priority": "High",
                "category": "Cost Management",
                "recommendation": "Implement cost monitoring and alerts",
                "rationale": f"Monthly costs exceed $1,000 (current: ${monthly_cost:,.2f})",
                "action": "Set up Azure Cost Management alerts and regular cost reviews"
            })

        return recommendations

    def get_pod_statistics(self) -> Dict[str, Any]:
        """Get pod statistics."""
        print("Fetching pod statistics...")

        # Check kubectl access first
        if not self._ensure_kubectl_access():
            return {"error": "kubectl access not available"}

        try:
            # Get all pods
            pods_result = subprocess.run(
                ["kubectl", "get", "pods", "--all-namespaces", "-o", "json", "--request-timeout=300s"],
                capture_output=True,
                text=True,
                timeout=305,
                check=False
            )

            if pods_result.returncode != 0:
                error_msg = pods_result.stderr if pods_result.stderr else 'Failed to get pods'
                return {"error": f"Could not fetch pod statistics: {error_msg}"}

            if not pods_result.stdout.strip():
                return {"error": "No pod data returned"}

            pods = json.loads(pods_result.stdout)

            pod_items = pods.get("items", [])

            # Count pod phases
            phase_counts = {}
            namespace_counts = {}

            for pod in pod_items:
                phase = pod.get("status", {}).get("phase", "Unknown")
                namespace = pod.get("metadata", {}).get("namespace", "Unknown")

                phase_counts[phase] = phase_counts.get(phase, 0) + 1
                namespace_counts[namespace] = namespace_counts.get(namespace, 0) + 1

            return {
                "total_pods": len(pod_items),
                "by_phase": phase_counts,
                "by_namespace": namespace_counts
            }
        except Exception as e:
            print(f"Warning: Could not fetch pod statistics: {e}", file=sys.stderr)
            return {"error": str(e)}

    def get_pods_by_node(self) -> Dict[str, Any]:
        """Get detailed pod information organized by node."""
        # Check kubectl access first
        if not self._ensure_kubectl_access():
            print("Warning: kubectl access not available for pod analysis", file=sys.stderr)
            return {}

        try:
            # Get all pods with node information
            pods_result = subprocess.run(
                ["kubectl", "get", "pods", "--all-namespaces", "-o", "json",
                 "--field-selector=status.phase=Running", "--request-timeout=300s"],
                capture_output=True,
                text=True,
                timeout=305,
                check=False
            )

            if pods_result.returncode != 0:
                error_msg = pods_result.stderr if pods_result.stderr else 'Failed to get pods'
                print(f"Warning: Could not fetch pods via kubectl: {error_msg}", file=sys.stderr)
                return {}

            if not pods_result.stdout.strip():
                print("Warning: No pod data returned", file=sys.stderr)
                return {}

            pods = json.loads(pods_result.stdout)

            pods_by_node = {}

            for pod in pods.get("items", []):
                node_name = pod.get("spec", {}).get("nodeName")
                if not node_name:
                    continue

                pod_name = pod.get("metadata", {}).get("name", "")
                namespace = pod.get("metadata", {}).get("namespace", "")

                # Skip pods in system/infrastructure namespaces - these are not user workloads
                system_namespaces = {
                    "calico-system",
                    "kube-system",
                    "kube-public",
                    "kube-node-lease",
                    "cert-manager",
                    "ingress-core-internal",
                    "external-secrets",
                    "logging",
                    "monitoring"
                }
                if namespace in system_namespaces:
                    continue

                # Skip nfs-failure pods - these are infrastructure related
                if pod_name.startswith("nfs-failure"):
                    continue

                # Extract container names
                containers = []
                for container in pod.get("spec", {}).get("containers", []):
                    containers.append(container.get("name", ""))

                if node_name not in pods_by_node:
                    pods_by_node[node_name] = []

                pods_by_node[node_name].append({
                    "pod_name": pod_name,
                    "namespace": namespace,
                    "containers": containers
                })

            return pods_by_node
        except Exception as e:
            print(f"Warning: Could not fetch pod details: {e}", file=sys.stderr)
            return {}

    def analyze_agent_only_large_nodes(self, efficiency_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify large underutilized nodes that only have thoragent or thor-eclagent pods."""
        if "error" in efficiency_data:
            return []

        # Define what constitutes a "large" node (adjust these thresholds as needed)
        LARGE_VM_PATTERNS = [
            "D32", "D48", "D64",  # Large D-series
            "E32", "E48", "E64",  # Large E-series
            "L16", "L32", "L48", "L64",  # Large L-series
            "Standard_D16", "Standard_D32", "Standard_D48", "Standard_D64",
            "Standard_E16", "Standard_E32", "Standard_E48", "Standard_E64",
            "Standard_L16", "Standard_L32", "Standard_L48", "Standard_L64"
        ]

        # Agent container patterns to look for (case insensitive)
        AGENT_PATTERNS = ["thoragent", "thor-eclagent", "eclagent", "agent", "postrun"]

        pods_by_node = self.get_pods_by_node()
        underutilized_nodes = efficiency_data.get("underutilized_nodes", [])
        idle_nodes = efficiency_data.get("idle_nodes", [])

        agent_only_large_nodes = []



        # Analyze both idle and underutilized nodes for agent-only patterns
        all_nodes_to_check = underutilized_nodes + idle_nodes

        for node in all_nodes_to_check:
            node_name = node.get("name", "")

            # Check if this is a large node based on VM size patterns
            node_vm_size = ""
            node_pool_name = node.get("node_pool", "")

            # Try to get VM size from cluster data if available
            if self.cluster_data:
                for pool in self.cluster_data.get("agentPoolProfiles", []):
                    pool_name = pool.get("name", "")
                    # Match by pool name extracted from node analysis
                    if pool_name == node_pool_name or pool_name in node_name:
                        node_vm_size = pool.get("vmSize", "")
                        break

            is_large_node = any(pattern in node_vm_size for pattern in LARGE_VM_PATTERNS)



            if not is_large_node:
                continue

            # Get pods running on this node
            node_pods = pods_by_node.get(node_name, [])



            if not node_pods:
                # No pods or couldn't get pod info - still flag as potential issue
                pool_name = node.get("node_pool", "unknown")
                # Define compute pools that should NOT have agent-only nodes
                compute_pool_patterns = ["thord", "thorl"]  # Thor compute pools
                is_on_compute_pool = any(pattern in pool_name for pattern in compute_pool_patterns)

                # Determine severity for no-pods nodes
                if is_on_compute_pool:
                    issue_severity = "CRITICAL_MISPLACEMENT"  # No pods on expensive compute pool
                    recommendation = "🚨 CRITICAL - Remove from expensive compute pool"
                else:
                    issue_severity = "UNKNOWN_POOL"
                    recommendation = "❓ INVESTIGATE - Large node with no detected pods"

                agent_only_large_nodes.append({
                    "node_name": node_name,
                    "vm_size": node_vm_size,
                    "cpu_percent": node.get("cpu_percent_numeric", 0),
                    "memory_percent": node.get("memory_percent_numeric", 0),
                    "age_display": node.get("age_display", "?"),
                    "node_pool": pool_name,
                    "pods": [],
                    "issue_type": "no_pods_detected",
                    "issue_severity": issue_severity,
                    "is_on_compute_pool": is_on_compute_pool,
                    "estimated_monthly_cost": node.get("estimated_monthly_cost", 0),
                    "recommendation": recommendation
                })
                continue

            # Check if all pods are agent-only
            all_agent_pods = True
            pod_details = []
            non_agent_containers = []

            for pod_info in node_pods:
                pod_name = pod_info["pod_name"]
                containers = pod_info["containers"]
                namespace = pod_info["namespace"]

                pod_details.append({
                    "name": pod_name,
                    "namespace": namespace,
                    "containers": containers
                })

                # Check if any container is NOT an agent
                pod_has_non_agent = False
                for container in containers:
                    is_agent = any(agent_pattern in container.lower() for agent_pattern in AGENT_PATTERNS)
                    if not is_agent:
                        pod_has_non_agent = True
                        non_agent_containers.append(f"{pod_name}:{container}")
                        break

                if pod_has_non_agent:
                    all_agent_pods = False
                    break

            if all_agent_pods and len(node_pods) > 0:
                # Check if this agent-only node is on an inappropriate (compute) pool
                pool_name = node.get("node_pool", "unknown")

                # Define compute pools that should NOT have agent-only nodes
                compute_pool_patterns = ["thord", "thorl"]  # Thor compute pools
                server_pool_patterns = ["servd", "server", "admin", "agent", "system"]  # Appropriate pools for agents

                is_on_compute_pool = any(pattern in pool_name.lower() for pattern in compute_pool_patterns)
                is_on_server_pool = any(pattern in pool_name.lower() for pattern in server_pool_patterns)

                if is_on_compute_pool:
                    issue_severity = "CRITICAL_MISPLACEMENT"
                    recommendation = "🚨 RELOCATE - Move agents to server/admin pool"
                elif is_on_server_pool:
                    issue_severity = "APPROPRIATE_POOL"
                    recommendation = "✅ MONITOR - Agents on appropriate pool"
                else:
                    issue_severity = "UNKNOWN_POOL"
                    recommendation = "❓ INVESTIGATE - Unknown pool type"

                # This is a large node with only agent pods - flag it
                agent_only_large_nodes.append({
                    "node_name": node_name,
                    "vm_size": node_vm_size,
                    "cpu_percent": node.get("cpu_percent_numeric", 0),
                    "memory_percent": node.get("memory_percent_numeric", 0),
                    "age_display": node.get("age_display", "?"),
                    "node_pool": pool_name,
                    "pods": pod_details,
                    "pod_count": len(node_pods),
                    "issue_type": "agent_only_large_node",
                    "issue_severity": issue_severity,
                    "is_on_compute_pool": is_on_compute_pool,
                    "estimated_monthly_cost": node.get("estimated_monthly_cost", 0),
                    "recommendation": recommendation
                })

        # Sort by severity (critical misplacements first), then by VM size and waste level
        severity_priority = {
            "CRITICAL_MISPLACEMENT": 0,
            "UNKNOWN_POOL": 1,
            "APPROPRIATE_POOL": 2
        }
        agent_only_large_nodes.sort(key=lambda x: (
            severity_priority.get(x.get("issue_severity", "UNKNOWN_POOL"), 3),
            x["vm_size"],
            -(x["cpu_percent"] + x["memory_percent"])
        ))



        return agent_only_large_nodes

    def estimate_costs(self) -> Dict[str, Any]:
        """Estimate cluster costs based on VM pricing (approximate)."""
        if not self.cluster_data:
            return {}

        # Approximate hourly costs for common VM sizes (East US pricing - as of 2024)
        vm_pricing = {
            # D-series v4
            "Standard_D4ds_v4": 0.192,
            "Standard_D8ds_v4": 0.384,
            "Standard_D16ds_v4": 0.768,
            "Standard_D32ds_v4": 1.536,
            "Standard_D48ds_v4": 2.304,
            "Standard_D64ds_v4": 3.072,
            # D-series v5
            "Standard_D4ds_v5": 0.192,
            "Standard_D8ds_v5": 0.384,
            "Standard_D16ds_v5": 0.768,
            "Standard_D32ds_v5": 1.536,
            "Standard_D48ds_v5": 2.304,
            "Standard_D64ds_v5": 3.072,
            # E-series v5
            "Standard_E4ds_v5": 0.252,
            "Standard_E8ds_v5": 0.504,
            "Standard_E16ds_v5": 1.008,
            "Standard_E32ds_v5": 2.016,
            "Standard_E48ds_v5": 3.024,
            "Standard_E64ds_v5": 4.032,
            # L-series v3 (storage optimized)
            "Standard_L8s_v3": 0.832,
            "Standard_L16s_v3": 1.664,
            "Standard_L32s_v3": 3.328,
            "Standard_L48s_v3": 4.992,
            "Standard_L64s_v3": 6.656,
            # Roxie series
            "Standard_D32s_v5": 1.536,
            "Standard_D48s_v5": 2.304,
        }

        node_pools = self.cluster_data.get("agentPoolProfiles", [])
        total_hourly = 0
        total_nodes = 0
        pool_costs = []
        unknown_vm_sizes = set()

        for pool in node_pools:
            vm_size = pool.get("vmSize")
            count = pool.get("count", 0)
            hourly_rate = vm_pricing.get(vm_size, 0)

            if hourly_rate == 0 and count > 0 and vm_size:
                unknown_vm_sizes.add(vm_size)

            # Apply discounts to the hourly rate
            discounted_hourly_rate = self._apply_discounts(hourly_rate, vm_size)

            pool_hourly = discounted_hourly_rate * count
            total_hourly += pool_hourly
            total_nodes += count

            pool_costs.append({
                "pool_name": pool.get("name"),
                "vm_size": vm_size,
                "count": count,
                "max_count": pool.get("maxCount", count),
                "hourly_cost": round(pool_hourly, 2),
                "daily_cost": round(pool_hourly * 24, 2),
                "monthly_cost": round(pool_hourly * 24 * 30, 2),
                "has_pricing": hourly_rate > 0,
                "list_price_hourly": round(hourly_rate * count, 2),
                "discount_applied": round((hourly_rate * count - pool_hourly), 2) if hourly_rate > 0 else 0
            })

        # Sort by monthly cost descending
        pool_costs.sort(key=lambda x: x["monthly_cost"], reverse=True)

        # Calculate total savings from discounts
        total_list_hourly = sum(pool["list_price_hourly"] for pool in pool_costs)
        total_discount_amount = round(total_list_hourly - total_hourly, 2)

        result = {
            "estimated_hourly": round(total_hourly, 2),
            "estimated_daily": round(total_hourly * 24, 2),
            "estimated_monthly": round(total_hourly * 24 * 30, 2),
            "list_price_hourly": round(total_list_hourly, 2),
            "list_price_monthly": round(total_list_hourly * 24 * 30, 2),
            "discount_amount_hourly": total_discount_amount,
            "discount_amount_monthly": round(total_discount_amount * 24 * 30, 2),
            "total_nodes": total_nodes,
            "by_pool": pool_costs,
            "discount_info": {
                "enterprise_discount": self.enterprise_discount,
                "d_series_discount": self.d_series_discount,
                "total_effective_discount": self._calculate_effective_discount_rate(total_list_hourly, total_hourly)
            },
            "note": f"Estimates based on VM compute costs only (East US pricing). Discounts: Enterprise {self.enterprise_discount}% (all VMs), D-series {self.d_series_discount}% (D-series VMs use higher discount). Does not include storage, networking, load balancers, or other Azure services."
        }

        if unknown_vm_sizes:
            result["warning"] = f"Pricing not available for VM sizes: {', '.join(sorted(unknown_vm_sizes))}"

        return result

    def get_current_month_actual_costs(self) -> Dict[str, Any]:
        """Get actual costs for the current month from Azure Cost Management."""
        current_date = datetime.now()
        # Get first day of current month
        start_of_month = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        days_in_current_month = (current_date - start_of_month).days + 1

        print(f"Fetching actual costs for current month ({days_in_current_month} days)...")

        # Try multiple approaches to get actual cost data
        actual_costs = self.get_cost_analysis(days_back=days_in_current_month)

        # If consumption API fails, try fallback methods
        if "error" in actual_costs or actual_costs.get("total_cost", 0) == 0:
            print("Trying fallback cost retrieval methods...")
            actual_costs = self._try_cost_management_query()

        if "error" in actual_costs or actual_costs.get("total_cost", 0) == 0:
            return {
                "error": "Unable to retrieve actual cost data from Azure",
                "methods_tried": ["consumption_api", "consumption_usage_fallback"],
                "suggestion": "Ensure proper permissions for Cost Management Reader role. Cost Management extension may not be available in this Azure CLI version.",
                "note": "Will use estimated costs based on VM pricing instead of actual billing data"
            }

        # Calculate monthly projection from current month data
        daily_average = actual_costs.get("total_cost", 0) / max(days_in_current_month, 1)

        # Get days in current month for projection
        if current_date.month == 12:
            next_month = current_date.replace(year=current_date.year + 1, month=1, day=1)
        else:
            next_month = current_date.replace(month=current_date.month + 1, day=1)

        days_in_month = (next_month - start_of_month).days
        projected_monthly = daily_average * days_in_month

        return {
            "actual_month_to_date": actual_costs.get("total_cost", 0),
            "days_in_period": days_in_current_month,
            "daily_average": daily_average,
            "projected_monthly": projected_monthly,
            "days_in_month": days_in_month,
            "data_source": actual_costs.get("method", "azure_api"),
            "confidence": "high" if days_in_current_month >= 7 else "medium",
            "service_breakdown": actual_costs.get("service_breakdown", [])
        }

    def _try_cost_management_query(self) -> Dict[str, Any]:
        """Try alternative cost retrieval methods (fallback when costmanagement query command unavailable)."""
        # Use cached API availability test result
        if not self._test_cost_api_availability():
            return {
                "error": "Azure cost APIs not accessible in current environment",
                "note": "Will use estimated costs based on VM pricing instead",
                "methods_attempted": ["consumption_usage_cached"],
                "suggestion": "For actual cost data, ensure proper Azure permissions and Cost Management access"
            }

        # API is available, try more detailed queries
        try:
            # Method 1: Try az billing period list to get recent billing data
            print("Attempting to retrieve billing period information...")
            billing_periods = self.run_az_command([
                "az", "billing", "period", "list",
                "-o", "json"
            ], timeout=30)

            if billing_periods and isinstance(billing_periods, list) and len(billing_periods) > 0:
                print(f"Found {len(billing_periods)} billing periods, but detailed cost data requires subscription-level access")
                # Note: This gives us period info but not detailed costs without proper permissions

        except Exception as e:
            print(f"Billing period query failed: {e}", file=sys.stderr)

        # Since API is available, try full consumption usage query
        try:
            print("Attempting full consumption usage query...")
            current_date = datetime.now()
            start_date = current_date - timedelta(days=30)

            usage_result = self.run_az_command([
                "az", "consumption", "usage", "list",
                "--start-date", start_date.strftime("%Y-%m-%d"),
                "--end-date", current_date.strftime("%Y-%m-%d"),
                "-o", "json"
            ], timeout=30)

            if "error" not in usage_result and usage_result:
                print("Consumption API is accessible but may require more specific parameters")
                return {
                    "error": "Consumption API accessible but requires proper date range and permissions",
                    "suggestion": "Contact Azure administrator for Cost Management Reader permissions"
                }

        except Exception as e:
            print(f"Full consumption API query failed: {e}", file=sys.stderr)

        return {
            "error": "Azure cost APIs accessible but insufficient permissions",
            "note": "Will use estimated costs based on VM pricing instead",
            "methods_attempted": ["billing_periods", "consumption_usage_full"],
            "suggestion": "For actual cost data, ensure proper Azure permissions and Cost Management access"
        }

    def _calculate_historical_waste_trends(self, historical_costs: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate waste trends from historical cost and utilization data."""
        trends = {
            "idle_trend_days": 0,
            "utilization_trend_days": 0,
            "avg_daily_idle_waste": 0,
            "avg_cpu_utilization": 0,
            "idle_consistent": False,
            "utilization_consistent": False,
            "historical_waste_confidence": "low"
        }

        if not historical_costs or "error" in historical_costs:
            return trends

        # Analyze daily cost patterns
        daily_costs = historical_costs.get("daily_costs", [])
        if not daily_costs:
            return trends

        # Load historical idle tracking data
        history = self.load_idle_history()
        idle_nodes_history = history.get("nodes", {})

        if len(daily_costs) >= 7:  # At least a week of data
            trends["utilization_trend_days"] = len(daily_costs)

            # Calculate average waste from idle nodes
            total_idle_cost = 0
            idle_days_count = 0

            for node_name, node_history in idle_nodes_history.items():
                if node_history.get("is_currently_idle", False):
                    idle_duration_hours = node_history.get("total_idle_hours", 0)
                    if idle_duration_hours >= 24:  # Idle for at least a day
                        idle_days_count += 1
                        # Estimate daily waste (rough approximation)
                        node_daily_cost = 50  # Default estimate per node per day
                        total_idle_cost += node_daily_cost

            if idle_days_count > 0:
                trends["avg_daily_idle_waste"] = total_idle_cost / max(len(daily_costs), 1)
                trends["idle_trend_days"] = len(daily_costs)
                trends["idle_consistent"] = idle_days_count >= 3  # Multiple nodes consistently idle

            # Analyze cost variance to detect waste patterns
            if len(daily_costs) >= 3:
                daily_amounts = [day.get("cost", 0) for day in daily_costs[-7:]]  # Last 7 days
                if daily_amounts:
                    avg_cost = sum(daily_amounts) / len(daily_amounts)
                    cost_variance = statistics.variance(daily_amounts) if len(daily_amounts) > 1 else 0

                    # High variance might indicate inefficient scaling
                    if cost_variance > (avg_cost * 0.2):  # 20% variance threshold
                        trends["historical_waste_confidence"] = "medium"

                    if len(daily_costs) >= 14:  # Two weeks of data
                        trends["historical_waste_confidence"] = "high"

        return trends

    def _apply_discounts(self, hourly_rate: float, vm_size: str) -> float:
        """Apply enterprise and D-series discounts to VM hourly rate."""
        if hourly_rate == 0:
            return 0

        # For D-series VMs, use the higher of enterprise or D-series discount (not stacked)
        if vm_size and "D" in vm_size.upper() and self.d_series_discount > 0:
            # Use the higher discount rate for D-series VMs
            effective_discount = max(self.enterprise_discount, self.d_series_discount)
            return hourly_rate * (1 - effective_discount / 100)

        # For non-D-series VMs, use enterprise discount only
        if self.enterprise_discount > 0:
            return hourly_rate * (1 - self.enterprise_discount / 100)

        return hourly_rate

    def _calculate_effective_discount_rate(self, list_price: float, discounted_price: float) -> float:
        """Calculate the effective discount percentage."""
        if list_price == 0:
            return 0
        return round(((list_price - discounted_price) / list_price) * 100, 1)

    def get_cost_analysis(self, days_back: int = 30) -> Dict[str, Any]:
        """Get actual cost data from Azure Cost Management using consumption APIs."""
        print(f"Fetching cost analysis for last {days_back} days...")

        if not self.resource_group:
            return {"error": "Resource group not found"}

        # Test API availability using cached result
        if not self._test_cost_api_availability():
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            return self._get_cost_analysis_fallback(start_date, end_date)

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        try:
            # API is available, try to get usage details
            if self.enable_timeout_protection:
                print("Querying Azure consumption API (with timeout protection enabled)...")
                timeout_value = 30
                top_limit = "100"  # Reduced from 1000 to prevent timeouts
            else:
                print("Querying Azure consumption API (timeout protection disabled)...")
                timeout_value = 300  # 5 minutes for full data retrieval
                top_limit = "1000"  # Full data retrieval

            usage_result = self.run_az_command([
                "az", "consumption", "usage", "list",
                "--start-date", start_date.strftime("%Y-%m-%d"),
                "--end-date", end_date.strftime("%Y-%m-%d"),
                "--top", top_limit,
                "--include-additional-properties",
                "--include-meter-details",
                "-o", "json"
            ], timeout=timeout_value)

            # Check if the command timed out (only relevant when timeout protection is enabled)
            if "error" in usage_result and "timed out" in usage_result.get("error", ""):
                if self.enable_timeout_protection:
                    print("⏱️  Azure consumption API timed out (>30 seconds) - this is a known issue with the Azure CLI consumption command")
                    print("   Switching to estimated cost calculations...")
                    return self._get_cost_analysis_fallback(start_date, end_date)
                else:
                    print("⏱️  Azure consumption API timed out (>5 minutes) - API may be unavailable")
                    print("   Switching to estimated cost calculations...")
                    return self._get_cost_analysis_fallback(start_date, end_date)

            # Process usage data to get costs by resource group
            cost_analysis = {
                "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                "total_cost": 0,
                "daily_costs": [],
                "service_breakdown": [],
                "cost_trend": "stable",
                "method": "consumption_api"
            }

            if usage_result and isinstance(usage_result, list) and "error" not in usage_result:
                # Filter by resource group and aggregate costs
                rg_costs = defaultdict(float)
                service_costs = defaultdict(float)
                daily_costs = defaultdict(float)

                for usage in usage_result:
                    instance_id = usage.get("instanceId", "")
                    if self.resource_group.lower() in instance_id.lower():
                        cost = float(usage.get("pretaxCost", 0) or 0)
                        service_name = usage.get("meterCategory", "Unknown")
                        usage_date = usage.get("usageStart", "")[:10]  # Extract date part

                        cost_analysis["total_cost"] += cost
                        service_costs[service_name] += cost
                        if usage_date:
                            daily_costs[usage_date] += cost

                # Convert to expected format
                cost_analysis["service_breakdown"] = [
                    {"service": service, "cost": cost}
                    for service, cost in service_costs.items()
                ]

                cost_analysis["daily_costs"] = [
                    {"date": date, "cost": cost}
                    for date, cost in sorted(daily_costs.items())
                ]

                # Determine cost trend
                if len(daily_costs) >= 7:
                    dates = sorted(daily_costs.keys())
                    recent_avg = statistics.mean([daily_costs[d] for d in dates[-7:]])
                    older_avg = statistics.mean([daily_costs[d] for d in dates[:7]])

                    if recent_avg > older_avg * 1.1:
                        cost_analysis["cost_trend"] = "increasing"
                    elif recent_avg < older_avg * 0.9:
                        cost_analysis["cost_trend"] = "decreasing"

            # If no data found, try alternative approach
            if cost_analysis["total_cost"] == 0:
                cost_analysis = self._get_cost_analysis_fallback(start_date, end_date)

            return cost_analysis

        except Exception as e:
            print(f"Warning: Could not fetch cost analysis: {e}", file=sys.stderr)
            return self._get_cost_analysis_fallback(start_date, end_date)

    def _get_cost_analysis_fallback(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fallback method for cost analysis when consumption API fails."""
        # Error message already shown in _test_cost_api_availability(), no need to repeat
        return {
            "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "total_cost": 0,
            "daily_costs": [],
            "service_breakdown": [],
            "cost_trend": "unknown",
            "method": "fallback_estimated_only",
            "error": "Cost data unavailable - Azure consumption API timeout or access denied",
            "note": "Analysis will continue using estimated costs based on VM pricing"
        }

    def generate_cost_savings_summary(self) -> Dict[str, Any]:
        """Generate a comprehensive cost savings summary report."""
        print("Generating cost savings analysis...")

        # Get all required data
        costs = self.estimate_costs()
        utilization = self.get_node_utilization()
        efficiency = self.analyze_resource_efficiency(utilization)
        optimization_opportunities = self.calculate_cost_optimization_opportunities()
        cost_analysis = self.get_cost_analysis(30)

        # Get actual current month costs
        current_month_costs = self.get_current_month_actual_costs()

        # Determine which cost to use as primary
        if "error" not in current_month_costs and current_month_costs.get("confidence") in ["high", "medium"]:
            primary_monthly_cost = current_month_costs.get("projected_monthly", costs.get("estimated_monthly", 0))
            cost_source = "actual_projected"
            cost_confidence = current_month_costs.get("confidence", "medium")
        else:
            primary_monthly_cost = costs.get("estimated_monthly", 0)
            cost_source = "estimated"
            cost_confidence = "low"

        # Create summary
        summary = {
            "executive_summary": {
                "current_monthly_estimate": primary_monthly_cost,
                "cost_source": cost_source,
                "cost_confidence": cost_confidence,
                "actual_month_to_date": current_month_costs.get("actual_month_to_date", 0) if "error" not in current_month_costs else 0,
                "days_tracked": current_month_costs.get("days_in_period", 0) if "error" not in current_month_costs else 0,
                "total_nodes": costs.get("total_nodes", 0),
                "average_utilization": efficiency.get("summary", {}).get("efficiency_score", 0),
                "potential_monthly_savings": optimization_opportunities.get("total_potential_savings", 0),
                "cost_efficiency_grade": self._calculate_efficiency_grade(efficiency, costs)
            },
            "waste_identification": {
                "idle_nodes": 0 if efficiency.get("metrics_unreliable", False) else efficiency.get("summary", {}).get("idle_nodes", 0),
                "underutilized_nodes": 0 if efficiency.get("metrics_unreliable", False) else efficiency.get("summary", {}).get("underutilized_nodes", 0),
                "idle_cost_estimate": 0,
                "underutilized_cost_estimate": 0,
                "idle_duration_stats": {} if efficiency.get("metrics_unreliable", False) else efficiency.get("summary", {}).get("idle_duration_stats", {}),
                "metrics_unreliable": efficiency.get("metrics_unreliable", False)
            },
            "optimization_scenarios": optimization_opportunities.get("optimization_scenarios", []),
            "recommendations": optimization_opportunities.get("recommendations", []),
            "cost_trends": {
                "actual_monthly_cost": cost_analysis.get("total_cost", 0) if "error" not in cost_analysis else 0,
                "cost_trend": cost_analysis.get("cost_trend", "unknown") if "error" not in cost_analysis else "unknown",
                "estimate_vs_actual_variance": 0
            }
        }

        # Calculate waste costs
        if costs.get("total_nodes", 0) > 0:
            avg_node_cost = costs.get("estimated_monthly", 0) / costs.get("total_nodes", 1)
            summary["waste_identification"]["idle_cost_estimate"] = (
                efficiency.get("summary", {}).get("idle_nodes", 0) * avg_node_cost
            )
            summary["waste_identification"]["underutilized_cost_estimate"] = (
                efficiency.get("summary", {}).get("underutilized_nodes", 0) * avg_node_cost * 0.3
            )

        # Calculate estimate vs actual variance
        if cost_analysis.get("total_cost", 0) > 0:
            estimated_monthly = costs.get("estimated_monthly", 0)
            actual_monthly = cost_analysis.get("total_cost", 0)
            variance = ((estimated_monthly - actual_monthly) / actual_monthly) * 100
            summary["cost_trends"]["estimate_vs_actual_variance"] = round(variance, 1)

        return summary

    def _calculate_efficiency_grade(self, efficiency: Dict[str, Any], costs: Dict[str, Any]) -> str:
        """Calculate an efficiency grade (A-F) based on utilization and cost factors."""
        efficiency_score = efficiency.get("summary", {}).get("efficiency_score", 0)
        idle_percentage = 0

        total_nodes = efficiency.get("summary", {}).get("total_nodes", 0)
        if total_nodes > 0:
            idle_nodes = efficiency.get("summary", {}).get("idle_nodes", 0)
            idle_percentage = (idle_nodes / total_nodes) * 100

        # Grade based on multiple factors
        if efficiency_score >= 70 and idle_percentage <= 10:
            return "A"
        elif efficiency_score >= 50 and idle_percentage <= 20:
            return "B"
        elif efficiency_score >= 30 and idle_percentage <= 30:
            return "C"
        elif efficiency_score >= 20 and idle_percentage <= 50:
            return "D"
        else:
            return "F"

    def _calculate_thor_worker_costs(self) -> Dict[str, Any]:
        """Calculate costs specifically for nodes hosting Thor worker pods."""
        try:
            # Get nodes and their efficiency data
            utilization = self.get_node_utilization()
            efficiency = self.analyze_resource_efficiency(utilization)
            
            if "error" in efficiency:
                return {"error": "Unable to analyze node utilization", "thor_worker_nodes": 0, "total_monthly_cost": 0}
            
            return self._calculate_thor_worker_costs_with_data(efficiency)
            
        except Exception as e:
            print(f"Warning: Could not calculate Thor worker costs: {e}", file=sys.stderr)
            return {"error": str(e), "thor_worker_nodes": 0, "total_monthly_cost": 0}

    def _calculate_thor_worker_costs_with_data(self, efficiency: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate costs specifically for nodes hosting Thor worker pods using pre-computed efficiency data."""
        try:
            # Get pods by node to identify which nodes host Thor worker pods
            pods_by_node = self.get_pods_by_node()
            
            # If pods_by_node is empty, try a direct kubectl call
            if not pods_by_node:
                try:
                    # Try direct kubectl call to get thorworker pods with node info from ALL namespaces
                    cmd = ["kubectl", "get", "pods", "--all-namespaces", "-o", 
                           "jsonpath={range .items[*]}{.metadata.namespace}{\"\t\"}{.metadata.name}{\"\t\"}{.spec.nodeName}{\"\n\"}{end}",
                           "--field-selector=status.phase=Running"]
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                    
                    if result.returncode == 0 and result.stdout.strip():
                        # Parse the output and build pods_by_node for thorworker pods
                        pods_by_node = {}
                        for line in result.stdout.strip().split('\n'):
                            parts = line.split('\t')
                            if len(parts) >= 3 and 'thorworker-job' in parts[1]:
                                namespace, pod_name, node_name = parts[0], parts[1], parts[2]
                                if node_name not in pods_by_node:
                                    pods_by_node[node_name] = []
                                pods_by_node[node_name].append({
                                    "pod_name": pod_name,
                                    "namespace": namespace,
                                    "containers": ["thorworker"]  # Simplified
                                })
                except Exception as e:
                    print(f"   Warning: Direct kubectl query failed: {e}")
                    
            # Get cost information by pool
            costs = self.estimate_costs()
            pool_costs = costs.get("by_pool", [])
            pool_cost_lookup = {pool.get("pool_name"): pool for pool in pool_costs}
            
            # Get all nodes from efficiency analysis AND get all cluster nodes to ensure we don't miss any
            all_nodes = (efficiency.get("idle_nodes", []) + 
                        efficiency.get("underutilized_nodes", []) +
                        efficiency.get("efficient_nodes", []))
            
            # If we don't have many nodes from efficiency analysis, try to get all nodes
            if len(all_nodes) < 50:  # Assuming cluster should have more nodes
                try:
                    # Get basic node info directly from kubectl
                    cmd = ["kubectl", "get", "nodes", "-o", "json"]
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                    if result.returncode == 0:
                        import json
                        nodes_data = json.loads(result.stdout)
                        for node_item in nodes_data.get("items", []):
                            node_name = node_item.get("metadata", {}).get("name", "")
                            if node_name and not any(n.get("name") == node_name for n in all_nodes):
                                # Add basic node info for nodes not in efficiency analysis
                                all_nodes.append({
                                    "name": node_name,
                                    "node_pool": "unknown",
                                    "cpu_percent": "0%",
                                    "memory_percent": "0%"
                                })
                except Exception as e:
                    pass  # If this fails, continue with what we have
            
            nodes_hosting_thor_workers = []
            total_thor_hosting_cost = 0.0
            
            # Analyze each node to see if it hosts Thor worker pods
            for node in all_nodes:
                node_name = node.get("name", "")
                node_pool = node.get("node_pool", "")
                
                # If node_pool is unknown or empty, try to derive it from node name
                if not node_pool or node_pool == "unknown":
                    # Extract pool name from AKS node naming pattern: aks-{poolname}-{vmss}-{instance}
                    if node_name.startswith("aks-") and "-" in node_name:
                        parts = node_name.split("-")
                        if len(parts) >= 3:
                            node_pool = parts[1]  # Second part is typically the pool name
                
                # Get pods running on this node
                node_pods = pods_by_node.get(node_name, [])
                
                has_thor_worker_pods = False
                thor_worker_pods = []
                
                # Check if node hosts Thor worker pods
                for pod_info in node_pods:
                    pod_name = pod_info.get("pod_name", "")
                    namespace = pod_info.get("namespace", "")
                    
                    # Check if this pod is a Thor worker pod (in any namespace)
                    if "thorworker-job" in pod_name.lower():
                        has_thor_worker_pods = True
                        thor_worker_pods.append(f"{namespace}:{pod_name}")
                
                # Include nodes that host Thor worker pods
                if has_thor_worker_pods:
                    # Get node cost from pool information
                    pool_cost_info = pool_cost_lookup.get(node_pool, {})
                    node_monthly_cost = 0.0
                    
                    if pool_cost_info and pool_cost_info.get("count", 0) > 0:
                        # Calculate per-node cost (total pool cost / number of nodes in pool)
                        pool_monthly_cost = pool_cost_info.get("monthly_cost", 0)
                        pool_node_count = pool_cost_info.get("count", 1)
                        node_monthly_cost = pool_monthly_cost / pool_node_count
                    
                    nodes_hosting_thor_workers.append({
                        "node_name": node_name,
                        "node_pool": node_pool,
                        "vm_size": pool_cost_info.get("vm_size", "Unknown"),
                        "monthly_cost": node_monthly_cost,
                        "cpu_percent": node.get("cpu_percent", "0%"),
                        "memory_percent": node.get("memory_percent", "0%"),
                        "thor_worker_pods": thor_worker_pods,
                        "total_pods": len(node_pods)
                    })
                    
                    total_thor_hosting_cost += node_monthly_cost
            
            # Debug: Show what we found
            thor_hosting_nodes = [node_name for node_name in pods_by_node.keys() 
                                if any("thorworker-job" in pod["pod_name"].lower() 
                                      for pod in pods_by_node[node_name])]
            available_pools = list(pool_cost_lookup.keys())
            
            debug_info = {
                "thor_pod_nodes_found": len(thor_hosting_nodes),
                "sample_thor_nodes": thor_hosting_nodes[:3],
                "available_cost_pools": len(available_pools),
                "sample_cost_pools": available_pools[:3],
                "nodes_analyzed": len(all_nodes),
                "pods_by_node_keys": len(pods_by_node)
            }
            
            return {
                "thor_worker_nodes": len(nodes_hosting_thor_workers),
                "total_monthly_cost": total_thor_hosting_cost,
                "node_details": nodes_hosting_thor_workers,
                "average_cost_per_node": total_thor_hosting_cost / len(nodes_hosting_thor_workers) if nodes_hosting_thor_workers else 0,
                "debug_info": debug_info,
                "analysis_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"Warning: Could not calculate Thor worker hosting costs: {e}", file=sys.stderr)
            return {"error": str(e), "thor_worker_nodes": 0, "total_monthly_cost": 0}

    def get_cluster_health(self) -> Dict[str, Any]:
        """Get cluster health status."""
        print("Checking cluster health...")

        health = {
            "provisioning_state": self.cluster_data.get("provisioningState"),
            "power_state": self.cluster_data.get("powerState", {}).get("code"),
            "kubernetes_version": self.cluster_data.get("kubernetesVersion"),
            "api_server_accessible": True
        }

        try:
            # Test API server connectivity
            subprocess.run(
                ["kubectl", "cluster-info"],
                capture_output=True,
                check=True,
                timeout=10
            )
        except:
            health["api_server_accessible"] = False

        return health

    def _create_ecl_watch_request(self, url: str) -> urllib.request.Request:
        """Create an HTTP request for ECL Watch with proper authentication."""
        req = urllib.request.Request(url)
        req.add_header('Accept', 'application/json')
        req.add_header('User-Agent', 'hpcc-aks-cluster-insights/1.0')

        # Add basic authentication if credentials are available
        if self.ecl_watch_username and self.ecl_watch_password:
            credentials = f"{self.ecl_watch_username}:{self.ecl_watch_password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            req.add_header('Authorization', f'Basic {encoded_credentials}')

        return req

    def _resolve_ecl_watch_credentials(self) -> bool:
        """Resolve ECL Watch credentials from various sources with automatic interactive prompts."""
        if self._credentials_resolved:
            return self.ecl_watch_username and self.ecl_watch_password

        # Precedence: 1. Environment variables (most secure)
        self.ecl_watch_username = os.getenv('ECLWATCH_USERNAME')
        self.ecl_watch_password = os.getenv('ECLWATCH_PASSWORD')

        # Precedence: 2. Command line parameters (less secure, but provided)
        if not self.ecl_watch_username:
            self.ecl_watch_username = self._ecl_watch_username_param
        if not self.ecl_watch_password:
            self.ecl_watch_password = self._ecl_watch_password_param

        # Precedence: 3. Interactive prompts (automatic when URL provided and credentials incomplete)
        if self.ecl_watch_url and self.ecl_watch_username and not self.ecl_watch_password:
            try:
                print(f"ECL Watch authentication required for: {self.ecl_watch_url}")
                self.ecl_watch_password = getpass.getpass(f"ECL Watch password for '{self.ecl_watch_username}': ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\nECL Watch authentication cancelled by user.")
                return False

        self._credentials_resolved = True
        return bool(self.ecl_watch_username and self.ecl_watch_password)

    def get_ecl_watch_job_analysis(self) -> Dict[str, Any]:
        """Query ECL Watch for running jobs and recent job history to provide workload context (cached)."""

        # Use unified ECL Watch data to avoid duplicate API calls
        unified_data = self._get_unified_ecl_watch_data()

        if "error" in unified_data:
            return {
                "error": unified_data["error"],
                "note": unified_data.get("note", "ECL Watch query failed")
            }

        # Extract workload analysis from unified data
        workload_data = {
            "ecl_watch_url": unified_data.get("ecl_watch_url"),
            "timestamp": unified_data.get("timestamp"),
            "running_jobs": unified_data.get("running_jobs", []),
            "recent_jobs": unified_data.get("recent_jobs", []),
            "queue_analysis": {},
            "workload_assessment": unified_data.get("workload_assessment", {}),
            "error": None
        }

        # Generate queue analysis from the job data
        queue_analysis = self._analyze_job_queue_activity(
            workload_data["running_jobs"],
            workload_data["recent_jobs"]
        )
        workload_data["queue_analysis"] = queue_analysis

        return workload_data

    def _query_ecl_watch_running_jobs(self) -> Dict[str, Any]:
        """Query ECL Watch for currently active jobs (non-completed states)."""
        try:
            # ECL Watch API endpoint for workunit information - use correct URL structure
            url = f"{self.ecl_watch_url.rstrip('/')}/WsWorkunits/WUQuery.json"
            params = {
                'PageSize': '500',  # Increased to examine up to 500 jobs
                'Sortby': 'Wuid',
                'Descending': 'true'
            }

            query_string = urllib.parse.urlencode(params)
            full_url = f"{url}?{query_string}"

            req = self._create_ecl_watch_request(full_url)

            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())

                jobs = []
                workunits = data.get('WUQueryResponse', {}).get('Workunits', {}).get('ECLWorkunit', [])

                # Handle both single workunit and array of workunits
                if isinstance(workunits, dict):
                    workunits = [workunits]

                for wu in workunits:
                    state = wu.get('State', '').lower()

                    # Filter out completed jobs - only include active states
                    if state != 'completed':
                        job_info = {
                            'wuid': wu.get('Wuid', ''),
                            'state': wu.get('State', ''),
                            'jobname': wu.get('Jobname', ''),
                            'cluster': wu.get('Cluster', ''),
                            'queue': wu.get('Queue', ''),
                            'owner': wu.get('Owner', ''),
                            'protected': wu.get('Protected', False),
                            'total_cluster_time': wu.get('TotalClusterTime', ''),
                            'description': wu.get('Description', '')
                        }
                        jobs.append(job_info)



                return {
                    "jobs": jobs,
                    "total_count": len(jobs),
                    "query_time": datetime.now().isoformat()
                }

        except urllib.error.HTTPError as e:
            if e.code in [401, 403]:
                return {"error": f"Authentication failed (HTTP {e.code}): Check ECL Watch username and password"}
            else:
                return {"error": f"HTTP error {e.code} querying ECL Watch running jobs: {e}"}
        except urllib.error.URLError as e:
            return {"error": f"Network error querying ECL Watch: {e}"}
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON response from ECL Watch: {e}"}
        except Exception as e:
            return {"error": f"Unexpected error querying running jobs: {e}"}

    def _query_ecl_watch_recent_jobs(self, hours_back: int = 2) -> Dict[str, Any]:
        """Query ECL Watch for recent active jobs (non-completed states)."""
        try:
            # Calculate time range for recent jobs
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours_back)

            url = f"{self.ecl_watch_url.rstrip('/')}/WsWorkunits/WUQuery.json"
            params = {
                'PageSize': '500',  # Increased to examine up to 500 jobs
                'Sortby': 'Wuid',
                'Descending': 'true',
                'StartDate': start_time.strftime('%Y-%m-%dT%H:%M:%S'),
                'EndDate': end_time.strftime('%Y-%m-%dT%H:%M:%S')
            }

            query_string = urllib.parse.urlencode(params)
            full_url = f"{url}?{query_string}"

            req = self._create_ecl_watch_request(full_url)

            with urllib.request.urlopen(req, timeout=15) as response:
                data = json.loads(response.read().decode())

                jobs = []
                workunits = data.get('WUQueryResponse', {}).get('Workunits', {}).get('ECLWorkunit', [])

                # Handle both single workunit and array of workunits
                if isinstance(workunits, dict):
                    workunits = [workunits]

                for wu in workunits:
                    state = wu.get('State', '').lower()

                    # Include all jobs (active and completed) in recent timeframe for activity analysis
                    job_info = {
                        'wuid': wu.get('Wuid', ''),
                        'state': wu.get('State', ''),
                        'jobname': wu.get('Jobname', ''),
                        'cluster': wu.get('Cluster', ''),
                        'queue': wu.get('Queue', ''),
                        'owner': wu.get('Owner', ''),
                        'total_cluster_time': wu.get('TotalClusterTime', ''),
                        'state_id': wu.get('StateID', 0),
                        'completion_time': wu.get('StateDesc', ''),  # Contains completion info
                        'description': wu.get('Description', '')
                    }
                    jobs.append(job_info)



                return {
                    "jobs": jobs,
                    "total_count": len(jobs),
                    "time_range": f"{start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}",
                    "hours_analyzed": hours_back
                }

        except urllib.error.HTTPError as e:
            if e.code in [401, 403]:
                return {"error": f"Authentication failed (HTTP {e.code}): Check ECL Watch username and password"}
            else:
                return {"error": f"HTTP error {e.code} querying ECL Watch recent jobs: {e}"}
        except urllib.error.URLError as e:
            return {"error": f"Network error querying ECL Watch recent jobs: {e}"}
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON response from ECL Watch recent jobs: {e}"}
        except Exception as e:
            return {"error": f"Unexpected error querying recent jobs: {e}"}

    def _analyze_job_queue_activity(self, running_jobs: List[Dict], recent_jobs: List[Dict]) -> Dict[str, Any]:
        """Analyze job activity by queue/cluster to determine workload patterns."""
        queue_stats = defaultdict(lambda: {
            'running_jobs': 0,
            'recent_jobs': 0,
            'total_jobs': 0,
            'clusters': set(),
            'owners': set(),
            'activity_level': 'idle'
        })

        # Analyze running jobs by queue
        for job in running_jobs:
            # Only count jobs that are actually in active states (not completed)
            state = job.get('state', '').lower()
            if state != 'completed':  # Include all non-completed states as running/active
                queue = job.get('queue', job.get('cluster', 'unknown'))  # Fallback to cluster if queue is missing
                if not queue or queue.strip() == '':
                    queue = job.get('cluster', 'unknown')
                cluster = job.get('cluster', 'unknown')
                owner = job.get('owner', 'unknown')

                queue_stats[queue]['running_jobs'] += 1
                queue_stats[queue]['clusters'].add(cluster)
                queue_stats[queue]['owners'].add(owner)

        # Analyze recent jobs by queue
        for job in recent_jobs:
            # Only count completed jobs for recent activity (running jobs are already counted above)
            state = job.get('state', '').lower()
            if state in ['completed', 'failed', 'aborted']:
                queue = job.get('queue', job.get('cluster', 'unknown'))  # Fallback to cluster if queue is missing
                if not queue or queue.strip() == '':
                    queue = job.get('cluster', 'unknown')
                cluster = job.get('cluster', 'unknown')
                owner = job.get('owner', 'unknown')

                queue_stats[queue]['recent_jobs'] += 1
                queue_stats[queue]['clusters'].add(cluster)
                queue_stats[queue]['owners'].add(owner)

        # Calculate totals and activity levels
        for queue, stats in queue_stats.items():
            stats['total_jobs'] = stats['running_jobs'] + stats['recent_jobs']
            stats['clusters'] = list(stats['clusters'])
            stats['owners'] = list(stats['owners'])

            # Determine activity level
            if stats['running_jobs'] > 0:
                if stats['running_jobs'] >= 5:
                    stats['activity_level'] = 'high'
                elif stats['running_jobs'] >= 2:
                    stats['activity_level'] = 'medium'
                else:
                    stats['activity_level'] = 'low'
            elif stats['recent_jobs'] > 0:
                if stats['recent_jobs'] >= 10:
                    stats['activity_level'] = 'recently_active'
                elif stats['recent_jobs'] >= 3:
                    stats['activity_level'] = 'recently_low'
                else:
                    stats['activity_level'] = 'recently_minimal'
            else:
                stats['activity_level'] = 'idle'

        return dict(queue_stats)

    def _assess_workload_context(self, queue_analysis: Dict[str, Any], total_running_jobs: int = None, total_recent_jobs: int = None, total_pending_jobs: int = None) -> Dict[str, Any]:
        """Assess whether idle/underutilized nodes are justified based on workload activity."""
        assessment = {
            "idle_justified": False,
            "underutilization_justified": False,
            "active_queues": [],
            "idle_queues": [],
            "workload_summary": "",
            "recommendations": []
        }

        # Use provided totals if available, otherwise calculate from queue analysis
        if total_running_jobs is not None and total_recent_jobs is not None:
            total_running = total_running_jobs
            total_recent = total_recent_jobs
            total_pending = total_pending_jobs if total_pending_jobs is not None else 0
        else:
            # Fallback to queue analysis only when explicit values aren't provided
            total_running = sum(stats['running_jobs'] for stats in queue_analysis.values())
            total_recent = sum(stats['recent_jobs'] for stats in queue_analysis.values())
            # Calculate total pending jobs from queue analysis
            total_pending = 0
            for stats in queue_analysis.values():
                total_pending += stats.get('queued_jobs', 0)
                total_pending += stats.get('submitted_jobs', 0)
                total_pending += stats.get('compiling_jobs', 0)
                total_pending += stats.get('compiled_jobs', 0)

        active_queues = []
        idle_queues = []

        for queue, stats in queue_analysis.items():
            if stats['activity_level'] in ['high', 'medium', 'low']:
                active_queues.append({
                    'queue': queue,
                    'running_jobs': stats['running_jobs'],
                    'recent_jobs': stats['recent_jobs'],
                    'activity_level': stats['activity_level'],
                    'clusters': stats['clusters']
                })
            else:
                idle_queues.append({
                    'queue': queue,
                    'recent_jobs': stats['recent_jobs'],
                    'activity_level': stats['activity_level'],
                    'clusters': stats['clusters']
                })

        assessment["active_queues"] = active_queues
        assessment["idle_queues"] = idle_queues

        # Determine if idle/underutilization is justified
        if total_running == 0:
            if total_pending > 10:
                # High number of pending jobs - idle nodes are justified for quick job startup
                assessment["idle_justified"] = True
                assessment["underutilization_justified"] = True
                assessment["workload_summary"] = f"High pending workload ({total_pending} queued/preparing jobs) - idle nodes justified for rapid job processing"
                assessment["recommendations"].append(f"Significant job backlog ({total_pending} jobs) - maintain idle capacity for efficient job processing")
            elif total_pending > 3:
                # Moderate pending jobs - some idle nodes justified
                assessment["idle_justified"] = True
                assessment["underutilization_justified"] = True
                assessment["workload_summary"] = f"Moderate pending workload ({total_pending} queued/preparing jobs) - some idle capacity justified"
                assessment["recommendations"].append(f"Moderate job queue ({total_pending} jobs) - maintain some idle nodes for job startup efficiency")
            elif total_recent == 0:
                assessment["idle_justified"] = True
                assessment["workload_summary"] = "No jobs active, pending, or completed recently - cluster idle state expected"
                assessment["recommendations"].append("Consider scaling down idle node pools during extended no-job periods")
            elif total_recent < 5:
                assessment["idle_justified"] = True
                assessment["underutilization_justified"] = True
                assessment["workload_summary"] = f"Minimal recent activity ({total_recent} jobs) - low utilization expected"
                assessment["recommendations"].append("Light workload detected - some idle nodes acceptable for rapid job startup")
            else:
                assessment["underutilization_justified"] = True
                assessment["workload_summary"] = f"Recent activity detected ({total_recent} jobs) but no current active jobs - temporary idle period"
                assessment["recommendations"].append("Recent job activity suggests workload variability - maintain some capacity for job spikes")
        else:
            # There are running jobs - assess based on total workload including pending
            if total_pending >= 10:
                # High number of pending jobs - idle nodes are definitely justified
                assessment["idle_justified"] = True
                assessment["underutilization_justified"] = True
                assessment["workload_summary"] = f"High job activity with significant queue ({total_running} active, {total_pending} pending, {total_recent} recent) - idle nodes justified for queue processing"
                assessment["recommendations"].append(f"Significant job backlog ({total_pending} pending jobs) - idle nodes needed for efficient queue processing")
            elif total_pending >= 5:
                # Moderate pending jobs - idle nodes are justified
                assessment["idle_justified"] = True
                assessment["underutilization_justified"] = True
                assessment["workload_summary"] = f"Moderate job activity with pending queue ({total_running} active, {total_pending} pending, {total_recent} recent) - idle capacity justified for job processing"
                assessment["recommendations"].append(f"Job queue building up ({total_pending} pending) - maintain idle capacity for queue processing")
            elif total_running >= 10:
                # High running jobs but low pending - investigate utilization
                assessment["workload_summary"] = f"High job activity ({total_running} active, {total_pending} pending, {total_recent} recent) - utilization should be high"
                assessment["recommendations"].append("High job activity but low queue - investigate why nodes appear underutilized")
            elif total_running >= 3:
                # Moderate running jobs, low pending
                assessment["underutilization_justified"] = True
                assessment["workload_summary"] = f"Moderate job activity ({total_running} active, {total_pending} pending, {total_recent} recent) - some underutilization acceptable"
                assessment["recommendations"].append("Moderate workload - optimize node pool sizes to match typical demand")
            else:
                # Low running jobs, low pending
                assessment["idle_justified"] = True
                assessment["underutilization_justified"] = True
                assessment["workload_summary"] = f"Low job activity ({total_running} active, {total_pending} pending, {total_recent} recent) - underutilization expected"
                if total_pending > 0:
                    assessment["recommendations"].append(f"Low activity but {total_pending} jobs queued - maintain minimal capacity for queue processing")
                else:
                    assessment["recommendations"].append("Low job activity - consider scaling down excess capacity")

        return assessment

    def _get_unified_ecl_watch_data(self) -> Dict[str, Any]:
        """Single unified ECL Watch query function to avoid multiple calls."""
        # Return cached results if already queried
        if self._ecl_watch_query_attempted:
            cached_result = self._ecl_watch_job_analysis or {
                "error": "ECL Watch query failed",
                "note": "ECL Watch was queried but failed - see previous error messages"
            }
            return cached_result

        # Mark that we've attempted the query to prevent duplicate calls
        self._ecl_watch_query_attempted = True

        if not self.ecl_watch_url:
            self._ecl_watch_job_analysis = {
                "error": "ECL Watch URL not provided",
                "note": "Use --ecl-watch-url parameter to enable job activity analysis"
            }
            return self._ecl_watch_job_analysis

        # Only require username for cluster statistics, allow basic analysis without it
        if self._ecl_watch_username_param and not self._resolve_ecl_watch_credentials():
            credential_methods = [
                "1. Set ECLWATCH_USERNAME and ECLWATCH_PASSWORD environment variables (recommended)",
                "2. Use --ecl-watch-username parameter (password will be prompted interactively)",
                "3. Use --ecl-watch-username and --ecl-watch-password parameters (less secure)"
            ]
            self._ecl_watch_job_analysis = {
                "error": "ECL Watch credentials not provided or incomplete",
                "note": "Provide credentials using one of these methods:\n" + "\n".join(credential_methods)
            }
            return self._ecl_watch_job_analysis

        print(f"Querying ECL Watch at {self.ecl_watch_url} for job activity...")

        unified_data = {
            "ecl_watch_url": self.ecl_watch_url,
            "timestamp": datetime.now().isoformat(),
            "running_jobs": [],
            "recent_jobs": [],
            "workload_assessment": {
                "total_running_jobs": 0,
                "total_recent_jobs": 0,
                "active_queues": [],
                "idle_justified": False,
                "underutilization_justified": False
            },
            "cluster_statistics": {
                "clusters": {},
                "summary": {},
                "success": False
            }
        }

        try:
            # Query active jobs (up to 500 jobs, excluding completed jobs)
            running_result = self._query_ecl_watch_running_jobs()
            recent_result = self._query_ecl_watch_recent_jobs(hours_back=24)  # 24 hours of active jobs only

            if "error" in recent_result:
                unified_data["error"] = f"Failed to query recent jobs: {recent_result['error']}"
                self._ecl_watch_job_analysis = unified_data
                return unified_data

            # Store job data
            unified_data["running_jobs"] = running_result.get("jobs", [])
            unified_data["recent_jobs"] = recent_result.get("jobs", [])

            # Generate workload assessment
            # Use cluster statistics to get accurate active job counts
            running_jobs_list = running_result.get("jobs", [])
            recent_jobs_list = recent_result.get("jobs", [])
                
            # Calculate active jobs using the same method as cluster statistics
            # This ensures consistency with the displayed "Total Active Jobs" count
            total_active_jobs = 0
            total_pending_jobs = 0
            if self._ecl_watch_username_param and self.ecl_watch_username and self.ecl_watch_password:
                # Generate cluster statistics to get accurate counts
                cluster_stats = self._generate_cluster_statistics_from_jobs(
                    running_jobs_list,
                    recent_jobs_list,
                    "error" in running_result
                )
                if cluster_stats.get("success", False):
                    cluster_summary = cluster_stats.get("summary", {})
                    total_running = cluster_summary.get('total_running_jobs', 0)
                    total_queued = cluster_summary.get('total_queued_jobs', 0)
                    total_submitted = cluster_summary.get('total_submitted_jobs', 0)
                    total_compiling = cluster_summary.get('total_compiling_jobs', 0)
                    total_compiled = cluster_summary.get('total_compiled_jobs', 0)
                    total_active_jobs = total_running + total_queued + total_submitted + total_compiling + total_compiled
                    total_pending_jobs = total_queued + total_submitted + total_compiling + total_compiled
                else:
                    # Fallback to raw count if cluster stats fail
                    total_active_jobs = len(running_jobs_list)
                    total_pending_jobs = 0
            else:
                # Fallback to raw count if no credentials
                total_active_jobs = len(running_jobs_list)
                total_pending_jobs = 0
            
            unified_data["workload_assessment"]["total_running_jobs"] = total_active_jobs
            unified_data["workload_assessment"]["total_recent_jobs"] = len(recent_jobs_list)

            # Analyze queue activity for workload context
            queue_analysis = self._analyze_job_queue_activity(
                unified_data["running_jobs"],
                unified_data["recent_jobs"]
            )

            # Determine workload assessment
            workload_assessment = self._assess_workload_context(
                queue_analysis, 
                unified_data["workload_assessment"]["total_running_jobs"],
                unified_data["workload_assessment"]["total_recent_jobs"],
                total_pending_jobs
            )
            unified_data["workload_assessment"].update(workload_assessment)

            # Generate cluster statistics if we have credential access
            if self._ecl_watch_username_param and self.ecl_watch_username and self.ecl_watch_password:
                cluster_stats = self._generate_cluster_statistics_from_jobs(
                    unified_data["running_jobs"],
                    unified_data["recent_jobs"],
                    "error" in running_result
                )
                unified_data["cluster_statistics"] = cluster_stats

        except Exception as e:
            unified_data["error"] = f"Failed to query ECL Watch: {str(e)}"
            print(f"Warning: ECL Watch query failed: {e}", file=sys.stderr)

        # Cache the results for future calls
        self._ecl_watch_job_analysis = unified_data
        return unified_data

    def _generate_cluster_statistics_from_jobs(self, running_jobs: List[Dict], recent_jobs: List[Dict], running_jobs_failed: bool) -> Dict[str, Any]:
        """Generate cluster statistics from job data."""
        cluster_stats = defaultdict(lambda: {
            'cluster_name': '',
            'running_jobs': 0,
            'submitted_jobs': 0,
            'compiling_jobs': 0,
            'compiled_jobs': 0,
            'queued_jobs': 0,  # Jobs in waiting/blocked states
            'completed_jobs_24h': 0,
            'failed_jobs_24h': 0,
            'aborted_jobs_24h': 0,
            'last_job_time': None,
            'last_job_wuid': '',
            'queues': set(),
            'owners': set(),
            'job_states': defaultdict(int)
        })

        # Process running jobs
        for job in running_jobs:
            cluster = job.get('cluster', 'unknown')
            state = job.get('state', '').lower()

            if cluster:
                cluster_stats[cluster]['cluster_name'] = cluster

                if state == 'running':
                    cluster_stats[cluster]['running_jobs'] += 1
                elif state == 'submitted':
                    cluster_stats[cluster]['submitted_jobs'] += 1
                elif state == 'compiling':
                    cluster_stats[cluster]['compiling_jobs'] += 1
                elif state == 'compiled':
                    cluster_stats[cluster]['compiled_jobs'] += 1
                elif state in ['blocked', 'wait', 'queued']:
                    cluster_stats[cluster]['queued_jobs'] += 1

                cluster_stats[cluster]['job_states'][state] += 1
                cluster_stats[cluster]['queues'].add(job.get('queue', ''))
                cluster_stats[cluster]['owners'].add(job.get('owner', ''))

        # Process recent jobs to find last execution time and extract running jobs if needed
        for job in recent_jobs:
            cluster = job.get('cluster', 'unknown')
            state = job.get('state', '').lower()
            wuid = job.get('wuid', '')

            if cluster:
                cluster_stats[cluster]['cluster_name'] = cluster

                # If running jobs query failed, count running jobs from recent data
                if running_jobs_failed:
                    if state == 'running':
                        cluster_stats[cluster]['running_jobs'] += 1
                    elif state == 'submitted':
                        cluster_stats[cluster]['submitted_jobs'] += 1
                    elif state == 'compiling':
                        cluster_stats[cluster]['compiling_jobs'] += 1
                    elif state == 'compiled':
                        cluster_stats[cluster]['compiled_jobs'] += 1
                    elif state in ['blocked', 'wait', 'queued']:
                        cluster_stats[cluster]['queued_jobs'] += 1

                # Note: completed jobs are now filtered out at API level
                if state == 'failed':
                    cluster_stats[cluster]['failed_jobs_24h'] += 1
                elif state == 'aborted':
                    cluster_stats[cluster]['aborted_jobs_24h'] += 1

                cluster_stats[cluster]['job_states'][state] += 1
                cluster_stats[cluster]['queues'].add(job.get('queue', ''))
                cluster_stats[cluster]['owners'].add(job.get('owner', ''))

                # Track most recent job (WUIDs are typically time-ordered)
                if (not cluster_stats[cluster]['last_job_time'] or
                    wuid > cluster_stats[cluster]['last_job_wuid']):
                    cluster_stats[cluster]['last_job_wuid'] = wuid
                    cluster_stats[cluster]['last_job_time'] = wuid  # WUID contains timestamp info

        # Convert sets to lists for JSON serialization
        final_stats = {}
        for cluster, stats in cluster_stats.items():
            final_stats[cluster] = {
                'cluster_name': stats['cluster_name'],
                'running_jobs': stats['running_jobs'],
                'submitted_jobs': stats['submitted_jobs'],
                'compiling_jobs': stats['compiling_jobs'],
                'compiled_jobs': stats['compiled_jobs'],
                'queued_jobs': stats['queued_jobs'],
                'completed_jobs_24h': stats['completed_jobs_24h'],  # Will be 0 due to filtering
                'failed_jobs_24h': stats['failed_jobs_24h'],
                'aborted_jobs_24h': stats['aborted_jobs_24h'],
                'total_failed_24h': stats['failed_jobs_24h'] + stats['aborted_jobs_24h'],
                'last_job_wuid': stats['last_job_wuid'],
                'queues': sorted(list(stats['queues'])) if stats['queues'] else [],
                'owners': sorted(list(stats['owners'])) if stats['owners'] else [],
                'job_states': dict(stats['job_states']),
                'activity_status': self._determine_cluster_activity_status(stats)
            }

        summary = {
            'total_clusters': len(final_stats),
            'total_running_jobs': sum(s['running_jobs'] for s in final_stats.values()),
            'total_submitted_jobs': sum(s['submitted_jobs'] for s in final_stats.values()),
            'total_compiling_jobs': sum(s['compiling_jobs'] for s in final_stats.values()),
            'total_compiled_jobs': sum(s['compiled_jobs'] for s in final_stats.values()),
            'total_queued_jobs': sum(s['queued_jobs'] for s in final_stats.values()),
            'total_completed_24h': sum(s['completed_jobs_24h'] for s in final_stats.values()),  # Will be 0 due to filtering
            'total_failed_24h': sum(s['failed_jobs_24h'] for s in final_stats.values()),
            'total_aborted_24h': sum(s['aborted_jobs_24h'] for s in final_stats.values()),
            'query_time': datetime.now().isoformat(),
            'time_range': '24 hours (active jobs only)',
            'note': 'Analysis excludes completed jobs - focuses on active job states (up to 500 jobs examined)'
        }

        return {
            'clusters': final_stats,
            'summary': summary,
            'success': True
        }

    def get_cluster_job_statistics(self) -> Dict[str, Any]:
        """Get comprehensive job statistics by cluster from ECL Watch."""

        # Use unified ECL Watch data to avoid duplicate API calls
        unified_data = self._get_unified_ecl_watch_data()

        if "error" in unified_data:
            return {
                "error": unified_data["error"],
                "note": unified_data.get("note", "ECL Watch query failed")
            }

        # Return the cluster statistics from the unified query
        cluster_statistics = unified_data.get("cluster_statistics", {})

        if not cluster_statistics.get("success", False):
            return {
                "error": "Cluster statistics not available",
                "note": "Detailed cluster statistics require ECL Watch credentials"
            }

        return cluster_statistics

    def _determine_cluster_activity_status(self, stats: Dict) -> str:
        """Determine the activity status of a cluster based on job statistics."""
        running = stats['running_jobs']
        submitted = stats.get('submitted_jobs', 0)
        compiling = stats.get('compiling_jobs', 0)
        compiled = stats.get('compiled_jobs', 0)
        queued = stats['queued_jobs']
        completed = stats['completed_jobs_24h']

        # Total active jobs (not including queued/blocked)
        total_active = running + submitted + compiling + compiled

        if total_active >= 5:
            return "High Activity"
        elif total_active >= 2 or (total_active >= 1 and queued >= 2):
            return "Active"
        elif total_active >= 1 or queued >= 1 or completed >= 10:
            return "Light Activity"
        elif completed >= 1:
            return "Recent Activity"
        else:
            return "Idle"

    def analyze_job_node_correlation(self) -> Dict[str, Any]:
        """Correlate ECL Watch job activity with specific node utilization patterns."""
        if not self.ecl_watch_url or not self._ecl_watch_username_param:
            return {"error": "ECL Watch URL and username required for job-node correlation"}

        try:
            # Get ECL Watch job analysis
            job_analysis = self.get_ecl_watch_job_analysis()
            if "error" in job_analysis:
                return {"error": f"ECL Watch analysis failed: {job_analysis['error']}"}

            # Get node utilization and efficiency data
            utilization = self.get_node_utilization()
            efficiency = self.analyze_resource_efficiency(utilization)

            if "error" in efficiency:
                return {"error": f"Node efficiency analysis failed: {efficiency['error']}"}

            # Build correlation analysis
            correlation_analysis = {
                "thor_cluster_analysis": {},
                "mismatched_resources": [],
                "justified_idle_nodes": [],
                "unjustified_idle_nodes": [],
                "summary": {
                    "total_clusters_analyzed": 0,
                    "clusters_with_jobs": 0,
                    "clusters_idle": 0,
                    "mismatched_nodes": 0,
                    "correlation_accuracy": 0.0
                }
            }

            # Extract running and recent jobs
            running_jobs = job_analysis.get("running_jobs", [])
            recent_jobs = job_analysis.get("recent_jobs", [])
            all_jobs = running_jobs + recent_jobs

            # Group jobs by Thor cluster
            jobs_by_cluster = defaultdict(lambda: {
                "running": 0,
                "recent": 0,
                "total": 0,
                "job_details": []
            })

            for job in all_jobs:
                cluster_name = job.get("cluster", "unknown")
                if cluster_name != "unknown":
                    if job in running_jobs:
                        jobs_by_cluster[cluster_name]["running"] += 1
                    else:
                        jobs_by_cluster[cluster_name]["recent"] += 1
                    jobs_by_cluster[cluster_name]["total"] += 1
                    jobs_by_cluster[cluster_name]["job_details"].append({
                        "wuid": job.get("wuid", ""),
                        "state": job.get("state", ""),
                        "queue": job.get("queue", ""),
                        "owner": job.get("owner", "")
                    })

            # Analyze each Thor cluster and correlate with nodes
            all_nodes = (efficiency.get("idle_nodes", []) +
                        efficiency.get("underutilized_nodes", []) +
                        efficiency.get("efficient_nodes", []))

            # Group nodes by their Thor cluster assignment
            nodes_by_thor_cluster = defaultdict(lambda: {
                "idle": [],
                "underutilized": [],
                "efficient": [],
                "total_nodes": 0
            })

            for node in all_nodes:
                pool_name = node.get("node_pool", "")
                thor_cluster = self._get_thor_cluster_info(pool_name).get("thor_cluster", "N/A")

                if thor_cluster != "N/A":
                    # Determine node status
                    if node in efficiency.get("idle_nodes", []):
                        nodes_by_thor_cluster[thor_cluster]["idle"].append(node)
                    elif node in efficiency.get("underutilized_nodes", []):
                        nodes_by_thor_cluster[thor_cluster]["underutilized"].append(node)
                    else:
                        nodes_by_thor_cluster[thor_cluster]["efficient"].append(node)

                    nodes_by_thor_cluster[thor_cluster]["total_nodes"] += 1

            # Perform correlation analysis for each Thor cluster
            for thor_cluster in set(list(jobs_by_cluster.keys()) + list(nodes_by_thor_cluster.keys())):
                job_data = jobs_by_cluster.get(thor_cluster, {"running": 0, "recent": 0, "total": 0, "job_details": []})
                node_data = nodes_by_thor_cluster.get(thor_cluster, {"idle": [], "underutilized": [], "efficient": [], "total_nodes": 0})

                # Analyze the correlation
                has_active_jobs = job_data["running"] > 0
                has_recent_jobs = job_data["recent"] > 0
                idle_node_count = len(node_data["idle"])
                underutil_node_count = len(node_data["underutilized"])
                total_node_count = node_data["total_nodes"]

                # Determine correlation status
                correlation_status = "unknown"
                justification = ""

                if has_active_jobs and idle_node_count > 0:
                    correlation_status = "mismatch"
                    justification = f"Active jobs ({job_data['running']}) but {idle_node_count} idle nodes"
                elif has_active_jobs and underutil_node_count > 0:
                    correlation_status = "partial_mismatch"
                    justification = f"Active jobs ({job_data['running']}) but {underutil_node_count} underutilized nodes"
                elif has_recent_jobs and idle_node_count > 0:
                    correlation_status = "recent_activity"
                    justification = f"Recent jobs ({job_data['recent']}) with {idle_node_count} idle nodes - may be justified"
                elif not has_active_jobs and not has_recent_jobs and idle_node_count > 0:
                    correlation_status = "justified_idle"
                    justification = f"No job activity - {idle_node_count} idle nodes justified"
                elif has_active_jobs and total_node_count > 0:
                    correlation_status = "well_utilized"
                    justification = f"Active jobs ({job_data['running']}) with appropriate node utilization"
                else:
                    correlation_status = "no_data"
                    justification = "Insufficient data for correlation analysis"

                # Store analysis results
                correlation_analysis["thor_cluster_analysis"][thor_cluster] = {
                    "job_activity": {
                        "running_jobs": job_data["running"],
                        "recent_jobs": job_data["recent"],
                        "total_jobs": job_data["total"],
                        "job_details": job_data["job_details"][:5]  # Limit details
                    },
                    "node_utilization": {
                        "idle_nodes": len(node_data["idle"]),
                        "underutilized_nodes": len(node_data["underutilized"]),
                        "efficient_nodes": len(node_data["efficient"]),
                        "total_nodes": total_node_count,
                        "idle_node_details": [{"name": n.get("name", ""), "cpu": n.get("cpu_percent", 0), "memory": n.get("memory_percent", 0)} for n in node_data["idle"][:3]]
                    },
                    "correlation_status": correlation_status,
                    "justification": justification,
                    "recommendation": self._get_correlation_recommendation(correlation_status, job_data, node_data)
                }

                # Track mismatched resources
                if correlation_status in ["mismatch", "partial_mismatch"]:
                    correlation_analysis["mismatched_resources"].append({
                        "thor_cluster": thor_cluster,
                        "status": correlation_status,
                        "justification": justification,
                        "idle_nodes": len(node_data["idle"]),
                        "underutilized_nodes": len(node_data["underutilized"]),
                        "running_jobs": job_data["running"]
                    })

                # Categorize idle nodes
                for node in node_data["idle"]:
                    if correlation_status in ["justified_idle", "recent_activity"]:
                        correlation_analysis["justified_idle_nodes"].append(node)
                    elif correlation_status in ["mismatch", "partial_mismatch"]:
                        correlation_analysis["unjustified_idle_nodes"].append(node)

            # Calculate summary statistics
            correlation_analysis["summary"]["total_clusters_analyzed"] = len(correlation_analysis["thor_cluster_analysis"])
            correlation_analysis["summary"]["clusters_with_jobs"] = len([c for c in correlation_analysis["thor_cluster_analysis"].values() if c["job_activity"]["total_jobs"] > 0])
            correlation_analysis["summary"]["clusters_idle"] = len([c for c in correlation_analysis["thor_cluster_analysis"].values() if c["job_activity"]["total_jobs"] == 0])
            correlation_analysis["summary"]["mismatched_nodes"] = len(correlation_analysis["mismatched_resources"])

            # Calculate correlation accuracy
            total_correlations = len(correlation_analysis["thor_cluster_analysis"])
            accurate_correlations = len([c for c in correlation_analysis["thor_cluster_analysis"].values() if c["correlation_status"] in ["well_utilized", "justified_idle"]])
            if total_correlations > 0:
                correlation_analysis["summary"]["correlation_accuracy"] = (accurate_correlations / total_correlations) * 100

            return correlation_analysis

        except Exception as e:
            return {"error": f"Job-node correlation analysis failed: {str(e)}"}

    def _get_correlation_recommendation(self, correlation_status: str, job_data: Dict, node_data: Dict) -> str:
        """Generate recommendation based on job-node correlation analysis."""
        if correlation_status == "mismatch":
            return f"🚨 CRITICAL: Scale up resources immediately - {job_data['running']} jobs need more nodes"
        elif correlation_status == "partial_mismatch":
            return f"⚠️  OPTIMIZE: Consider right-sizing - active jobs with underutilized nodes"
        elif correlation_status == "recent_activity":
            return f"🟡 MONITOR: Recent activity detected - maintain some capacity for job spikes"
        elif correlation_status == "justified_idle":
            return f"✅ SCALE DOWN: No job activity - safe to remove idle nodes"
        elif correlation_status == "well_utilized":
            return f"✅ OPTIMAL: Good job-to-node ratio - maintain current resources"
        else:
            return f"❓ REVIEW: Insufficient data - manual analysis recommended"

    def _test_cost_api_availability(self) -> bool:
        """Test Azure Cost Management API availability once and cache the result."""
        if self._cost_api_tested:
            return self._cost_api_available

        if not self._cost_api_message_shown:
            print("Testing Azure Cost Management API accessibility...")
            self._cost_api_message_shown = True

        self._cost_api_tested = True

        try:
            # Try a simple consumption usage query with very short timeout
            usage_result = self.run_az_command([
                "az", "consumption", "usage", "list",
                "--top", "1",
                "-o", "json"
            ], timeout=10)  # Short timeout for initial test

            if "error" not in usage_result and usage_result:
                self._cost_api_available = True
                print("✅ Azure Cost Management API is accessible")
                return True
            else:
                self._cost_api_available = False
                print("⚠️  Azure Cost Management API not accessible - using estimated costs instead")
                print("   To get actual billing data, ensure you have:")
                print("   • Cost Management Reader role on the subscription")
                print("   • Azure CLI consumption extension working properly")
                print("   • Proper authentication and permissions")
                return False

        except Exception as e:
            self._cost_api_available = False
            print("⚠️  Azure Cost Management API not accessible - using estimated costs instead")
            print("   To get actual billing data, ensure you have:")
            print("   • Cost Management Reader role on the subscription")
            print("   • Azure CLI consumption extension working properly")
            print("   • Proper authentication and permissions")
            return False

    def _get_detailed_pool_node_info(self, pool_name: str) -> List[Dict[str, Any]]:
        """Get detailed node information for a specific pool."""
        try:
            # Get efficiency data to access node details
            utilization = self.get_node_utilization()
            efficiency = self.analyze_resource_efficiency(utilization)

            if "error" in efficiency:
                return []

            # Extract nodes from each category
            all_nodes = (
                efficiency.get("idle_nodes", []) +
                efficiency.get("underutilized_nodes", []) +
                efficiency.get("well_utilized_nodes", [])
            )

            # Filter nodes by the specific pool
            pool_nodes = []
            for node in all_nodes:
                node_pool = node.get("node_pool", "")
                if node_pool == pool_name:
                    pool_nodes.append(node)

            return pool_nodes

        except Exception as e:
            print(f"Warning: Could not get detailed pool node info for {pool_name}: {e}", file=sys.stderr)
            return []



    def _print_node_pool_cost_analysis(self):
        """Print detailed cost analysis grouped by node pool."""
        print(f"\n\n🏊 NODE POOL COST ANALYSIS")
        print("-" * 80)

        # Get efficiency and cost data
        utilization = self.get_node_utilization()
        efficiency = self.analyze_resource_efficiency(utilization)
        costs = self.estimate_costs()

        if "error" in efficiency:
            print("❌ Unable to analyze node pool costs - utilization data unavailable")
            return

        # Check if we're in fallback mode (no real utilization metrics)
        if efficiency.get("fallback_mode", False):
            print("⚠️  Node pool analysis available with limited data (kubectl access required for utilization metrics)")
            print("📊 Showing basic node pool information from Azure API:")
            print()

        # Get detailed node analysis
        detailed_analysis = self.get_detailed_node_analysis(efficiency)
        pool_analysis = detailed_analysis.get("pool_analysis", {})

        if not pool_analysis:
            # If in fallback mode, show basic pool cost information instead
            if efficiency.get("fallback_mode", False):
                print("📊 Basic node pool information (utilization metrics unavailable):")
                print()

                # Show basic pool costs without waste analysis
                pool_costs = costs.get("by_pool", [])
                if pool_costs:
                    print(f"{'Pool Name':<20} {'VM Size':<15} {'Nodes':<8} {'Monthly Cost':<15} {'Cost/Node':<12}")
                    print("-" * 80)

                    total_cost = 0
                    for pool in pool_costs:
                        pool_name = pool.get("pool_name", "")[:19]
                        vm_size = pool.get("vm_size", "")[:14] 
                        count = pool.get("count", 0)
                        monthly_cost = pool.get("monthly_cost", 0)
                        cost_per_node = monthly_cost / max(count, 1)
                        total_cost += monthly_cost

                        print(f"{pool_name:<20} {vm_size:<15} {count:<8} ${monthly_cost:>10,.2f}     ${cost_per_node:>8,.2f}")

                    print("-" * 80)
                    print(f"{'TOTAL':<20} {'':<15} {'':<8} ${total_cost:>10,.2f}     {'':<12}")

                    print(f"\n💡 To get utilization analysis and waste detection, ensure kubectl access to the cluster.")
                else:
                    print("No pool cost information available")
            else:
                print("✅ No node pool waste detected")
            return

        # Get cost information by pool from cost estimates
        pool_costs = costs.get("by_pool", [])
        pool_cost_lookup = {pool.get("pool_name"): pool for pool in pool_costs}

        # Create enhanced pool analysis with cost details
        enhanced_pool_data = []

        for pool_name, pool_data in pool_analysis.items():
            pool_cost_info = pool_cost_lookup.get(pool_name, {})

            enhanced_pool_data.append({
                "pool_name": pool_name,
                "idle_nodes": pool_data["idle_nodes"],
                "underutilized_nodes": pool_data["underutilized_nodes"],
                "total_waste_nodes": pool_data["total_waste"],
                "monthly_cost": pool_cost_info.get("monthly_cost", 0),
                "vm_size": pool_cost_info.get("vm_size", "Unknown"),
                "node_count": pool_cost_info.get("count", 0),
                "waste_percentage": round((pool_data["total_waste"] / max(pool_cost_info.get("count", 1), 1)) * 100, 1)
            })

        # Sort by waste percentage (highest first)
        enhanced_pool_data.sort(key=lambda x: x["waste_percentage"], reverse=True)

        # Display pool analysis
        print(f"Found {len(enhanced_pool_data)} node pools with waste:")

        for pool in enhanced_pool_data:
            if pool["total_waste_nodes"] > 0:
                print(f"\n🏊 Pool: {pool['pool_name']}")
                print(f"   VM Size: {pool['vm_size']} | Node Count: {pool['node_count']} | Monthly Cost: ${pool['monthly_cost']:,.2f}")
                print(f"   Waste: {pool['total_waste_nodes']} nodes ({pool['waste_percentage']}% of pool)")

                if pool["idle_nodes"] > 0:
                    idle_cost = (pool["idle_nodes"] / pool["node_count"]) * pool["monthly_cost"]
                    print(f"   • Idle: {pool['idle_nodes']} nodes (${idle_cost:,.2f}/month wasted)")

                if pool["underutilized_nodes"] > 0:
                    underutil_cost = (pool["underutilized_nodes"] / pool["node_count"]) * pool["monthly_cost"] * 0.3
                    print(f"   • Underutilized: {pool['underutilized_nodes']} nodes (~${underutil_cost:,.2f}/month potential savings)")

    def list_idle_nodes(self):
        """List all idle nodes with their details."""
        print("🛌 IDLE NODES REPORT")
        print("=" * 60)

        utilization = self.get_node_utilization()
        efficiency = self.analyze_resource_efficiency(utilization)

        if "error" in efficiency:
            print("❌ Unable to retrieve node utilization data")
            return

        idle_nodes = efficiency.get("idle_nodes", [])

        if not idle_nodes:
            print("✅ No idle nodes detected!")
            return

        print(f"Found {len(idle_nodes)} idle nodes:\n")

        for i, node in enumerate(idle_nodes, 1):
            print(f"{i}. {node.get('name', 'Unknown')}")
            print(f"   Pool: {node.get('node_pool', 'Unknown')}")
            print(f"   CPU: {node.get('cpu_percent', '0')}% | Memory: {node.get('memory_percent', '0')}%")
            print(f"   Age: {node.get('age_display', 'Unknown')}")
            if node.get('idle_duration_display'):
                print(f"   Idle Duration: {node.get('idle_duration_display')}")
            print()

    def list_underutilized_nodes(self):
        """List all underutilized nodes with their details."""
        print("📊 UNDERUTILIZED NODES REPORT")
        print("=" * 60)

        utilization = self.get_node_utilization()
        efficiency = self.analyze_resource_efficiency(utilization)

        if "error" in efficiency:
            print("❌ Unable to retrieve node utilization data")
            return

        underutilized_nodes = efficiency.get("underutilized_nodes", [])

        if not underutilized_nodes:
            print("✅ No underutilized nodes detected!")
            return

        print(f"Found {len(underutilized_nodes)} underutilized nodes:\n")

        for i, node in enumerate(underutilized_nodes, 1):
            print(f"{i}. {node.get('name', 'Unknown')}")
            print(f"   Pool: {node.get('node_pool', 'Unknown')}")
            print(f"   CPU: {node.get('cpu_percent', '0')}% | Memory: {node.get('memory_percent', '0')}%")
            print(f"   Age: {node.get('age_display', 'Unknown')}")
            print()

# End of AKSClusterInsights class


# Main functions and utility code below

    def _get_detailed_pool_node_info(self, pool_name: str) -> List[Dict[str, Any]]:
        """Get detailed node information for a specific pool."""
        try:
            # Get efficiency data to access node details
            utilization = self.get_node_utilization()
            efficiency = self.analyze_resource_efficiency(utilization)

            if "error" in efficiency:
                return []

            # Get all nodes (idle, underutilized, and efficient)
            all_nodes = []

            # Add idle nodes
            for node in efficiency.get("idle_nodes", []):
                if node.get("node_pool") == pool_name:
                    node_info = node.copy()
                    node_info["status"] = "idle"
                    all_nodes.append(node_info)

            # Add underutilized nodes
            for node in efficiency.get("underutilized_nodes", []):
                if node.get("node_pool") == pool_name:
                    node_info = node.copy()
                    node_info["status"] = "underutilized"
                    all_nodes.append(node_info)

            # Add efficient nodes for completeness
            for node in efficiency.get("efficient_nodes", []):
                if node.get("node_pool") == pool_name:
                    node_info = node.copy()
                    node_info["status"] = "efficient"
                    all_nodes.append(node_info)

            # Sort by idle duration (longest first) for idle nodes, then by utilization
            def sort_key(node):
                if node["status"] == "idle":
                    return (0, -(node.get("idle_duration_hours", 0)))  # Idle first, longest duration first
                elif node["status"] == "underutilized":
                    cpu = node.get("cpu_percent_numeric", 0)
                    mem = node.get("memory_percent_numeric", 0)
                    return (1, cpu + mem)  # Underutilized second, lowest utilization first
                else:
                    return (2, 0)  # Efficient last

            all_nodes.sort(key=sort_key)
            return all_nodes

        except Exception as e:
            print(f"Warning: Could not get detailed node info for pool {pool_name}: {e}", file=sys.stderr)
            return []



    def print_cost_optimization_summary(self):
        """Print focused cost optimization summary report."""
        print("\n" + "="*80)
        print(f"AKS CLUSTER COST OPTIMIZATION REPORT")
        print(f"Cluster: {self.cluster_name}")
        print(f"Subscription: {self.subscription}")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")

        # Generate cost savings summary
        summary = self.generate_cost_savings_summary()

        # Also get efficiency data directly for idle duration stats
        utilization = self.get_node_utilization()
        efficiency = self.analyze_resource_efficiency(utilization)

        # Get cost info for discount display
        costs = self.estimate_costs()

        # Executive Summary
        print("🎯 EXECUTIVE SUMMARY")
        print("-" * 80)
        exec_summary = summary["executive_summary"]
        waste_summary = summary["waste_identification"]

        # Display metrics warning if present
        metrics_warning = efficiency.get("summary", {}).get("metrics_warning", "")
        if metrics_warning:
            print(f"🚨 {metrics_warning}")
            print()

        # Display cost information with source indicator
        cost_source = exec_summary.get('cost_source', 'estimated')
        cost_confidence = exec_summary.get('cost_confidence', 'low')

        if cost_source == "actual_projected":
            cost_indicator = f"(projected from {exec_summary.get('days_tracked', 0)} days actual)"
            confidence_icon = "🟢" if cost_confidence == "high" else "🟡"
        else:
            cost_indicator = "(estimated from VM pricing)"
            confidence_icon = "🔴"

        # Show actual month-to-date if available
        if exec_summary.get('actual_month_to_date', 0) > 0:
            print(f"📅 Month-to-Date (Actual):   ${exec_summary['actual_month_to_date']:,.2f} ({exec_summary.get('days_tracked', 0)} days)")
            print(f"🎯 Cost Data Quality:        {confidence_icon} {cost_confidence.title()}")
        else:
            print(f"💡 Note: Using VM pricing estimates. For actual costs, ensure Cost Management Reader permissions.")

        print(f"📊 Cost Efficiency Grade:    {exec_summary['cost_efficiency_grade']}")
        print(f"⚡ Average Utilization:      {exec_summary['average_utilization']:.1f}%")
        print(f"🎯 Potential Monthly Savings: ${exec_summary['potential_monthly_savings']:,.2f}")
        print(f"📈 Annual Savings Potential:  ${exec_summary['potential_monthly_savings'] * 12:,.2f}")

        if exec_summary['potential_monthly_savings'] > 0:
            savings_percent = (exec_summary['potential_monthly_savings'] / exec_summary['current_monthly_estimate']) * 100
            print(f"📉 Cost Reduction Potential:  {savings_percent:.1f}%")

        # Workload Context Analysis (if ECL Watch is configured)
        workload_context = efficiency.get("summary", {}).get("workload_context", {})
        job_analysis = efficiency.get("job_analysis")
        if workload_context and job_analysis and "error" not in job_analysis:
            print(f"\n\n🏗️  WORKLOAD CONTEXT ANALYSIS")
            print("-" * 80)

            print(f"ECL Watch URL:         {job_analysis.get('ecl_watch_url', 'N/A')}")
            print(f"Analysis Time:         {job_analysis.get('timestamp', 'N/A')[:19].replace('T', ' ')}")
            print()

            # Job activity summary - get data from the same source as cluster statistics
            cluster_stats_data = self.get_cluster_job_statistics()
            if "error" not in cluster_stats_data and cluster_stats_data.get("success", False):
                cluster_summary = cluster_stats_data.get("summary", {})
                total_running = cluster_summary.get('total_running_jobs', 0)
                total_queued = cluster_summary.get('total_queued_jobs', 0)
                total_submitted = cluster_summary.get('total_submitted_jobs', 0)
                total_compiling = cluster_summary.get('total_compiling_jobs', 0)
                total_compiled = cluster_summary.get('total_compiled_jobs', 0)
                total_active_jobs = total_running + total_queued + total_submitted + total_compiling + total_compiled
            else:
                # Fallback to workload context if cluster stats unavailable
                total_running = workload_context.get('total_running_jobs', 0)
                total_active_jobs = total_running

            total_recent = workload_context.get('total_recent_jobs', 0)
            print(f"🚀 JOB ACTIVITY:")
            print(f"   Currently Running:   {total_running} jobs")
            if 'cluster_stats_data' in locals() and "error" not in cluster_stats_data:
                print(f"   Total Active Jobs:   {total_active_jobs} jobs (running + queued + submitted + compiling + compiled)")
            print(f"   Recent (2 hours):    {total_recent} jobs")
            print()

            # Workload assessment
            idle_justified = workload_context.get('idle_justified', False)
            underutil_justified = workload_context.get('underutilization_justified', False)
            workload_summary = workload_context.get('workload_summary', 'No assessment available')

            print(f"📊 WORKLOAD ASSESSMENT:")
            print(f"   {workload_summary}")
            print()

            # Add cost analysis
            # Get current cost from the main cost estimation
            costs = self.estimate_costs()
            current_monthly_cost = costs.get('estimated_monthly', 0)
            
            if current_monthly_cost > 0:
                # Convert monthly cost to hourly cost (monthly / (30 days * 24 hours))
                current_hourly_cost = current_monthly_cost / (30 * 24)
                # Calculate yearly costs
                yearly_total_cost = current_hourly_cost * 24 * 365
                
                print(f"💰 COST ANALYSIS:")
                print(f"   Current Cluster Cost:     ${current_hourly_cost:,.2f}/hour (projected ${yearly_total_cost:,.0f}/year)")
                
                # Calculate cost per currently running job per hour
                if total_running > 0:
                    hourly_cost_per_job = current_hourly_cost / total_running
                    yearly_cost_per_job = hourly_cost_per_job * 24 * 365
                    print(f"   Cost per Running Job:    ${hourly_cost_per_job:,.2f}/job/hour (${yearly_cost_per_job:,.0f}/job/year)")
                else:
                    print(f"   Cost per Running Job:    N/A (no running jobs)")
                
                # Calculate Thor worker nodes cost analysis
                thor_worker_cost_analysis = self._calculate_thor_worker_costs_with_data(efficiency)
                if thor_worker_cost_analysis and thor_worker_cost_analysis.get('thor_worker_nodes', 0) > 0:
                    thor_monthly_cost = thor_worker_cost_analysis['total_monthly_cost']
                    thor_hourly_cost = thor_monthly_cost / (30 * 24)
                    thor_yearly_cost = thor_hourly_cost * 24 * 365
                    thor_nodes = thor_worker_cost_analysis['thor_worker_nodes']
                    
                    print(f"   Nodes Hosting Thor Workers: ${thor_hourly_cost:,.2f}/hour (${thor_yearly_cost:,.0f}/year)")
                    print(f"   Thor Worker Hosting Nodes:  {thor_nodes} nodes")
                    
                    if total_running > 0:
                        thor_cost_per_job = thor_hourly_cost / total_running
                        thor_yearly_per_job = thor_cost_per_job * 24 * 365
                        print(f"   Thor Node Cost per Job:     ${thor_cost_per_job:,.2f}/job/hour (${thor_yearly_per_job:,.0f}/job/year)")
                else:
                    # Add debug output to understand the issue
                    if thor_worker_cost_analysis and 'error' in thor_worker_cost_analysis:
                        print(f"   Note: Thor worker hosting analysis failed: {thor_worker_cost_analysis['error']}")
                    elif thor_worker_cost_analysis:
                        # Check if we found any pods but no matching nodes
                        node_details = thor_worker_cost_analysis.get('node_details', [])
                        debug_info = thor_worker_cost_analysis.get('debug_info', {})
                        print(f"   Note: Thor pods on {debug_info.get('thor_pod_nodes_found', 0)} nodes, {debug_info.get('available_cost_pools', 0)} cost pools, {len(node_details)} matches")
                        print(f"   Debug: analyzed {debug_info.get('nodes_analyzed', 0)} nodes, {debug_info.get('pods_by_node_keys', 0)} nodes with pods")
                        if debug_info.get('sample_thor_nodes'):
                            print(f"   Sample Thor nodes: {debug_info.get('sample_thor_nodes', [])}")
                        if debug_info.get('sample_cost_pools'):
                            print(f"   Sample cost pools: {debug_info.get('sample_cost_pools', [])}")
                    else:
                        print(f"   Note: No Thor worker hosting nodes identified")
                
                print()

            if idle_justified:
                print(f"   ✅ Idle nodes justified by low job activity")
            else:
                print(f"   ⚠️  Idle nodes NOT justified - active jobs detected")

            if underutil_justified:
                print(f"   ✅ Underutilization expected given current workload")
            else:
                print(f"   ⚠️  Underutilization unexpected - investigate job distribution")
            print()

            # Job-Node Correlation Analysis
            correlation_analysis = self.analyze_job_node_correlation()
            if "error" not in correlation_analysis:
                print(f"🔗 JOB-TO-NODE CORRELATION ANALYSIS:")
                print(f"   Thor Clusters Analyzed: {correlation_analysis['summary']['total_clusters_analyzed']}")
                print(f"   Clusters with Jobs:     {correlation_analysis['summary']['clusters_with_jobs']}")
                print(f"   Correlation Accuracy:   {correlation_analysis['summary']['correlation_accuracy']:.1f}%")
                print()

                # Show mismatched resources (critical findings)
                mismatches = correlation_analysis.get('mismatched_resources', [])
                if mismatches:
                    print(f"🚨 RESOURCE MISMATCHES DETECTED:")
                    for mismatch in mismatches[:3]:  # Show top 3 mismatches
                        cluster = mismatch['thor_cluster']
                        status = mismatch['status']
                        running_jobs = mismatch['running_jobs']
                        idle_nodes = mismatch['idle_nodes']
                        underutil_nodes = mismatch['underutilized_nodes']

                        if status == "mismatch":
                            print(f"   🔴 {cluster}: {running_jobs} running jobs but {idle_nodes} idle nodes")
                        elif status == "partial_mismatch":
                            print(f"   🟡 {cluster}: {running_jobs} running jobs but {underutil_nodes} underutilized nodes")
                    print()

                # Show top Thor cluster analysis
                thor_analysis = correlation_analysis.get('thor_cluster_analysis', {})
                active_clusters = [(k, v) for k, v in thor_analysis.items()
                                 if v['job_activity']['running_jobs'] > 0]
                active_clusters.sort(key=lambda x: x[1]['job_activity']['running_jobs'], reverse=True)

                if active_clusters:
                    print(f"📊 THOR CLUSTER ACTIVITY CORRELATION:")
                    for cluster_name, analysis in active_clusters[:5]:  # Top 5 active clusters
                        job_activity = analysis['job_activity']
                        node_util = analysis['node_utilization']
                        status = analysis['correlation_status']

                        running = job_activity['running_jobs']
                        idle_nodes = node_util['idle_nodes']
                        total_nodes = node_util['total_nodes']

                        status_icon = "🟢" if status == "well_utilized" else "🟡" if status == "recent_activity" else "🔴"
                        print(f"   {status_icon} {cluster_name:<20} {running:>2} jobs | {idle_nodes:>2}/{total_nodes:>2} idle/total nodes")
                    print()

                # Specific recommendations based on correlation
                print(f"🎯 CORRELATION-BASED RECOMMENDATIONS:")
                recommendation_count = 0
                for cluster_name, analysis in thor_analysis.items():
                    recommendation = analysis.get('recommendation', '')
                    if recommendation and recommendation.startswith('🚨'):
                        recommendation_count += 1
                        print(f"   {recommendation_count}. {cluster_name}: {recommendation}")

                if recommendation_count == 0:
                    print(f"   ✅ No critical job-node mismatches detected")
                print()

            # Recommendations based on workload context
            recommendations = workload_context.get('recommendations', [])
            if recommendations:
                print(f"💡 WORKLOAD-BASED RECOMMENDATIONS:")
                for i, rec in enumerate(recommendations, 1):
                    print(f"   {i}. {rec}")
                print()
        elif self.ecl_watch_url:
            print(f"\n\n🏗️  WORKLOAD CONTEXT")
            print("-" * 80)
            print(f"⚠️  ECL Watch configured ({self.ecl_watch_url}) but job analysis failed")
            if job_analysis and "error" in job_analysis:
                print(f"   Error: {job_analysis['error']}")
            note = job_analysis.get('note', 'Unable to correlate job activity with node utilization') if job_analysis else 'Unable to correlate job activity with node utilization'
            print(f"   Note: {note}")
            print()

        # Add cluster job statistics if ECL Watch is configured
        if self.ecl_watch_url and self._ecl_watch_username_param:
            self.print_cluster_job_statistics()

        # Waste Identification with detailed cost breakdowns
        print(f"\n\n🗑️  RESOURCE WASTE ANALYSIS")
        print("-" * 80)

        # Check if metrics are unreliable
        if waste_summary.get("metrics_unreliable", False):
            print("⚠️  WARNING: Skipping waste analysis due to unreliable utilization metrics")
            print("🔍 ISSUE: Active jobs detected but all nodes show 0% utilization")
            print("📊 ECL Watch shows active workload, but kubectl metrics unavailable")
            print("🛠️  ACTION: Fix kubectl access and metrics server before analyzing resource waste")
            print("\n💡 RECOMMENDATIONS:")
            print("   1. Verify kubectl can connect to cluster")
            print("   2. Check metrics-server pod status: kubectl get pods -n kube-system | grep metrics")
            print("   3. Ensure proper RBAC permissions for metrics access")
            print("   4. Re-run analysis after fixing metrics collection")
            return

        # Calculate per-node cost details
        costs = self.estimate_costs()
        total_monthly_cost = costs.get("estimated_monthly", 0)
        total_nodes = exec_summary['total_nodes']
        avg_cost_per_node = total_monthly_cost / max(total_nodes, 1)

        print(f"Total Nodes:           {total_nodes}")
        print()

        # Idle nodes analysis
        idle_nodes = waste_summary['idle_nodes']
        idle_total_cost = waste_summary['idle_cost_estimate']
        if idle_nodes > 0:
            idle_cost_per_node = idle_total_cost / idle_nodes
            print(f"💸 IDLE NODES ANALYSIS:")
            print(f"   Count:              {idle_nodes} nodes")
            print(f"   Total Monthly Cost: ${idle_total_cost:,.2f}")
            print(f"   💰 Savings/Node:     ${idle_cost_per_node:,.2f}/month (100% - remove entirely)")
            print(f"   🎯 Total Savings:    ${idle_total_cost:,.2f}/month")
        else:
            print(f"✅ IDLE NODES:         None detected")

        print()

        # Underutilized nodes analysis
        underutil_nodes = waste_summary['underutilized_nodes']
        underutil_total_cost = waste_summary['underutilized_cost_estimate']
        if underutil_nodes > 0:
            underutil_cost_per_node = avg_cost_per_node  # Full cost per underutilized node
            potential_savings_per_node = underutil_cost_per_node * 0.3  # 30% savings from right-sizing
            print(f"📉 UNDERUTILIZED NODES ANALYSIS:")
            print(f"   Count:              {underutil_nodes} nodes")
            print(f"   💰 Savings/Node:     ${potential_savings_per_node:,.2f}/month (30% - right-size to smaller VM)")
            print(f"   🎯 Total Savings:    ${underutil_total_cost:,.2f}/month")
        else:
            print(f"✅ UNDERUTILIZED:      None detected")

        print()
        efficient_nodes = max(0, total_nodes - idle_nodes - underutil_nodes)
        print(f"✅ Efficiently Used:   {efficient_nodes} nodes (${efficient_nodes * avg_cost_per_node:,.2f}/month)")

        # Combined savings summary
        total_potential_savings = idle_total_cost + underutil_total_cost
        if total_potential_savings > 0:
            print()
            print("💡 IMMEDIATE COST OPTIMIZATION POTENTIAL:")
            print(f"   🗑️  Remove {idle_nodes} idle nodes:        ${idle_total_cost:,.2f}/month")
            print(f"   📉 Right-size {underutil_nodes} nodes:     ${underutil_total_cost:,.2f}/month")
            print(f"   🎯 TOTAL MONTHLY SAVINGS:              ${total_potential_savings:,.2f}")
            print(f"   📈 ANNUAL SAVINGS POTENTIAL:           ${total_potential_savings * 12:,.2f}")

            if total_monthly_cost > 0:
                savings_percentage = (total_potential_savings / total_monthly_cost) * 100
                print(f"   📊 Cost Reduction:                     {savings_percentage:.1f}%")

        # Resource waste rate summary
        if total_nodes > 0:
            # Ensure waste calculation is mathematically sound (idle + underutil can't exceed total)
            actual_waste_nodes = min(idle_nodes + underutil_nodes, total_nodes)
            waste_percent = (actual_waste_nodes / total_nodes) * 100
            actual_efficient_nodes = total_nodes - actual_waste_nodes

            print()
            print(f"📊 WASTE SUMMARY:")
            print(f"   Resource Waste Rate:   {waste_percent:.1f}% ({actual_waste_nodes}/{total_nodes} nodes)")
            print(f"   Efficiency Rate:       {100 - waste_percent:.1f}% ({actual_efficient_nodes}/{total_nodes} nodes)")

            # Add warning if node counts seem inconsistent
            if idle_nodes + underutil_nodes > total_nodes:
                print(f"   ⚠️  Note: Node counts may be fluctuating due to cluster scaling activity")

        # Add idle duration statistics if available
        idle_duration_stats = waste_summary.get('idle_duration_stats', {})
        if idle_duration_stats:
            print(f"\n⏰ IDLE DURATION ANALYSIS:")
            print(f"   Avg Idle Duration:     {idle_duration_stats['avg_idle_hours']:.1f}h ({idle_duration_stats['nodes_with_duration']} nodes tracked)")
            if idle_duration_stats.get('max_idle_hours', 0) > 24:
                print(f"   ⚠️ Longest Idle:        {idle_duration_stats['max_idle_hours']:.1f}h ({idle_duration_stats['max_idle_hours']/24:.1f} days)")

        # Historical Waste Analysis
        optimization_opportunities = self.calculate_cost_optimization_opportunities()
        historical_waste = optimization_opportunities.get("historical_waste_summary", {})
        if historical_waste and historical_waste.get("analysis_period_days", 0) > 0:
            print(f"\n\n📈 HISTORICAL WASTE ANALYSIS")
            print("-" * 80)
            print(f"Analysis Period:       {historical_waste['analysis_period_days']} days")
            print(f"Confidence Level:      {historical_waste['confidence_level'].title()}")
            if historical_waste.get("avg_daily_idle_waste", 0) > 0:
                monthly_historical_waste = historical_waste["avg_daily_idle_waste"] * 30
                print(f"Historical Idle Waste: ${monthly_historical_waste:,.2f}/month (estimated)")

            consistency_indicators = []
            if historical_waste.get("idle_pattern_consistent", False):
                consistency_indicators.append("Consistent idle patterns detected")
            if historical_waste.get("utilization_pattern_consistent", False):
                consistency_indicators.append("Stable utilization patterns")

            if consistency_indicators:
                print(f"Pattern Analysis:      {', '.join(consistency_indicators)}")
            else:
                print(f"Pattern Analysis:      Variable usage patterns - monitor trends")

        # Node Pool Cost Analysis
        self._print_node_pool_cost_analysis()

        # Cost Optimization Opportunities
        print(f"\n\n💡 COST OPTIMIZATION OPPORTUNITIES")
        print("-" * 80)

        # Check if metrics are unreliable
        if optimization_opportunities.get("metrics_unreliable", False):
            print("⚠️  WARNING: Utilization metrics appear unreliable - optimization analysis limited")
            print("🔍 Issue: Most nodes showing 0% utilization, which suggests metrics collection problems")
            print("🛠️  Action: Please verify kubectl access, metrics server health, and cluster connectivity")
            print("\n📋 RECOMMENDED ACTIONS:")
            for rec in optimization_opportunities.get("recommendations", []):
                print(f"   • {rec.get('recommendation', 'No recommendation available')}")
                print(f"     Reason: {rec.get('rationale', 'No rationale provided')}")
            print("\n✋ Skipping utilization-based cost optimization until metrics are fixed")
            return

        scenarios = summary["optimization_scenarios"]

        if scenarios:
            for i, scenario in enumerate(scenarios[:5], 1):  # Show top 5 opportunities
                print(f"\n{i}. {scenario['name']}")
                print(f"   💰 Monthly Savings: ${scenario['monthly_savings']:,.2f}")
                print(f"   📝 Description: {scenario['description']}")
                print(f"   ⚠️  Impact: {scenario['impact']}")
                print(f"   🎯 Action: {scenario['action']}")

                # Show confidence and historical validation if available
                if scenario.get("confidence"):
                    confidence_icon = "🟢" if scenario["confidence"] == "High" else "🟡" if scenario["confidence"] == "Medium" else "🔴"
                    print(f"   📊 Confidence: {confidence_icon} {scenario['confidence']}")

                if scenario.get("historical_validation") and scenario["historical_validation"] != "low":
                    validation_icon = "✅" if scenario["historical_validation"] == "high" else "⚠️"
                    print(f"   📈 Historical Support: {validation_icon} {scenario['historical_validation'].title()}")
        else:
            print("✅ No major optimization opportunities identified")

        # Detailed Node Analysis
        utilization = self.get_node_utilization()
        efficiency = self.analyze_resource_efficiency(utilization)

        if "error" not in efficiency:
            detailed_analysis = self.get_detailed_node_analysis(efficiency)

            print(f"\n\n📊 DETAILED NODE ANALYSIS")
            print("-" * 80)

            # Show worst idle nodes
            worst_idle = detailed_analysis.get("worst_idle_nodes", [])
            if worst_idle:
                print(f"🔴 TOP 10 MOST IDLE NODES (CPU < 5% AND Memory < 10%) - Sorted by Idle Duration:")
                print(f"{'Node Name':<40} {'CPU':>6} {'Memory':>8} {'Age':>6} {'Idle':>8} {'Pool':>12} {'Action':>15}")
                print("-" * 110)
                for node in worst_idle[:10]:
                    cpu_val = node.get('cpu_percent_numeric', 0)
                    mem_val = node.get('memory_percent_numeric', 0)
                    age_display = node.get('age_display', '?')
                    idle_duration = node.get('idle_duration_display', '0h')
                    cpu_display = f"{cpu_val:.0f}%" if cpu_val is not None else "0%"
                    mem_display = f"{mem_val:.0f}%" if mem_val is not None else "0%"

                    # Determine action based on idle duration and utilization
                    idle_hours = node.get('idle_duration_hours', 0)
                    age_days = node.get('age_days', 0)

                    if idle_hours > 72:  # Idle for more than 3 days
                        action = "🚨 REMOVE NOW"
                    elif idle_hours > 24:  # Idle for more than 1 day
                        action = "⚠️ REMOVE SOON"
                    elif cpu_val <= 2 and mem_val <= 2:
                        action = "🚨 REMOVE" if age_days > 1 else "⚠️ SCALE DOWN"
                    elif cpu_val <= 5 and mem_val <= 5:
                        action = "⚠️ REMOVE SOON" if age_days > 7 else "🔍 INVESTIGATE"
                    else:
                        action = "🔍 INVESTIGATE"

                    node_pool_safe = node.get('node_pool') or 'unknown'
                    print(f"{node['name']:<40} {cpu_display:>6} {mem_display:>8} {age_display:>6} {idle_duration:>8} "
                          f"{node_pool_safe:>12} {action:>15}")

            # Show worst underutilized nodes
            worst_underutil = detailed_analysis.get("worst_underutilized_nodes", [])
            if worst_underutil:
                print(f"\n🟡 TOP 10 MOST UNDERUTILIZED NODES (CPU 5-15% OR Memory < 30%):")
                print(f"{'Node Name':<45} {'CPU':>6} {'Memory':>8} {'Age':>6} {'Pool':>15} {'Action':>15}")
                print("-" * 100)
                for node in worst_underutil[:10]:
                    cpu_val = node.get('cpu_percent_numeric', 0)
                    mem_val = node.get('memory_percent_numeric', 0)
                    age_display = node.get('age_display', '?')
                    cpu_display = f"{cpu_val:.0f}%" if cpu_val is not None else "0%"
                    mem_display = f"{mem_val:.0f}%" if mem_val is not None else "0%"

                    # Determine action based on utilization
                    if cpu_val <= 10 and mem_val <= 15:
                        action = "📉 RIGHT-SIZE"
                    elif cpu_val <= 15 and mem_val <= 20:
                        action = "📉 DOWNSIZE"
                    elif cpu_val <= 20 or mem_val <= 30:
                        action = "🔄 OPTIMIZE"
                    else:
                        action = "✅ MONITOR"

                    node_pool_safe = node.get('node_pool') or 'unknown'
                    print(f"{node['name']:<45} {cpu_display:>6} {mem_display:>8} {age_display:>6} "
                          f"{node_pool_safe:>15} {action:>15}")

            # Show analysis by node pool (quick summary - detailed cost analysis shown above)
            pool_analysis = detailed_analysis.get("pool_analysis", {})
            if pool_analysis:
                print(f"\n📈 NODE POOL WASTE SUMMARY (see detailed cost analysis above):")
                print(f"{'Pool Name':<20} {'Idle Nodes':>12} {'Underutil Nodes':>16} {'Total Waste':>12} {'Status':>15}")
                print("-" * 80)

                # Sort pools by total waste (descending)
                sorted_pools = sorted(pool_analysis.items(),
                                    key=lambda x: x[1]["total_waste"], reverse=True)

                for pool_name, data in sorted_pools[:15]:  # Show top 15 pools
                    idle_count = data["idle_nodes"]
                    underutil_count = data["underutilized_nodes"]
                    total_waste = data["total_waste"]

                    # Determine status
                    if total_waste >= 10:
                        status = "🚨 Critical"
                    elif total_waste >= 5:
                        status = "⚠️ High"
                    elif total_waste >= 2:
                        status = "🟡 Medium"
                    else:
                        status = "🟢 Low"

                    print(f"{pool_name:<20} {idle_count:>12} {underutil_count:>16} {total_waste:>12} {status:>15}")

            # Agent-only large nodes analysis
            agent_only_nodes = self.analyze_agent_only_large_nodes(efficiency)
            if agent_only_nodes:
                print(f"\n⚠️  LARGE NODES WITH ONLY AGENT WORKLOADS:")
                print(f"{'Node Name':<40} {'VM Size':<15} {'CPU':>6} {'Memory':>8} {'Age':>6} {'Pool':>12} {'Severity':>15}")
                print("-" * 120)

                for node in agent_only_nodes[:15]:  # Show top 15
                    cpu_display = f"{node['cpu_percent']:.0f}%" if node['cpu_percent'] is not None else "0%"
                    mem_display = f"{node['memory_percent']:.0f}%" if node['memory_percent'] is not None else "0%"

                    # Display severity based on pool appropriateness
                    if node.get('issue_severity') == 'CRITICAL_MISPLACEMENT':
                        severity_display = "🚨 RELOCATE"
                    elif node.get('issue_severity') == 'APPROPRIATE_POOL':
                        severity_display = "✅ OK"
                    elif node['issue_type'] == 'agent_only_large_node':
                        severity_display = "⚠️ AGENT-ONLY"
                    else:
                        severity_display = "❓ NO-PODS"

                    node_pool_safe = node.get('node_pool') or 'unknown'
                    print(f"{node['node_name']:<40} {node['vm_size']:<15} {cpu_display:>6} {mem_display:>8} "
                          f"{node['age_display']:>6} {node_pool_safe:>12} {severity_display:>15}")

                # Add severity-based summary
                critical_nodes = [n for n in agent_only_nodes if n.get('issue_severity') == 'CRITICAL_MISPLACEMENT']
                appropriate_nodes = [n for n in agent_only_nodes if n.get('issue_severity') == 'APPROPRIATE_POOL']
                unknown_nodes = [n for n in agent_only_nodes if n.get('issue_severity') == 'UNKNOWN_POOL']

                print(f"\n📊 Pool Placement Analysis:")
                if critical_nodes:
                    critical_cost = sum(n.get('estimated_monthly_cost', 0) for n in critical_nodes)
                    print(f"   🚨 Critical Misplacement: {len(critical_nodes)} nodes (${critical_cost:,.0f}/month)")
                    print(f"      → Relocate from compute pools to admin/server pools for cost savings")
                if appropriate_nodes:
                    appropriate_cost = sum(n.get('estimated_monthly_cost', 0) for n in appropriate_nodes)
                    print(f"   ✅ Appropriately Placed: {len(appropriate_nodes)} nodes (${appropriate_cost:,.0f}/month)")
                    print(f"      → Already on appropriate server/admin pools")
                if unknown_nodes:
                    unknown_cost = sum(n.get('estimated_monthly_cost', 0) for n in unknown_nodes)
                    print(f"   ❓ Unknown Pool Type: {len(unknown_nodes)} nodes (${unknown_cost:,.0f}/month)")
                    print(f"      → Review pool naming and placement strategy")

                print(f"\n💡 Agent-Only Large Node Details:")
                for i, node in enumerate(agent_only_nodes[:5], 1):  # Show details for top 5
                    print(f"   {i}. {node['node_name']} ({node['vm_size']})")
                    print(f"      Utilization: {node['cpu_percent']:.1f}% CPU, {node['memory_percent']:.1f}% Memory")
                    print(f"      Recommendation: {node['recommendation']}")
                    if node['pods']:
                        pod_names = [pod['name'] for pod in node['pods']]
                        print(f"      Running Pods: {', '.join(pod_names[:3])}" +
                              (f" (+{len(pod_names)-3} more)" if len(pod_names) > 3 else ""))
                    print()

        # Priority Recommendations
        print(f"\n\n🚀 PRIORITY RECOMMENDATIONS")
        print("-" * 80)
        recommendations = summary["recommendations"]

        high_priority = [r for r in recommendations if r.get("priority") == "High"]
        medium_priority = [r for r in recommendations if r.get("priority") == "Medium"]

        if high_priority:
            print("🔴 HIGH PRIORITY:")
            for rec in high_priority:
                print(f"   • {rec['recommendation']}")
                print(f"     Rationale: {rec['rationale']}")
                print(f"     Action: {rec['action']}")
                print()

        if medium_priority:
            print("🟡 MEDIUM PRIORITY:")
            for rec in medium_priority:
                print(f"   • {rec['recommendation']}")
                print(f"     Rationale: {rec['rationale']}")
                print()

        if not high_priority and not medium_priority:
            print("✅ No critical recommendations at this time")

        # Cost Trends
        cost_trends = summary["cost_trends"]
        if cost_trends["actual_monthly_cost"] > 0:
            print(f"\n\n📈 COST TRENDS")
            print("-" * 80)
            print(f"Actual Monthly Cost:        ${cost_trends['actual_monthly_cost']:,.2f}")
            print(f"Estimated vs Actual:        {cost_trends['estimate_vs_actual_variance']:+.1f}% variance")
            print(f"Cost Trend (30 days):       {cost_trends['cost_trend'].title()}")

        # Action Items Summary
        print(f"\n\n✅ IMMEDIATE ACTION ITEMS")
        print("-" * 80)
        action_items = []

        if waste_summary['idle_nodes'] > 0:
            action_items.append(f"Scale down or remove {waste_summary['idle_nodes']} idle nodes")

        if waste_summary['underutilized_nodes'] > 0:
            action_items.append(f"Right-size {waste_summary['underutilized_nodes']} underutilized nodes")

        if exec_summary['cost_efficiency_grade'] in ['D', 'F']:
            action_items.append("Review and optimize cluster configuration immediately")

        if not any(rec.get("category") == "Autoscaling" for rec in recommendations):
            action_items.append("Enable autoscaling on all appropriate node pools")

        if action_items:
            for i, item in enumerate(action_items, 1):
                print(f"{i}. {item}")
        else:
            print("✅ Cluster appears to be well-optimized!")

        print("\n" + "="*80)
        print("END OF COST OPTIMIZATION REPORT")
        print("="*80 + "\n")

    def list_idle_nodes(self):
        """List all idle nodes with detailed information."""
        print("\n" + "="*80)
        print("IDLE NODES LIST (CPU < 5% AND Memory < 10%)")
        print("="*80)

        utilization = self.get_node_utilization()
        efficiency = self.analyze_resource_efficiency(utilization)

        if "error" in efficiency:
            print("❌ Error: Could not get node utilization data")
            return

        idle_nodes = efficiency.get("idle_nodes", [])

        if not idle_nodes:
            print("✅ No idle nodes found!")
            return

        print(f"Found {len(idle_nodes)} idle nodes:\n")

        # Print header
        print(f"{'Node Name':<45} {'Pool':<15} {'VM Size':<18} {'CPU':<6} {'Mem':<6} {'Age':<8} {'Idle Duration':<12} {'Monthly Cost'}")
        print("-" * 125)

        total_monthly_cost = 0.0
        for node in idle_nodes:
            node_name = node.get("name", "")[:44]  # Truncate if too long
            pool = node.get("node_pool", "unknown")[:14]
            vm_size = self._get_vm_size_for_node(node_name)[:17]
            cpu = f"{node.get('cpu_percent_numeric', 0):.0f}%"
            memory = f"{node.get('memory_percent_numeric', 0):.0f}%"
            age = node.get("age_display", "?")
            idle_duration = node.get("idle_duration_display", "0h")
            monthly_cost = self._get_monthly_cost_for_node(node_name)
            total_monthly_cost += monthly_cost

            cost_display = f"${monthly_cost:,.0f}" if monthly_cost > 0 else "N/A"

            print(f"{node_name:<45} {pool:<15} {vm_size:<18} {cpu:<6} {memory:<6} {age:<8} {idle_duration:<12} {cost_display}")

        print(f"\nTotal idle nodes: {len(idle_nodes)}")
        print(f"💰 Total monthly cost of idle nodes: ${total_monthly_cost:,.2f}")
        print("💡 Consider removing these nodes to save costs.")

    def list_underutilized_nodes(self):
        """List all underutilized nodes with detailed information."""
        print("\n" + "="*80)
        print("UNDERUTILIZED NODES LIST (CPU 5-15% OR Memory < 30%)")
        print("="*80)

        utilization = self.get_node_utilization()
        efficiency = self.analyze_resource_efficiency(utilization)

        if "error" in efficiency:
            print("❌ Error: Could not get node utilization data")
            return

        underutilized_nodes = efficiency.get("underutilized_nodes", [])

        if not underutilized_nodes:
            print("✅ No underutilized nodes found!")
            return

        print(f"Found {len(underutilized_nodes)} underutilized nodes:\n")

        # Print header
        print(f"{'Node Name':<45} {'Pool':<15} {'VM Size':<18} {'CPU':<6} {'Mem':<6} {'Age':<8} {'Utilization':<11} {'Monthly Cost'}")
        print("-" * 125)

        total_monthly_cost = 0.0
        for node in underutilized_nodes:
            node_name = node.get("name", "")[:44]  # Truncate if too long
            pool = node.get("node_pool", "unknown")[:14]
            vm_size = self._get_vm_size_for_node(node_name)[:17]
            cpu = f"{node.get('cpu_percent_numeric', 0):.0f}%"
            memory = f"{node.get('memory_percent_numeric', 0):.0f}%"
            age = node.get("age_display", "?")
            utilization = f"{node.get('waste_level', 0):.0f}%"
            monthly_cost = self._get_monthly_cost_for_node(node_name)
            total_monthly_cost += monthly_cost

            cost_display = f"${monthly_cost:,.0f}" if monthly_cost > 0 else "N/A"

            print(f"{node_name:<45} {pool:<15} {vm_size:<18} {cpu:<6} {memory:<6} {age:<8} {utilization:<11} {cost_display}")

        print(f"\nTotal underutilized nodes: {len(underutilized_nodes)}")
        print(f"💰 Total monthly cost of underutilized nodes: ${total_monthly_cost:,.2f}")
        print("💡 Consider right-sizing these nodes to smaller VM sizes.")

    def _get_vm_size_for_node(self, node_name: str) -> str:
        """Get VM size for a specific node from cluster data."""
        if not self.cluster_data:
            return "unknown"

        # Extract pool name from node name
        node_pool = self._extract_node_pool_from_name(node_name)

        # Find VM size from cluster data
        for pool in self.cluster_data.get("agentPoolProfiles", []):
            if pool.get("name") == node_pool:
                return pool.get("vmSize", "unknown")

        return "unknown"

    def _get_monthly_cost_for_node(self, node_name: str) -> float:
        """Calculate estimated monthly cost for a specific node."""
        vm_size = self._get_vm_size_for_node(node_name)

        if vm_size == "unknown":
            return 0.0

        # VM pricing (same as in estimate_costs function)
        vm_pricing = {
            # D-series v4
            "Standard_D4ds_v4": 0.192,
            "Standard_D8ds_v4": 0.384,
            "Standard_D16ds_v4": 0.768,
            "Standard_D32ds_v4": 1.536,
            "Standard_D48ds_v4": 2.304,
            "Standard_D64ds_v4": 3.072,
            # D-series v5
            "Standard_D4ds_v5": 0.192,
            "Standard_D8ds_v5": 0.384,
            "Standard_D16ds_v5": 0.768,
            "Standard_D32ds_v5": 1.536,
            "Standard_D48ds_v5": 2.304,
            "Standard_D64ds_v5": 3.072,
            # E-series v5
            "Standard_E4ds_v5": 0.252,
            "Standard_E8ds_v5": 0.504,
            "Standard_E16ds_v5": 1.008,
            "Standard_E32ds_v5": 2.016,
            "Standard_E48ds_v5": 3.024,
            "Standard_E64ds_v5": 4.032,
            # L-series v3 (storage optimized)
            "Standard_L8s_v3": 0.832,
            "Standard_L16s_v3": 1.664,
            "Standard_L32s_v3": 3.328,
            "Standard_L48s_v3": 4.992,
            "Standard_L64s_v3": 6.656,
            # Roxie series
            "Standard_D32s_v5": 1.536,
            "Standard_D48s_v5": 2.304,
        }

        hourly_rate = vm_pricing.get(vm_size, 0.0)
        if hourly_rate == 0.0:
            return 0.0

        # Apply discounts
        discounted_hourly_rate = self._apply_discounts(hourly_rate, vm_size)

        # Calculate monthly cost (hourly * 24 hours * 30 days)
        monthly_cost = discounted_hourly_rate * 24 * 30

        return monthly_cost

    def print_report(self):
        """Generate and print comprehensive insights report."""
        print("\n" + "="*80)
        print(f"AKS CLUSTER INSIGHTS REPORT")
        print(f"Cluster: {self.cluster_name}")
        print(f"Subscription: {self.subscription}")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80 + "\n")

        # Cluster Overview
        print("CLUSTER OVERVIEW")
        print("-" * 80)
        if self.cluster_data:
            print(f"Resource Group:    {self.cluster_data.get('resourceGroup')}")
            print(f"Location:          {self.cluster_data.get('location')}")
            print(f"Kubernetes:        {self.cluster_data.get('kubernetesVersion')}")
            print(f"SKU:               {self.cluster_data.get('sku', {}).get('tier')}")
            print(f"Network Plugin:    {self.cluster_data.get('networkProfile', {}).get('networkPlugin')}")
            print(f"Network Policy:    {self.cluster_data.get('networkProfile', {}).get('networkPolicy')}")

        # Cluster Health
        print("\n\nCLUSTER HEALTH")
        print("-" * 80)
        health = self.get_cluster_health()
        for key, value in health.items():
            print(f"{key.replace('_', ' ').title():20} {value}")

        # Node Pools
        print("\n\nNODE POOLS")
        print("-" * 80)
        pools = self.get_node_pool_summary()
        for pool in pools:
            print(f"\nPool: {pool['name']}")
            print(f"  VM Size:        {pool['vm_size']}")
            print(f"  Current Nodes:  {pool['current_count']}")
            if pool['autoscaling']:
                print(f"  Autoscaling:    {pool['min_count']} - {pool['max_count']} nodes")
            print(f"  Mode:           {pool['mode']}")
            print(f"  Thor Cluster:   {pool['thor_cluster']}")
            print(f"  Purpose:        {pool['thor_purpose']}")
            if pool['thor_workers'] > 0:
                print(f"  Thor Workers:   {pool['thor_workers']}")
            print(f"  Description:    {pool['thor_description']}")
            print(f"  K8s Version:    {pool['orchestrator_version']}")
            print(f"  State:          {pool['provisioning_state']} / {pool['power_state']}")
            if pool['labels']:
                print(f"  Labels:         {', '.join([f'{k}={v}' for k, v in list(pool['labels'].items())[:3]])}")

        # Node Utilization & Efficiency Analysis
        print("\n\nNODE UTILIZATION & EFFICIENCY")
        print("-" * 80)
        utilization = self.get_node_utilization()
        if "error" not in utilization:
            efficiency = self.analyze_resource_efficiency(utilization)

            print(f"Total Nodes: {utilization.get('total_nodes', 0)}")

            if "error" not in efficiency:
                summary = efficiency["summary"]
                print(f"Efficiency Score: {summary['efficiency_score']:.1f}% (CPU: {summary['average_cpu_usage']:.1f}%, Memory: {summary['average_memory_usage']:.1f}%)")
                print(f"Well Utilized: {summary['well_utilized_nodes']} nodes")
                print(f"Underutilized: {summary['underutilized_nodes']} nodes")
                print(f"Idle: {summary['idle_nodes']} nodes")

            node_metrics = utilization.get("node_metrics", [])
            if node_metrics:
                print(f"\n{'Node Name':<50} {'CPU':>12} {'Memory':>12} {'Status':>15}")
                print("-" * 80)

                # Sort by utilization (lowest first to highlight issues)
                sorted_nodes = sorted(node_metrics,
                                    key=lambda x: x.get('cpu_percent_numeric', 0) + x.get('memory_percent_numeric', 0))

                for node in sorted_nodes[:10]:  # Show first 10 nodes
                    cpu_pct = node.get('cpu_percent_numeric', 0)
                    mem_pct = node.get('memory_percent_numeric', 0)

                    # Determine status
                    if cpu_pct <= 10 and mem_pct <= 10:
                        status = "🔴 IDLE"
                    elif cpu_pct <= 20 or mem_pct <= 30:
                        status = "🟡 UNDERUSED"
                    else:
                        status = "🟢 ACTIVE"

                    print(f"{node['name']:<50} {node['cpu_percent']:>12} {node['memory_percent']:>12} {status:>15}")

                if len(node_metrics) > 10:
                    print(f"... and {len(node_metrics) - 10} more nodes")
        else:
            print(f"Could not fetch utilization: {utilization['error']}")

        # Pod Statistics
        print("\n\nPOD STATISTICS")
        print("-" * 80)
        pod_stats = self.get_pod_statistics()
        if "error" not in pod_stats:
            print(f"Total Pods: {pod_stats.get('total_pods', 0)}")

            print("\nBy Phase:")
            for phase, count in pod_stats.get('by_phase', {}).items():
                print(f"  {phase:15} {count:5}")

            print("\nTop Namespaces by Pod Count:")
            namespaces = sorted(pod_stats.get('by_namespace', {}).items(),
                              key=lambda x: x[1], reverse=True)[:10]
            for ns, count in namespaces:
                print(f"  {ns:30} {count:5}")
        else:
            print(f"Could not fetch pod statistics: {pod_stats['error']}")

        # Cost Analysis with Optimization Insights
        print("\n\nCOST ANALYSIS & OPTIMIZATION")
        print("-" * 80)
        costs = self.estimate_costs()
        optimization_opportunities = self.calculate_cost_optimization_opportunities()

        if costs:
            print(f"Total Active Nodes:      {costs['total_nodes']}")
            print(f"Estimated Hourly Cost:   ${costs['estimated_hourly']:,.2f}")
            print(f"Estimated Daily Cost:    ${costs['estimated_daily']:,.2f}")
            print(f"Estimated Monthly Cost:  ${costs['estimated_monthly']:,.2f}")

            # Show optimization potential
            if optimization_opportunities.get("metrics_unreliable", False):
                print(f"⚠️  Metrics Unreliable:  Cannot calculate savings - fix kubectl/metrics first")
            elif optimization_opportunities.get('total_potential_savings', 0) > 0:
                savings = optimization_opportunities['total_potential_savings']
                savings_percent = (savings / costs['estimated_monthly']) * 100
                print(f"💰 Potential Savings:    ${savings:,.2f}/month ({savings_percent:.1f}%)")
                print(f"💰 Annual Savings:       ${savings * 12:,.2f}")

            if costs.get('warning'):
                print(f"\n⚠ WARNING: {costs['warning']}")

            print(f"\n{costs['note']}")

            print("\n\nCost Breakdown by Node Pool (Active Nodes Only):")
            print(f"{'Pool Name':<20} {'VM Size':<20} {'Nodes':>6} {'Max':>6} {'Hourly $':>10} {'Monthly $':>12}")
            print("-" * 80)

            active_pools = [p for p in costs.get('by_pool', []) if p['count'] > 0]

            for pool in active_pools:
                pricing_indicator = "" if pool['has_pricing'] else "*"
                print(f"{pool['pool_name']:<20} {pool['vm_size']:<20} {pool['count']:>6} {pool['max_count']:>6} "
                      f"${pool['hourly_cost']:>9,.2f} ${pool['monthly_cost']:>11,.2f}{pricing_indicator}")

            if active_pools:
                print("-" * 80)
                print(f"{'TOTAL':<48} ${costs['estimated_hourly']:>9,.2f} ${costs['estimated_monthly']:>11,.2f}")

            # Show potential max cost
            max_total_hourly = sum(p['hourly_cost'] / p['count'] * p['max_count']
                                  for p in costs.get('by_pool', [])
                                  if p['count'] > 0 and p['has_pricing'])
            if max_total_hourly > costs['estimated_hourly']:
                print(f"\n\nPotential Maximum Cost (if all pools scale to max):")
                print(f"  Max Hourly:   ${max_total_hourly:,.2f}")
                print(f"  Max Monthly:  ${max_total_hourly * 24 * 30:,.2f}")

            # Show idle pools count
            idle_pools = [p for p in costs.get('by_pool', []) if p['count'] == 0]
            if idle_pools:
                print(f"\n\nIdle Node Pools: {len(idle_pools)} pools with 0 nodes (not shown in costs)")

            # Show top optimization opportunities
            scenarios = optimization_opportunities.get('optimization_scenarios', [])
            if scenarios:
                print(f"\n\n🎯 TOP COST OPTIMIZATION OPPORTUNITIES:")
                for i, scenario in enumerate(scenarios[:3], 1):
                    print(f"  {i}. {scenario['name']}: ${scenario['monthly_savings']:,.2f}/month")
                    print(f"     {scenario['description']}")

                print(f"\n💡 Run with --cost-optimization for detailed analysis")

        # Show detailed node analysis if requested
        if hasattr(self, '_show_detailed_nodes') and self._show_detailed_nodes:
            self._print_detailed_node_analysis(utilization)

        print("\n" + "="*80)
        print("END OF REPORT")
        print("="*80 + "\n")

    def _print_detailed_node_analysis(self, utilization_data: Dict[str, Any]):
        """Print detailed analysis of idle and underutilized nodes."""
        if "error" in utilization_data:
            print("Could not analyze nodes - no utilization data available")
            return

        efficiency = self.analyze_resource_efficiency(utilization_data)
        if "error" in efficiency:
            print("Could not analyze node efficiency")
            return

        detailed_analysis = self.get_detailed_node_analysis(efficiency)

        print(f"\n\n📊 DETAILED NODE WASTE ANALYSIS")
        print("-" * 80)

        # Show summary
        summary = detailed_analysis.get("summary", {})
        print(f"Pools with idle nodes: {summary.get('pools_with_idle_nodes', 0)}")
        print(f"Pools with underutilized nodes: {summary.get('pools_with_underutilized_nodes', 0)}")
        print(f"Total pools affected: {summary.get('total_pools_affected', 0)}")

        # Show worst nodes
        worst_idle = detailed_analysis.get("worst_idle_nodes", [])
        if worst_idle:
            print(f"\n🔴 MOST WASTEFUL IDLE NODES (< 10% CPU & Memory) - By Priority:")
            print(f"{'Node Name':<45} {'CPU':>6} {'Memory':>8} {'Age':>6} {'Pool':>15} {'Recommendation':>15}")
            print("-" * 100)
            for node in worst_idle[:15]:
                cpu_val = node.get('cpu_percent_numeric', 0)
                mem_val = node.get('memory_percent_numeric', 0)
                age_display = node.get('age_display', '?')
                cpu_display = f"{cpu_val:.0f}%" if cpu_val is not None else "0%"
                mem_display = f"{mem_val:.0f}%" if mem_val is not None else "0%"

                # Provide specific recommendation based on age and utilization
                age_days = node.get('age_days', 0)
                if cpu_val <= 2 and mem_val <= 2 and age_days > 1:
                    recommendation = "🚨 Remove ASAP"
                elif cpu_val <= 2 and mem_val <= 2:
                    recommendation = "🚨 Remove Now"
                elif cpu_val <= 5 and mem_val <= 5 and age_days > 7:
                    recommendation = "⚠️ Remove Soon"
                elif cpu_val <= 5 and mem_val <= 5:
                    recommendation = "⚠️ Scale Down"
                else:
                    recommendation = "🔍 Investigate"

                node_pool_safe = node.get('node_pool') or 'unknown'
                print(f"{node['name']:<45} {cpu_display:>6} {mem_display:>8} {age_display:>6} "
                      f"{node_pool_safe:>15} {recommendation:>15}")

        worst_underutil = detailed_analysis.get("worst_underutilized_nodes", [])
        if worst_underutil:
            print(f"\n🟡 MOST WASTEFUL UNDERUTILIZED NODES (< 20% CPU OR < 30% Memory):")
            print(f"{'Node Name':<45} {'CPU':>6} {'Memory':>8} {'Age':>6} {'Pool':>15} {'Recommendation':>15}")
            print("-" * 100)
            for node in worst_underutil[:15]:
                cpu_val = node.get('cpu_percent_numeric', 0)
                mem_val = node.get('memory_percent_numeric', 0)
                age_display = node.get('age_display', '?')
                cpu_display = f"{cpu_val:.0f}%" if cpu_val is not None else "0%"
                mem_display = f"{mem_val:.0f}%" if mem_val is not None else "0%"

                # Provide specific recommendation
                if cpu_val <= 10 and mem_val <= 15:
                    recommendation = "📉 Right-size"
                elif cpu_val <= 15 or mem_val <= 25:
                    recommendation = "🔄 Optimize"
                else:
                    recommendation = "✅ Monitor"

                node_pool_safe = node.get('node_pool') or 'unknown'
                print(f"{node['name']:<45} {cpu_display:>6} {mem_display:>8} {age_display:>6} "
                      f"{node_pool_safe:>15} {recommendation:>15}")

        # Show pool analysis
        pool_analysis = detailed_analysis.get("pool_analysis", {})
        if pool_analysis:
            print(f"\n📈 WASTE BY NODE POOL:")
            print(f"{'Pool Name':<20} {'Idle':>8} {'Underutil':>12} {'Total':>8} {'Priority':>12}")
            print("-" * 65)

            sorted_pools = sorted(pool_analysis.items(),
                                key=lambda x: x[1]["total_waste"], reverse=True)

            for pool_name, data in sorted_pools:
                idle_count = data["idle_nodes"]
                underutil_count = data["underutilized_nodes"]
                total_waste = data["total_waste"]

                if total_waste >= 10:
                    priority = "🚨 Critical"
                elif total_waste >= 5:
                    priority = "⚠️ High"
                elif total_waste >= 2:
                    priority = "🟡 Medium"
                else:
                    priority = "🟢 Low"

                print(f"{pool_name:<20} {idle_count:>8} {underutil_count:>12} {total_waste:>8} {priority:>12}")

        # Add cluster job statistics if ECL Watch is configured
        if self.ecl_watch_url and self._ecl_watch_username_param:
            self.print_cluster_job_statistics()

    def print_cluster_job_statistics(self):
        """Print cluster job statistics report from ECL Watch."""
        print("\n" + "="*80)
        print("ECL WATCH CLUSTER JOB STATISTICS")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

        # Get cluster statistics
        stats_result = self.get_cluster_job_statistics()

        if "error" in stats_result:
            print(f"\n❌ Error: {stats_result['error']}")
            print("\nTo use this feature, ensure:")
            print("   • ECL Watch URL is provided (--ecl-watch-url or -w)")
            print("   • ECL Watch credentials are available:")
            print("     - Username via --ecl-watch-username or ECLWATCH_USERNAME")
            print("     - Password via --ecl-watch-password or ECLWATCH_PASSWORD")
            print("     - Or use interactive password prompt with username")
            return

        clusters = stats_result.get('clusters', {})
        summary = stats_result.get('summary', {})

        if not clusters:
            print("\n📊 No cluster data available from ECL Watch")
            print("   • Check ECL Watch URL and connectivity")
            print("   • Verify authentication credentials")
            print("   • Ensure ECL Watch is accessible and has recent job data")
            return

        # Print combined cluster job statistics table
        print(f"\n🏗️  CLUSTER JOB STATISTICS")
        print("-" * 145)
        print(f"{'Cluster Name':<20} {'Status':<14} {'Run':<4} {'Sub':<4} {'Comp':<4} {'Cmpd':<4} {'Que':<4} {'Failed':<8} {'Aborted':<8} {'Owners':<15} {'Last Job Time':<18}")
        print("-" * 145)

        # Sort clusters by activity level and then by running jobs
        activity_order = {"High Activity": 1, "Active": 2, "Light Activity": 3, "Recent Activity": 4, "Idle": 5}

        sorted_clusters = sorted(
            clusters.items(),
            key=lambda x: (
                activity_order.get(x[1]['activity_status'], 6),
                -x[1]['running_jobs'],
                -(x[1]['running_jobs'] + x[1]['submitted_jobs'] + x[1]['compiling_jobs'] + x[1]['compiled_jobs'] + x[1]['queued_jobs'])
            )
        )

        for cluster_name, cluster_data in sorted_clusters:
            # Truncate cluster name if too long
            display_name = cluster_name[:19] if len(cluster_name) > 19 else cluster_name

            # Format activity status with icon
            activity = cluster_data['activity_status']
            status_icon = "🔴" if activity == "High Activity" else "🟡" if activity == "Active" else "🟢" if activity in ["Light Activity", "Recent Activity"] else "⚪"
            status_display = f"{status_icon}{activity[:12]}"

            # Format owners (truncate if too long)
            if cluster_data['owners']:
                owners_str = ', '.join(cluster_data['owners'][:2])
                if len(cluster_data['owners']) > 2:
                    owners_str += f" +{len(cluster_data['owners']) - 2}"
                owners_display = owners_str[:14]
            else:
                owners_display = "none"

            # Extract timestamp from WUID if possible (WUIDs typically start with W followed by timestamp)
            last_job = cluster_data['last_job_wuid']
            last_job_display = "No recent jobs"
            if last_job:
                # Try to parse WUID timestamp (format usually like W20241015-123456)
                try:
                    if last_job.startswith('W') and '-' in last_job:
                        date_part = last_job[1:].split('-')[0]
                        time_part = last_job[1:].split('-')[1] if '-' in last_job[1:] else "000000"
                        if len(date_part) >= 8 and len(time_part) >= 6:
                            year = date_part[:4]
                            month = date_part[4:6]
                            day = date_part[6:8]
                            hour = time_part[:2]
                            minute = time_part[2:4]
                            last_job_display = f"{month}-{day} {hour}:{minute}"
                        else:
                            last_job_display = last_job[:17]
                    else:
                        last_job_display = last_job[:17]
                except:
                    last_job_display = last_job[:17]

            print(f"{display_name:<20} {status_display:<14} {cluster_data['running_jobs']:<4} {cluster_data['submitted_jobs']:<4} {cluster_data['compiling_jobs']:<4} {cluster_data['compiled_jobs']:<4} {cluster_data['queued_jobs']:<4} {cluster_data['failed_jobs_24h']:<8} {cluster_data['aborted_jobs_24h']:<8} {owners_display:<15} {last_job_display:<18}")

        if len(sorted_clusters) > 10:
            print(f"\n   ... showing top 10 clusters, {len(sorted_clusters) - 10} more available (use --detailed-nodes for full list)")

        # Print recommendations
        print(f"\n💡 WORKLOAD INSIGHTS")
        print("-" * 80)

        high_activity_clusters = [name for name, data in clusters.items() if data['activity_status'] == 'High Activity']
        active_clusters = [name for name, data in clusters.items() if data['activity_status'] == 'Active']
        idle_clusters = [name for name, data in clusters.items() if data['activity_status'] == 'Idle']

        if high_activity_clusters:
            print(f"🔴 High Activity Clusters ({len(high_activity_clusters)}): {', '.join(high_activity_clusters[:3])}")
            print("   → These clusters are actively processing jobs and should maintain current capacity")

        if active_clusters:
            print(f"🟡 Active Clusters ({len(active_clusters)}): {', '.join(active_clusters[:3])}")
            print("   → These clusters have moderate job activity - monitor for scaling opportunities")

        if idle_clusters:
            print(f"⚪ Idle Clusters ({len(idle_clusters)}): {', '.join(idle_clusters[:3])}")
            print("   → These clusters have no recent activity - potential candidates for scaling down")

        total_active_jobs = (summary.get('total_running_jobs', 0) +
                             summary.get('total_submitted_jobs', 0) +
                             summary.get('total_compiling_jobs', 0) +
                             summary.get('total_compiled_jobs', 0) +
                             summary.get('total_queued_jobs', 0))
        if total_active_jobs == 0:
            print(f"\n📊 Overall Status: No active jobs detected across all clusters")
            print("   → Consider scaling down idle resources during low-activity periods")
        elif total_active_jobs < 5:
            print(f"\n📊 Overall Status: Light workload activity ({total_active_jobs} active jobs)")
            print("   → Workload supports current resource allocation")
        else:
            print(f"\n📊 Overall Status: Active workload processing ({total_active_jobs} active jobs)")
            print("   → High activity justifies current resource allocation")

        print("\n" + "="*80)
        print("END OF CLUSTER JOB STATISTICS")
        print("="*80)

def main():
    parser = argparse.ArgumentParser(
        description="AKS Cluster Insights - Get detailed cluster utilization and cost optimization information"
    )
    parser.add_argument(
        "--cluster",
        required=True,
        help="AKS cluster name"
    )
    parser.add_argument(
        "--subscription",
        required=True,
        help="Azure subscription name or ID"
    )
    parser.add_argument(
        "--resource-group",
        help="Resource group name (auto-detected if not provided)"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format"
    )
    parser.add_argument(
        "--cost-optimization",
        action="store_true",
        help="Show focused cost optimization report with detailed savings analysis"
    )
    parser.add_argument(
        "--historical-days",
        type=int,
        default=7,
        help="Number of days to analyze for historical metrics (default: 7)"
    )
    parser.add_argument(
        "--detailed-nodes",
        action="store_true",
        help="Show detailed analysis of idle and underutilized nodes"
    )
    parser.add_argument(
        "--list-idle-nodes",
        action="store_true",
        help="List all idle nodes (CPU < 5%% AND Memory < 10%%) with detailed information"
    )
    parser.add_argument(
        "--list-underutilized-nodes",
        action="store_true",
        help="List all underutilized nodes (CPU 5-15%% OR Memory < 30%%) with detailed information"
    )
    parser.add_argument(
        "--enterprise-discount",
        type=float,
        default=0.0,
        help="Enterprise discount percentage (0-100). Applied to all VM costs. Default: 0"
    )
    parser.add_argument(
        "--d-series-discount",
        type=float,
        default=0.0,
        help="Alternative D-series specific discount percentage (0-100). For D-series VMs, uses the higher of enterprise or D-series discount (not stacked). Default: 0"
    )
    parser.add_argument(
        "--ecl-watch-url", "-w",
        type=str,
        help="ECL Watch URL (e.g., http://esp.example.com:8010) to analyze job activity and provide workload context for idle/underutilized node analysis"
    )
    parser.add_argument(
        "--ecl-watch-username", "-u",
        type=str,
        help="ECL Watch username for authentication. Alternatively, set ECLWATCH_USERNAME environment variable (recommended for security)"
    )
    parser.add_argument(
        "--ecl-watch-password",
        type=str,
        help="ECL Watch password for authentication. If not provided and username is given, password will be prompted interactively (recommended). NOT RECOMMENDED to use command line - use environment variable instead. WARNING: Command line passwords may be visible in process lists."
    )
    parser.add_argument(
        "--enable-timeout-protection",
        action="store_true",
        help="Enable timeout protection for Azure consumption API queries (30 second limit). By default, timeout protection is disabled to allow full cost data retrieval. Enable this if you experience hanging consumption API calls."
    )

    args = parser.parse_args()

    insights = AKSClusterInsights(
        cluster_name=args.cluster,
        subscription=args.subscription,
        resource_group=args.resource_group,
        enterprise_discount=args.enterprise_discount,
        d_series_discount=args.d_series_discount,
        ecl_watch_url=args.ecl_watch_url,
        ecl_watch_username=args.ecl_watch_username,
        ecl_watch_password=args.ecl_watch_password,
        enable_timeout_protection=args.enable_timeout_protection
    )

    try:
        insights.set_subscription()
        insights.get_cluster_details()

        # Display discount configuration and warnings
        insights.display_discount_info()

        if args.json:
            # Output JSON format with enhanced cost data
            utilization = insights.get_node_utilization()
            efficiency = insights.analyze_resource_efficiency(utilization)
            optimization = insights.calculate_cost_optimization_opportunities()

            report = {
                "cluster_name": args.cluster,
                "subscription": args.subscription,
                "timestamp": datetime.now().isoformat(),
                "cluster_overview": {
                    "resource_group": insights.cluster_data.get("resourceGroup"),
                    "location": insights.cluster_data.get("location"),
                    "kubernetes_version": insights.cluster_data.get("kubernetesVersion"),
                },
                "health": insights.get_cluster_health(),
                "node_pools": insights.get_node_pool_summary(),
                "utilization": utilization,
                "efficiency_analysis": efficiency,
                "pod_statistics": insights.get_pod_statistics(),
                "cost_estimates": insights.estimate_costs(),
                "optimization_opportunities": optimization,
                "cost_savings_summary": insights.generate_cost_savings_summary()
            }
            print(json.dumps(report, indent=2))
        # Check for specific node listing requests first (these can run alongside other reports)
        if args.list_idle_nodes:
            # List all idle nodes
            insights.list_idle_nodes()
        elif args.list_underutilized_nodes:
            # List all underutilized nodes
            insights.list_underutilized_nodes()
        elif args.cost_optimization:
            # Output focused cost optimization report
            insights.print_cost_optimization_summary()
        else:
            # Set detailed nodes flag if requested
            if args.detailed_nodes:
                insights._show_detailed_nodes = True
            # Output standard comprehensive report
            insights.print_report()

    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
