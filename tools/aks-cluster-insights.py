#!/usr/bin/env python3
"""
AKS Cluster Insights & Cost Optimization Script

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
- Calculates potential savings from right-sizing, spot instances, reserved instances
- Provides efficiency grading (A-F) for the cluster
- Suggests specific optimization actions with risk assessment

Usage Examples:
    # Standard comprehensive report
    python aks-cluster-insights.py --cluster my-cluster --subscription my-sub

    # Focused cost optimization report
    python aks-cluster-insights.py --cluster my-cluster --subscription my-sub --cost-optimization

    # JSON output for automation
    python aks-cluster-insights.py --cluster my-cluster --subscription my-sub --json

Prerequisites:
- Azure CLI installed and authenticated
- kubectl installed and configured
- Appropriate Azure permissions for the cluster and cost management
- Kubernetes metrics server enabled in the cluster
- Optional: Azure Cost Management extension (az extension add --name costmanagement) for detailed cost analysis

Author: Enhanced for cost optimization and resource efficiency analysis
Version: 2.0 - Cost Optimization Edition
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


class AKSClusterInsights:
    def __init__(self, cluster_name: str, subscription: str, resource_group: Optional[str] = None, 
                 enterprise_discount: float = 0.0, d_series_discount: float = 0.0):
        self.cluster_name = cluster_name
        self.subscription = subscription
        self.resource_group = resource_group
        self.cluster_data = None
        self.history_file = f".aks-idle-history-{cluster_name}.json"
        self.enterprise_discount = max(0.0, min(100.0, enterprise_discount))  # Clamp between 0-100
        self.d_series_discount = max(0.0, min(100.0, d_series_discount))  # Clamp between 0-100
        
    def run_az_command(self, command: List[str]) -> Dict[str, Any]:
        """Run Azure CLI command and return JSON output."""
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            return json.loads(result.stdout) if result.stdout else {}
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
            }
            summary.append(pool_info)
        
        return summary
    
    def get_node_utilization(self) -> Dict[str, Any]:
        """Get node utilization metrics using kubectl."""
        print("Fetching node utilization metrics...")
        
        # Get kubectl credentials
        subprocess.run([
            "az", "aks", "get-credentials",
            "--name", self.cluster_name,
            "--resource-group", self.resource_group,
            "--overwrite-existing"
        ], capture_output=True, check=True)
        
        try:
            # Get node metrics
            nodes_result = subprocess.run(
                ["kubectl", "get", "nodes", "-o", "json"],
                capture_output=True,
                text=True,
                check=True
            )
            nodes = json.loads(nodes_result.stdout)
            
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
            
            # Get top nodes
            top_result = subprocess.run(
                ["kubectl", "top", "nodes", "--no-headers"],
                capture_output=True,
                text=True
            )
            
            node_utilization = []
            if top_result.returncode == 0:
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
            
            return {
                "total_nodes": len(nodes.get("items", [])),
                "node_metrics": node_utilization
            }
        except Exception as e:
            print(f"Warning: Could not fetch node utilization: {e}", file=sys.stderr)
            return {"error": str(e)}
    
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
        
        IDLE_CPU_THRESHOLD = 10.0
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
        
        # Update idle duration tracking
        idle_durations = self.update_idle_tracking(utilization_data)
        
        node_metrics = utilization_data["node_metrics"]
        
        # Define thresholds for analysis
        LOW_CPU_THRESHOLD = 20.0  # CPU usage below this is considered underutilized
        LOW_MEMORY_THRESHOLD = 30.0  # Memory usage below this is considered underutilized
        IDLE_THRESHOLD = 10.0  # Both CPU and memory below this is considered idle
        
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
            if cpu_numeric <= IDLE_THRESHOLD and mem_numeric <= IDLE_THRESHOLD:
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
                if cpu_numeric <= IDLE_THRESHOLD and mem_numeric <= IDLE_THRESHOLD:
                    # Prioritize nodes that have been idle longer
                    idle_hours = idle_duration.get("total_hours", 0)
                    node["removal_priority"] = idle_hours * 100 + age_days * 10 + (10 - cpu_numeric - mem_numeric)
            else:
                node["idle_duration_hours"] = 0
                node["idle_duration_display"] = "0h"
                node["idle_since"] = None
            
            # Categorize nodes
            if cpu_numeric <= IDLE_THRESHOLD and mem_numeric <= IDLE_THRESHOLD:
                idle_nodes.append(node)
            elif cpu_numeric <= LOW_CPU_THRESHOLD or mem_numeric <= LOW_MEMORY_THRESHOLD:
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

        efficiency_analysis = {
            "summary": {
                "total_nodes": len(node_metrics),
                "idle_nodes": len(idle_nodes),
                "underutilized_nodes": len(underutilized_nodes),
                "well_utilized_nodes": len(well_utilized_nodes),
                "average_cpu_usage": round(avg_cpu, 1),
                "average_memory_usage": round(avg_memory, 1),
                "efficiency_score": round((avg_cpu + avg_memory) / 2, 1),
                "idle_duration_stats": idle_duration_stats
            },
            "idle_nodes": idle_nodes,
            "underutilized_nodes": underutilized_nodes,
            "well_utilized_nodes": well_utilized_nodes,
            "idle_durations": idle_durations,
            "thresholds": {
                "low_cpu": LOW_CPU_THRESHOLD,
                "low_memory": LOW_MEMORY_THRESHOLD,
                "idle": IDLE_THRESHOLD
            }
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
        """Calculate potential cost savings from various optimization strategies."""
        if not self.cluster_data:
            return {"error": "Cluster data not available"}
        
        node_pools = self.cluster_data.get("agentPoolProfiles", [])
        costs = self.estimate_costs()
        utilization = self.get_node_utilization()
        efficiency = self.analyze_resource_efficiency(utilization)
        
        if "error" in efficiency:
            return {"error": "Cannot calculate optimizations without utilization data"}
        
        opportunities = {
            "current_monthly_cost": costs.get("estimated_monthly", 0),
            "optimization_scenarios": [],
            "total_potential_savings": 0,
            "recommendations": []
        }
        
        # Scenario 1: Right-sizing based on utilization
        idle_nodes_count = efficiency["summary"]["idle_nodes"]
        underutilized_count = efficiency["summary"]["underutilized_nodes"]
        
        if idle_nodes_count > 0:
            # Calculate savings from removing idle nodes
            avg_node_cost = costs.get("estimated_monthly", 0) / max(costs.get("total_nodes", 1), 1)
            idle_cost_savings = idle_nodes_count * avg_node_cost
            
            opportunities["optimization_scenarios"].append({
                "name": "Remove Idle Nodes",
                "description": f"Remove {idle_nodes_count} idle nodes with <10% CPU and memory usage",
                "monthly_savings": round(idle_cost_savings, 2),
                "impact": "Low risk - nodes are barely used",
                "action": f"Scale down node pools or enable aggressive autoscaling"
            })
            opportunities["total_potential_savings"] += idle_cost_savings
        
        if underutilized_count > 0:
            # Calculate savings from downsizing underutilized nodes
            downsize_savings = underutilized_count * (costs.get("estimated_monthly", 0) / max(costs.get("total_nodes", 1), 1)) * 0.3  # 30% savings from smaller VMs
            
            opportunities["optimization_scenarios"].append({
                "name": "Downsize Underutilized Nodes",
                "description": f"Consider smaller VM sizes for {underutilized_count} underutilized nodes",
                "monthly_savings": round(downsize_savings, 2),
                "impact": "Medium risk - requires testing workload performance",
                "action": "Test workloads on smaller VM sizes (e.g., move from D8 to D4)"
            })
            opportunities["total_potential_savings"] += downsize_savings
        
        # Scenario 2: Spot instances opportunities
        system_pools = [p for p in node_pools if p.get("mode") == "System"]
        user_pools = [p for p in node_pools if p.get("mode") == "User"]
        
        if user_pools:
            user_pool_cost = sum(costs.get("by_pool", [{}])[i].get("monthly_cost", 0) 
                               for i, p in enumerate(node_pools) if p.get("mode") == "User")
            spot_savings = user_pool_cost * 0.7  # Up to 70% savings with spot instances
            
            opportunities["optimization_scenarios"].append({
                "name": "Implement Spot Instances",
                "description": f"Use spot instances for fault-tolerant user workloads",
                "monthly_savings": round(spot_savings, 2),
                "impact": "Medium risk - workloads must handle interruptions",
                "action": "Create spot node pools for batch jobs and stateless applications"
            })
        
        # Scenario 3: Reserved instances
        if costs.get("estimated_monthly", 0) > 500:  # Only recommend for significant costs
            reserved_savings = costs.get("estimated_monthly", 0) * 0.4  # Up to 40% savings
            
            opportunities["optimization_scenarios"].append({
                "name": "Reserved Instances",
                "description": "Purchase 1-year reserved instances for predictable workloads",
                "monthly_savings": round(reserved_savings, 2),
                "impact": "Low risk - requires commitment",
                "action": "Analyze workload patterns and purchase reservations for stable capacity"
            })
        
        # Generate specific recommendations
        opportunities["recommendations"] = self.generate_optimization_recommendations(
            efficiency, costs, node_pools
        )
        
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
        
        try:
            # Get all pods
            pods_result = subprocess.run(
                ["kubectl", "get", "pods", "--all-namespaces", "-o", "json"],
                capture_output=True,
                text=True,
                check=True
            )
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
        try:
            # Get all pods with node information
            pods_result = subprocess.run(
                ["kubectl", "get", "pods", "--all-namespaces", "-o", "json", 
                 "--field-selector=status.phase=Running"],
                capture_output=True,
                text=True,
                check=True
            )
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
        
        # Debug: Check if the specific node is in idle or underutilized lists
        target_node = "aks-thord48a1-13180010-vmss0004wg"
        found_in_idle = any(target_node in node.get("name", "") for node in idle_nodes)
        found_in_underutil = any(target_node in node.get("name", "") for node in underutilized_nodes)
        print(f"DEBUG: Target node {target_node} - In idle: {found_in_idle}, In underutilized: {found_in_underutil}", file=sys.stderr)
        
        # Debug: Track what we're analyzing  
        total_underutil = len(underutilized_nodes)
        total_idle = len(idle_nodes)
        large_nodes_found = 0
        
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
            
            if is_large_node:
                large_nodes_found += 1
            
            if not is_large_node:
                continue
            
            # Get pods running on this node
            node_pods = pods_by_node.get(node_name, [])
            
            # Debug specific node mentioned by user
            if "aks-thord48a1-13180010-vmss0004wg" in node_name:
                print(f"DEBUG: Analyzing specific node {node_name}", file=sys.stderr)
                print(f"DEBUG: VM size: {node_vm_size}, Large node: {is_large_node}", file=sys.stderr)
                print(f"DEBUG: Found {len(node_pods)} pods on this node", file=sys.stderr)
                for pod in node_pods:
                    print(f"DEBUG: Pod: {pod['pod_name']} (ns: {pod['namespace']}) containers: {pod['containers']}", file=sys.stderr)
            
            if not node_pods:
                # No pods or couldn't get pod info - still flag as potential issue
                pool_name = node.get("node_pool", "unknown")
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
        
        # Add debug info if no large nodes found
        if len(agent_only_large_nodes) == 0:
            print(f"Debug: Analyzed {total_underutil} underutilized + {total_idle} idle nodes, found {large_nodes_found} large nodes, 0 agent-only (filtered out calico-system and other system namespaces)", file=sys.stderr)
        
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
            "note": f"Estimates based on VM compute costs only (East US pricing). Discounts applied: Enterprise {self.enterprise_discount}%, D-series {self.d_series_discount}%. Does not include storage, networking, load balancers, or other Azure services."
        }
        
        if unknown_vm_sizes:
            result["warning"] = f"Pricing not available for VM sizes: {', '.join(sorted(unknown_vm_sizes))}"
        
        return result
    
    def _apply_discounts(self, hourly_rate: float, vm_size: str) -> float:
        """Apply enterprise and D-series discounts to VM hourly rate."""
        if hourly_rate == 0:
            return 0
        
        discounted_rate = hourly_rate
        
        # Apply enterprise discount first (applies to all VMs)
        if self.enterprise_discount > 0:
            discounted_rate = discounted_rate * (1 - self.enterprise_discount / 100)
        
        # Apply additional D-series discount if applicable
        if self.d_series_discount > 0 and vm_size and "D" in vm_size.upper():
            discounted_rate = discounted_rate * (1 - self.d_series_discount / 100)
        
        return discounted_rate
    
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
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        try:
            # Try to get usage details using az consumption
            usage_result = self.run_az_command([
                "az", "consumption", "usage", "list",
                "--start-date", start_date.strftime("%Y-%m-%d"),
                "--end-date", end_date.strftime("%Y-%m-%d"),
                "--top", "1000",
                "--include-additional-properties",
                "--include-meter-details",
                "-o", "json"
            ])
            
            # Process usage data to get costs by resource group
            cost_analysis = {
                "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                "total_cost": 0,
                "daily_costs": [],
                "service_breakdown": [],
                "cost_trend": "stable",
                "method": "consumption_api"
            }
            
            if usage_result and isinstance(usage_result, list):
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
        return {
            "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "total_cost": 0,
            "daily_costs": [],
            "service_breakdown": [],
            "cost_trend": "unknown",
            "method": "fallback",
            "error": "Cost data unavailable - requires Azure Cost Management permissions or extension"
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
        
        # Create summary
        summary = {
            "executive_summary": {
                "current_monthly_estimate": costs.get("estimated_monthly", 0),
                "total_nodes": costs.get("total_nodes", 0),
                "average_utilization": efficiency.get("summary", {}).get("efficiency_score", 0),
                "potential_monthly_savings": optimization_opportunities.get("total_potential_savings", 0),
                "cost_efficiency_grade": self._calculate_efficiency_grade(efficiency, costs)
            },
            "waste_identification": {
                "idle_nodes": efficiency.get("summary", {}).get("idle_nodes", 0),
                "underutilized_nodes": efficiency.get("summary", {}).get("underutilized_nodes", 0),
                "idle_cost_estimate": 0,
                "underutilized_cost_estimate": 0,
                "idle_duration_stats": efficiency.get("summary", {}).get("idle_duration_stats", {})
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
                efficiency.get("summary", {}).get("underutilized_nodes", 0) * avg_node_cost * 0.5
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
        
        print(f"💰 Current Monthly Cost:     ${exec_summary['current_monthly_estimate']:,.2f} (estimated)")
        print(f"📊 Cost Efficiency Grade:    {exec_summary['cost_efficiency_grade']}")
        print(f"⚡ Average Utilization:      {exec_summary['average_utilization']:.1f}%")
        print(f"🎯 Potential Monthly Savings: ${exec_summary['potential_monthly_savings']:,.2f}")
        print(f"📈 Annual Savings Potential:  ${exec_summary['potential_monthly_savings'] * 12:,.2f}")
        
        if exec_summary['potential_monthly_savings'] > 0:
            savings_percent = (exec_summary['potential_monthly_savings'] / exec_summary['current_monthly_estimate']) * 100
            print(f"📉 Cost Reduction Potential:  {savings_percent:.1f}%")
        
        # Display discount information if discounts are applied
        if costs.get('discount_info') and (costs['discount_info']['enterprise_discount'] > 0 or costs['discount_info']['d_series_discount'] > 0):
            print(f"\n💸 DISCOUNT INFORMATION")
            discount_info = costs['discount_info']
            if discount_info['enterprise_discount'] > 0:
                print(f"   Enterprise Discount:     {discount_info['enterprise_discount']:.1f}%")
            if discount_info['d_series_discount'] > 0:
                print(f"   D-Series Discount:       {discount_info['d_series_discount']:.1f}%")
            print(f"   Effective Total Discount: {discount_info['total_effective_discount']:.1f}%")
            print(f"   Monthly Savings from Discounts: ${costs.get('discount_amount_monthly', 0):,.2f}")
            print(f"   List Price (without discounts): ${costs.get('list_price_monthly', 0):,.2f}")
        
        # Waste Identification
        print(f"\n\n🗑️  RESOURCE WASTE ANALYSIS")
        print("-" * 80)
        print(f"Total Nodes:           {exec_summary['total_nodes']}")
        print(f"Idle Nodes:            {waste_summary['idle_nodes']} (wasting ~${waste_summary['idle_cost_estimate']:,.2f}/month)")
        print(f"Underutilized Nodes:   {waste_summary['underutilized_nodes']} (wasting ~${waste_summary['underutilized_cost_estimate']:,.2f}/month)")
        print(f"Efficiently Used:      {exec_summary['total_nodes'] - waste_summary['idle_nodes'] - waste_summary['underutilized_nodes']}")
        
        # Add idle duration statistics if available
        idle_duration_stats = waste_summary.get('idle_duration_stats', {})
        if idle_duration_stats:
            print(f"Avg Idle Duration:     {idle_duration_stats['avg_idle_hours']:.1f}h ({idle_duration_stats['nodes_with_duration']} nodes tracked)")
            if idle_duration_stats.get('max_idle_hours', 0) > 24:
                print(f"⚠️  Longest Idle:        {idle_duration_stats['max_idle_hours']:.1f}h ({idle_duration_stats['max_idle_hours']/24:.1f} days)")
        
        if exec_summary['total_nodes'] > 0:
            waste_percent = ((waste_summary['idle_nodes'] + waste_summary['underutilized_nodes']) / exec_summary['total_nodes']) * 100
            print(f"Resource Waste Rate:   {waste_percent:.1f}%")
        
        # Cost Optimization Opportunities
        print(f"\n\n💡 COST OPTIMIZATION OPPORTUNITIES")
        print("-" * 80)
        scenarios = summary["optimization_scenarios"]
        
        if scenarios:
            for i, scenario in enumerate(scenarios[:5], 1):  # Show top 5 opportunities
                print(f"\n{i}. {scenario['name']}")
                print(f"   💰 Monthly Savings: ${scenario['monthly_savings']:,.2f}")
                print(f"   📝 Description: {scenario['description']}")
                print(f"   ⚠️  Impact: {scenario['impact']}")
                print(f"   🎯 Action: {scenario['action']}")
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
                print(f"🔴 TOP 10 MOST IDLE NODES (CPU + Memory < 10%) - Sorted by Idle Duration:")
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
                    
                    print(f"{node['name']:<40} {cpu_display:>6} {mem_display:>8} {age_display:>6} {idle_duration:>8} "
                          f"{node.get('node_pool', 'unknown'):>12} {action:>15}")
            
            # Show worst underutilized nodes
            worst_underutil = detailed_analysis.get("worst_underutilized_nodes", [])
            if worst_underutil:
                print(f"\n🟡 TOP 10 MOST UNDERUTILIZED NODES (CPU < 20% OR Memory < 30%):")
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
                    
                    print(f"{node['name']:<45} {cpu_display:>6} {mem_display:>8} {age_display:>6} "
                          f"{node.get('node_pool', 'unknown'):>15} {action:>15}")
            
            # Show analysis by node pool
            pool_analysis = detailed_analysis.get("pool_analysis", {})
            if pool_analysis:
                print(f"\n📈 WASTE ANALYSIS BY NODE POOL:")
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
                    
                    print(f"{node['node_name']:<40} {node['vm_size']:<15} {cpu_display:>6} {mem_display:>8} "
                          f"{node['age_display']:>6} {node['node_pool']:>12} {severity_display:>15}")
                
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
            if optimization_opportunities.get('total_potential_savings', 0) > 0:
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
                
                print(f"{node['name']:<45} {cpu_display:>6} {mem_display:>8} {age_display:>6} "
                      f"{node.get('node_pool', 'unknown'):>15} {recommendation:>15}")
        
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
                
                print(f"{node['name']:<45} {cpu_display:>6} {mem_display:>8} {age_display:>6} "
                      f"{node.get('node_pool', 'unknown'):>15} {recommendation:>15}")
        
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
        "--enterprise-discount",
        type=float,
        default=0.0,
        help="Enterprise discount percentage (0-100). Applied to all VM costs. Default: 0"
    )
    parser.add_argument(
        "--d-series-discount", 
        type=float,
        default=0.0,
        help="Additional D-series specific discount percentage (0-100). Stacks with enterprise discount. Default: 0"
    )
    
    args = parser.parse_args()
    
    insights = AKSClusterInsights(
        cluster_name=args.cluster,
        subscription=args.subscription,
        resource_group=args.resource_group,
        enterprise_discount=args.enterprise_discount,
        d_series_discount=args.d_series_discount
    )
    
    try:
        insights.set_subscription()
        insights.get_cluster_details()
        
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
