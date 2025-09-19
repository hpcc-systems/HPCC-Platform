#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use Pod::Usage;

# Function to show usage
sub show_usage {
    print <<EOF;
Usage: $0 -g RESOURCE_GROUP --name CLUSTER_NAME --namespace NAMESPACE [--show-pods] [--show-nodes] [--ea-discount PERCENT] [--ds_v4_family_discount PERCENT]

Required options:
  -g, --resource-group RESOURCE_GROUP   Azure resource group name
  --name, --cluster-name CLUSTER_NAME   AKS cluster name
  --namespace NAMESPACE                 Kubernetes namespace

Optional options:
  --show-pods                           Show pods in the output
  --show-nodes                          Show detailed node information with pods per node
  --ea-discount PERCENT                 Enterprise Agreement discount percentage (default: 10)
  --ds_v4_family_discount PERCENT       Dds_v4 family discount percentage for D16ds v4 and larger (default: 10)
  -h, --help                            Show this help message
  --help-detailed                       Show detailed help with column descriptions

Example:
  $0 -g app-hpcc-dev-eastus-73130 --name catest-aks-1 --namespace catest
  $0 --resource-group app-hpcc-dev-eastus-73130 --cluster-name catest-aks-1 --namespace catest --show-pods
  $0 -g app-hpcc-dev-eastus-73130 --name catest-aks-1 --namespace catest --show-nodes
  $0 -g app-hpcc-dev-eastus-73130 --name catest-aks-1 --namespace catest --ea-discount 25 --ds_v4_family_discount 75

EOF
}

# Function to show detailed help
sub show_detailed_help {
    print <<EOF;
HPCC AKS Utility - Detailed Help
================================

This script analyzes Azure Kubernetes Service (AKS) clusters running HPCC workloads,
providing resource utilization metrics, cost estimates, and pod details.

MAIN OUTPUT COLUMNS:
===================

Node Pool       - Name of the AKS node pool (e.g., thorpool0, miscpool0, roxiepool0)
NumNodes        - Number of active nodes in this pool
CPUs Requested  - Total CPU cores requested by pods in the specified namespace
CPU Capacity    - Total CPU cores available across all nodes in the pool
CPUs Allocated  - Total CPU cores allocatable to pods (slightly less than capacity due to system overhead)
Alloc(%)        - Percentage of total capacity that is allocatable (Allocated/Capacity * 100)
CPUs In Use     - Actual CPU cores currently being consumed by all workloads
InUse(%)        - Percentage of allocated CPU being used (In Use/Allocated * 100)
HPCC(%)         - Percentage of allocated CPU requested by HPCC components
Mem Alloc(Mi)   - Total memory allocated/available across all nodes in the pool (in MiB)
Mem Used(Mi)    - Total memory currently being used across all nodes in the pool (in MiB)
Mem(%)          - Percentage of memory currently being used across all nodes in the pool
Cost/Hr         - Estimated hourly cost for all nodes in this pool (USD, with applied discounts)

POD DETAILS (when --show-pods is used):
======================================

Pod Name        - Kubernetes pod name
Node            - Specific node where the pod is running
Phase           - Pod lifecycle phase (Running, Pending, Failed, etc.)
Status          - Pod readiness status (Ready/NotReady)
CPU Req         - CPU cores requested by this pod (affects scheduling)
Mem Req(Mi)     - Memory requested by this pod in MiB (affects scheduling)

NODE DETAILS (when --show-nodes is used):
========================================

Node Name       - Kubernetes node name within the pool
CPU Capacity    - Total CPU cores available on this specific node
CPU Allocatable - CPU cores available for pod scheduling (after system overhead)
CPU Used        - Actual CPU cores currently being consumed on this node
CPU(%)          - Percentage of allocatable CPU being used on this node
Mem Capacity(Mi)- Total memory available on this specific node (in MiB)
Mem Alloc(Mi)   - Memory available for pod scheduling (after system overhead, in MiB)
Mem Used(Mi)    - Actual memory currently being consumed on this node (in MiB)
Mem(%)          - Percentage of allocatable memory being used on this node
VM Size         - Azure VM instance type for this node

For each node, ALL pods running on that node are listed (from all namespaces), showing:
Namespace       - Kubernetes namespace the pod belongs to
Pod Name        - Kubernetes pod name
Phase           - Pod lifecycle phase (Running, Pending, Failed, etc.)
Status          - Pod readiness status (Ready/NotReady)
CPU Req         - CPU cores requested by this pod (affects scheduling)
Mem Req(Mi)     - Memory requested by this pod in MiB (affects scheduling)

COST CALCULATIONS:
=================

The script uses East US 2024 pricing as baseline and applies configurable discounts:

--ea-discount           Applied to most VM series (D-series v5, v4 smaller, E-series, F-series, B-series)
--ds_v4_family_discount Applied specifically to D16ds v4 and larger instances

Default discounts are 10% each. Costs are estimates and actual pricing may vary.

MESSAGES SECTION:
================

The script provides actionable insights:

- Resource Request Issues: Identifies pods without CPU requests (prevents proper scheduling)
- Pod Health: Reports pods that are not ready with reasons
- Thor Conflicts: Detects thor agents and workers/managers in same pool (prevents scaling to 0)
- Resource Waste: Identifies pools with no target namespace workloads but consuming CPU

UNDERSTANDING THE METRICS:
=========================

Key efficiency indicators:
- HPCC(%) shows how much of the pool is used by your target namespace
- InUse(%) shows actual CPU consumption vs. what's available
- High Alloc(%) with low InUse(%) may indicate over-provisioning
- Cost/Hr helps identify expensive pools that might be candidates for optimization

For optimal resource utilization:
- HPCC(%) should be high for dedicated pools
- Pods should have appropriate CPU/memory requests
- No thor conflicts should exist
- Consider scaling down pools with 0% HPCC but >0% InUse

EOF
}

# Initialize variables
my $resource_group = "";
my $cluster_name = "";
my $namespace = "";
my $show_pods = 0;
my $show_nodes = 0;
my $ea_discount = 10;
my $ds_v4_family_discount = 10;
my $help = 0;
my $help_detailed = 0;

# Parse command line arguments
GetOptions(
    'g|resource-group=s' => \$resource_group,
    'name|cluster-name=s' => \$cluster_name,
    'namespace=s' => \$namespace,
    'show-pods' => \$show_pods,
    'show-nodes' => \$show_nodes,
    'ea-discount=i' => \$ea_discount,
    'ds_v4_family_discount=i' => \$ds_v4_family_discount,
    'h|help' => \$help,
    'help-detailed' => \$help_detailed
) or do {
    print "Error: Unknown option\n";
    show_usage();
    exit 1;
};

# Show help if requested
if ($help) {
    show_usage();
    exit 0;
}

# Show detailed help if requested
if ($help_detailed) {
    show_detailed_help();
    exit 0;
}

# Validate required parameters
if (!$resource_group || !$cluster_name || !$namespace) {
    print "Error: Missing required parameters.\n\n";
    show_usage();
    exit 1;
}

# Function to get estimated hourly cost for Azure VM sizes (East US pricing as of 2024)
# Includes configurable Enterprise Agreement and Dds_v4 family discounts
sub get_vm_hourly_cost {
    my ($vm_size) = @_;
    my $ea_multiplier = (100 - $ea_discount) / 100;
    my $ds_v4_family_multiplier = (100 - $ds_v4_family_discount) / 100;
    
    my %vm_costs = (
        # Standard D-series v5 (EA discount applied)
        'Standard_D2s_v5' => 0.096 * $ea_multiplier,
        'Standard_D4s_v5' => 0.192 * $ea_multiplier,
        'Standard_D8s_v5' => 0.384 * $ea_multiplier,
        'Standard_D16s_v5' => 0.768 * $ea_multiplier,
        'Standard_D32s_v5' => 1.536 * $ea_multiplier,
        'Standard_D48s_v5' => 2.304 * $ea_multiplier,
        'Standard_D64s_v5' => 3.072 * $ea_multiplier,
        
        # Standard D-series v4 (smaller sizes - EA discount applied)
        'Standard_D2s_v4' => 0.096 * $ea_multiplier,
        'Standard_D4s_v4' => 0.192 * $ea_multiplier,
        'Standard_D8s_v4' => 0.384 * $ea_multiplier,
        
        # Standard Dds_v4 family (D16ds v4 and larger, Linux) - Dds_v4 family discount applied
        'Standard_D16s_v4' => 0.768 * $ds_v4_family_multiplier,
        'Standard_D32s_v4' => 1.536 * $ds_v4_family_multiplier,
        'Standard_D48s_v4' => 2.304 * $ds_v4_family_multiplier,
        'Standard_D64s_v4' => 3.072 * $ds_v4_family_multiplier,
        
        # Standard E-series v5 (memory optimized - EA discount applied)
        'Standard_E2s_v5' => 0.126 * $ea_multiplier,
        'Standard_E4s_v5' => 0.252 * $ea_multiplier,
        'Standard_E8s_v5' => 0.504 * $ea_multiplier,
        'Standard_E16s_v5' => 1.008 * $ea_multiplier,
        'Standard_E32s_v5' => 2.016 * $ea_multiplier,
        'Standard_E48s_v5' => 3.024 * $ea_multiplier,
        'Standard_E64s_v5' => 4.032 * $ea_multiplier,
        
        # Standard F-series v2 (compute optimized - EA discount applied)
        'Standard_F2s_v2' => 0.085 * $ea_multiplier,
        'Standard_F4s_v2' => 0.169 * $ea_multiplier,
        'Standard_F8s_v2' => 0.338 * $ea_multiplier,
        'Standard_F16s_v2' => 0.676 * $ea_multiplier,
        'Standard_F32s_v2' => 1.352 * $ea_multiplier,
        'Standard_F48s_v2' => 2.028 * $ea_multiplier,
        'Standard_F64s_v2' => 2.704 * $ea_multiplier,
        'Standard_F72s_v2' => 3.042 * $ea_multiplier,
        
        # Standard B-series (burstable - EA discount applied)
        'Standard_B2s' => 0.041 * $ea_multiplier,
        'Standard_B2ms' => 0.083 * $ea_multiplier,
        'Standard_B4ms' => 0.166 * $ea_multiplier,
        'Standard_B8ms' => 0.333 * $ea_multiplier,
        'Standard_B12ms' => 0.499 * $ea_multiplier,
        'Standard_B16ms' => 0.666 * $ea_multiplier,
        'Standard_B20ms' => 0.832 * $ea_multiplier
    );
    
    if (exists $vm_costs{$vm_size}) {
        return sprintf("%.3f", $vm_costs{$vm_size});
    } else {
        # Default fallback - estimate based on CPU count if available
        if ($vm_size =~ /(\d+)/) {
            my $cpu_estimate = $1;
            return sprintf("%.3f", $cpu_estimate * 0.048 * $ea_multiplier);
        } else {
            return sprintf("%.3f", 0.100 * $ea_multiplier);
        }
    }
}

# Function to parse CPU value (handle 'm' suffix for millicores)
sub parse_cpu {
    my ($cpu_raw) = @_;
    return 0 unless defined $cpu_raw && $cpu_raw ne '';
    
    if ($cpu_raw =~ /^(\d+(?:\.\d+)?)m$/) {
        return $1 / 1000;
    } elsif ($cpu_raw =~ /^(\d+(?:\.\d+)?)$/) {
        return $1;
    }
    return 0;
}

# Function to parse memory value (handle Ki suffix)
sub parse_memory {
    my ($mem_raw) = @_;
    return 0 unless defined $mem_raw && $mem_raw ne '';
    
    if ($mem_raw =~ /^(\d+(?:\.\d+)?)Ki$/) {
        return $1 / 1024;  # Convert to Mi
    } elsif ($mem_raw =~ /^(\d+(?:\.\d+)?)Mi$/) {
        return $1;
    }
    return 0;
}

# Function to execute system commands and capture output
sub execute_command {
    my ($cmd) = @_;
    my $output = `$cmd 2>/dev/null`;
    chomp $output;
    return $output;
}

# Get credentials for the AKS cluster
execute_command("az aks get-credentials --resource-group '$resource_group' --name '$cluster_name' --overwrite-existing");

# Get node pool names
my $node_pools_output = execute_command("az aks nodepool list --resource-group '$resource_group' --cluster-name '$cluster_name' --query '[].name' -o tsv");
my @node_pools = split(/\n/, $node_pools_output);

# Header
printf "%-15s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s\n", 
       "Node Pool", "NumNodes", "CPUs", "CPU", "CPUs", "CPUs", "CPUs", "CPUs", "CPUs", "Mem", "Mem", "Mem(%)", "Cost/Hr";
printf "%26s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s\n", 
       "", "Requested", "Capacity", "Allocated", "Alloc(%)", "In Use", "InUse(%)", "HPCC(%)", "Alloc(Mi)", "Used(Mi)", "Used%", "USD";
print "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";

# Array to store messages
my @messages = ();

foreach my $pool (@node_pools) {
    chomp $pool;
    next unless $pool;
    
    my $allocated_cpu = 0;
    my $in_use_cpu = 0;
    my $total_mem = 0;
    my $used_mem = 0;
    my $requested_cpu = 0;
    my $all_requested_cpu = 0;
    my $node_count = 0;
    my $capacity_cpu_total = 0;
    my @pod_details = ();  # Store detailed pod information
    my @node_details = (); # Store detailed node information
    my @not_ready_pods = ();
    my @thor_conflicts = ();

    # Get VM size for this node pool
    my $vm_size = execute_command("az aks nodepool show --resource-group '$resource_group' --cluster-name '$cluster_name' --name '$pool' --query 'vmSize' -o tsv");
    $vm_size = "Unknown" unless $vm_size;
    
    # Get hourly cost per VM
    my $vm_hourly_cost = get_vm_hourly_cost($vm_size);

    # Get all node data for this pool
    my $node_data = execute_command("kubectl get nodes -l agentpool=$pool -o jsonpath='{range .items[*]}{.metadata.name}{\" \"}{.status.allocatable.cpu}{\" \"}{.status.capacity.cpu}{\" \"}{.status.allocatable.memory}{\" \"}{.status.capacity.memory}{\"\\n\"}{end}'");
    
    # Get all usage data for this pool
    my $usage_data = execute_command("kubectl top nodes -l agentpool=$pool --no-headers");

    # Get pod data for namespace with detailed information
    my $pod_data = execute_command("kubectl get pods --namespace=$namespace --no-headers -o wide");
    
    # Get pod resource requests separately
    my $pod_resources = execute_command("kubectl get pods --namespace=$namespace -o json | jq -r '.items[] | \"\\(.metadata.name) \\(.spec.nodeName) \\([.spec.containers[].resources.requests.cpu // \"0\"] | join(\" \")) \\([.spec.containers[].resources.requests.memory // \"0Mi\"] | join(\" \"))'");

    # Get simplified pod data for conflict detection
    my $conflict_pod_data = execute_command("kubectl get pods --namespace=$namespace --no-headers -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName");

    # Get all pods data
    my $all_pod_data = execute_command("kubectl get pods --all-namespaces -o json | jq -r '.items[] | \"\\(.metadata.namespace) \\(.metadata.name) \\(.spec.nodeName) \\([.spec.containers[].resources.requests.cpu // \"0\"] | join(\" \"))'");

    # Get pod status data with basic info
    my $pod_status_data = execute_command("kubectl get pods --namespace=$namespace --no-headers -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName,PHASE:.status.phase,READY:.status.conditions[0].status");

    # Create hash to store pod resource data
    my %pod_resources_map = ();
    foreach my $resource_info (split(/\n/, $pod_resources)) {
        next unless $resource_info;
        my @res_fields = split(/\s+/, $resource_info);
        next unless @res_fields >= 2;
        
        my $pod_name = $res_fields[0];
        my $pod_node_name = $res_fields[1];
        
        my $total_cpu_req = 0;
        my $total_mem_req = 0;
        
        # Process CPU requests (field 2 and onward, space-separated)
        if (@res_fields >= 3 && $res_fields[2] ne "0") {
            my @cpu_requests = split(/\s+/, $res_fields[2]);
            foreach my $cpu_req (@cpu_requests) {
                next unless $cpu_req && $cpu_req ne "0";
                $total_cpu_req += parse_cpu($cpu_req);
            }
        }
        
        # Process memory requests (field 3 and onward if available)
        if (@res_fields >= 4 && $res_fields[3] ne "0Mi") {
            my @mem_requests = split(/\s+/, $res_fields[3]);
            foreach my $mem_req (@mem_requests) {
                next unless $mem_req && $mem_req ne "0Mi";
                $total_mem_req += parse_memory($mem_req);
            }
        }
        
        $pod_resources_map{$pod_name} = {
            cpu_req => $total_cpu_req,
            mem_req => $total_mem_req
        };
    }

    my @pool_nodes = ();
    
    # Process node data
    foreach my $node_info (split(/\n/, $node_data)) {
        next unless $node_info;
        
        my ($node_name, $cpu_alloc_raw, $cpu_capacity_raw, $mem_alloc_raw, $mem_capacity_raw) = split(/\s+/, $node_info);
        next unless $node_name;
        
        push @pool_nodes, $node_name;
        $node_count++;

        # Process allocatable and capacity CPU and memory
        my $cpu_alloc = parse_cpu($cpu_alloc_raw);
        my $cpu_capacity = parse_cpu($cpu_capacity_raw);
        my $mem_alloc_mi = parse_memory($mem_alloc_raw);
        my $mem_capacity_mi = parse_memory($mem_capacity_raw);

        # Get usage data for this specific node
        my $cpu_used = 0;
        my $mem_used = 0;
        foreach my $usage_line (split(/\n/, $usage_data)) {
            if ($usage_line =~ /\Q$node_name\E/) {
                my @fields = split(/\s+/, $usage_line);
                if (@fields >= 4) {
                    $cpu_used = parse_cpu($fields[1]);
                    $mem_used = parse_memory($fields[3]);
                    last;
                }
            }
        }

        $allocated_cpu += $cpu_alloc;
        $in_use_cpu += $cpu_used;
        $total_mem += $mem_alloc_mi;
        $used_mem += $mem_used;
        $capacity_cpu_total += $cpu_capacity;

        # Store node details for --show-nodes option
        my @node_pods = ();
        if ($show_nodes) {
            # Get all pods from all namespaces running on this specific node
            my $all_pods_on_node = execute_command("kubectl get pods --all-namespaces --no-headers -o wide --field-selector spec.nodeName=$node_name");
            
            # Get all pod resource requests for all namespaces
            my $all_pod_resources = execute_command("kubectl get pods --all-namespaces -o json | jq -r '.items[] | select(.spec.nodeName == \"$node_name\") | \"\\(.metadata.namespace) \\(.metadata.name) \\([.spec.containers[].resources.requests.cpu // \"0\"] | join(\" \")) \\([.spec.containers[].resources.requests.memory // \"0Mi\"] | join(\" \"))'");
            
            # Create a hash for all pod resources on this node
            my %all_pod_resources_map = ();
            foreach my $resource_info (split(/\n/, $all_pod_resources)) {
                next unless $resource_info;
                my @res_fields = split(/\s+/, $resource_info);
                next unless @res_fields >= 2;
                
                my $pod_namespace = $res_fields[0];
                my $pod_name = $res_fields[1];
                my $pod_key = "$pod_namespace/$pod_name";
                
                my $total_cpu_req = 0;
                my $total_mem_req = 0;
                
                # Process CPU requests
                if (@res_fields >= 3 && $res_fields[2] ne "0") {
                    my @cpu_requests = split(/\s+/, $res_fields[2]);
                    foreach my $cpu_req (@cpu_requests) {
                        next unless $cpu_req && $cpu_req ne "0";
                        $total_cpu_req += parse_cpu($cpu_req);
                    }
                }
                
                # Process memory requests
                if (@res_fields >= 4 && $res_fields[3] ne "0Mi") {
                    my @mem_requests = split(/\s+/, $res_fields[3]);
                    foreach my $mem_req (@mem_requests) {
                        next unless $mem_req && $mem_req ne "0Mi";
                        $total_mem_req += parse_memory($mem_req);
                    }
                }
                
                $all_pod_resources_map{$pod_key} = {
                    cpu_req => $total_cpu_req,
                    mem_req => $total_mem_req
                };
            }
            
            # Collect all pods for this specific node
            foreach my $pod_info (split(/\n/, $all_pods_on_node)) {
                next unless $pod_info;
                
                my @pod_fields = split(/\s+/, $pod_info);
                next unless @pod_fields >= 7;
                
                my $pod_namespace = $pod_fields[0];
                my $pod_name = $pod_fields[1];
                my $ready_count = $pod_fields[2] || "0/0";
                my $pod_phase = $pod_fields[3] || "Unknown";
                my $pod_key = "$pod_namespace/$pod_name";
                
                my $ready_status = ($ready_count =~ /(\d+)\/(\d+)/ && $1 eq $2 && $1 > 0) ? "Ready" : "NotReady";
                my $cpu_req = $all_pod_resources_map{$pod_key} ? $all_pod_resources_map{$pod_key}{cpu_req} : 0;
                my $mem_req = $all_pod_resources_map{$pod_key} ? $all_pod_resources_map{$pod_key}{mem_req} : 0;
                
                push @node_pods, {
                    namespace => $pod_namespace,
                    name => $pod_name,
                    phase => $pod_phase,
                    ready => $ready_status,
                    cpu_req => sprintf("%.3f", $cpu_req),
                    mem_req => sprintf("%.0f", $mem_req)
                };
            }
            
            # Calculate node-level CPU and memory percentages
            my $node_cpu_percent = $cpu_alloc > 0 ? sprintf("%.0f", ($cpu_used * 100 / $cpu_alloc)) : "0";
            my $node_mem_percent = $mem_alloc_mi > 0 ? sprintf("%.0f", ($mem_used * 100 / $mem_alloc_mi)) : "0";
            
            push @node_details, {
                name => $node_name,
                cpu_capacity => sprintf("%.1f", $cpu_capacity),
                cpu_allocatable => sprintf("%.1f", $cpu_alloc),
                cpu_used => sprintf("%.1f", $cpu_used),
                cpu_percent => "${node_cpu_percent}%",
                mem_capacity => sprintf("%.0f", $mem_capacity_mi),
                mem_allocatable => sprintf("%.0f", $mem_alloc_mi),
                mem_used => sprintf("%.0f", $mem_used),
                mem_percent => "${node_mem_percent}%",
                vm_size => $vm_size,
                pods => \@node_pods
            };
        }

        # Process pod resources for this node from the pre-built map
        foreach my $pod_name (keys %pod_resources_map) {
            # Check if this pod is on this node - we need to get node info from pod data
            my $pod_on_this_node = 0;
            foreach my $pod_info (split(/\n/, $pod_data)) {
                next unless $pod_info;
                my @pod_fields = split(/\s+/, $pod_info);
                next unless @pod_fields >= 7;
                
                my $check_pod_name = $pod_fields[0];
                my $pod_node_name = $pod_fields[6];
                
                if ($check_pod_name eq $pod_name && $pod_node_name eq $node_name) {
                    $pod_on_this_node = 1;
                    last;
                }
            }
            
            if ($pod_on_this_node) {
                $requested_cpu += $pod_resources_map{$pod_name}{cpu_req};
            }
        }
        
        # Process pods for this node
        foreach my $pod_info (split(/\n/, $pod_data)) {
            next unless $pod_info;
            
            my @pod_fields = split(/\s+/, $pod_info);
            next unless @pod_fields >= 2;
            
            my $pod_name = $pod_fields[0];
            my $pod_node_name = $pod_fields[6]; # Node is typically the 7th field in wide output
            next unless $pod_name && $pod_node_name;
            
            if ($pod_node_name eq $node_name) {
                # Store detailed pod information for later display
                if ($show_pods) {
                    my $pod_phase = $pod_fields[2] || "Unknown";
                    my $ready_count = $pod_fields[1] || "0/0";
                    my $ready_status = ($ready_count =~ /(\d+)\/(\d+)/ && $1 eq $2 && $1 > 0) ? "Ready" : "NotReady";
                    my $cpu_req = $pod_resources_map{$pod_name} ? $pod_resources_map{$pod_name}{cpu_req} : 0;
                    my $mem_req = $pod_resources_map{$pod_name} ? $pod_resources_map{$pod_name}{mem_req} : 0;
                    
                    push @pod_details, {
                        name => $pod_name,
                        node => $node_name,
                        phase => $pod_phase,
                        ready => $ready_status,
                        cpu_req => sprintf("%.3f", $cpu_req),
                        mem_req => sprintf("%.0f", $mem_req)
                    };
                }
            }
        }

        # Process ALL pods for resource usage
        foreach my $all_pod_info (split(/\n/, $all_pod_data)) {
            next unless $all_pod_info;
            
            my @all_pod_fields = split(/\s+/, $all_pod_info);
            next unless @all_pod_fields >= 3;
            
            my $pod_namespace = $all_pod_fields[0];
            my $pod_name = $all_pod_fields[1];
            my $pod_node_name = $all_pod_fields[2];
            
            if ($pod_node_name eq $node_name) {
                # Process CPU requests
                for my $i (3..$#all_pod_fields) {
                    my $req = $all_pod_fields[$i];
                    next unless $req;
                    my $req_cpu = parse_cpu($req);
                    $all_requested_cpu += $req_cpu;
                }
            }
        }

        # Check pod readiness status
        foreach my $status_info (split(/\n/, $pod_status_data)) {
            next unless $status_info;
            
            my @status_fields = split(/\s+/, $status_info);
            next unless @status_fields >= 4;
            
            my $pod_name = $status_fields[0];
            my $pod_node_name = $status_fields[1];
            my $pod_phase = $status_fields[2];
            my $ready_status = $status_fields[3];
            
            if ($pod_node_name eq $node_name && $ready_status ne "True") {
                my $reason_text = "";
                
                if ($pod_phase && $pod_phase ne "Running") {
                    $reason_text = "Pod phase: $pod_phase";
                }
                
                if (!$reason_text) {
                    $reason_text = "Ready status: $ready_status";
                }
                
                push @not_ready_pods, "$pod_name ($reason_text)";
            }
        }
    }

    # Check for thor worker/manager conflicts
    my @thor_agents_in_pool = ();
    my @thor_workers_managers_in_pool = ();
    
    foreach my $conflict_pod_info (split(/\n/, $conflict_pod_data)) {
        next unless $conflict_pod_info;
        
        my ($pod_name, $pod_node_name) = split(/\s+/, $conflict_pod_info);
        next unless $pod_name && $pod_node_name;
        
        # Check if pod is in this pool
        my $pod_in_this_pool = 0;
        foreach my $pool_node (@pool_nodes) {
            if ($pod_node_name eq $pool_node) {
                $pod_in_this_pool = 1;
                last;
            }
        }
        
        if ($pod_in_this_pool) {
            if ($pod_name =~ /eclagent|thoragent/) {
                push @thor_agents_in_pool, $pod_name;
            } elsif ($pod_name =~ /thorworker|thormanager/) {
                push @thor_workers_managers_in_pool, $pod_name;
            }
        }
    }
    
    # Check for conflicts
    if (@thor_agents_in_pool && @thor_workers_managers_in_pool) {
        push @thor_conflicts, "Thor agents and workers/managers running in same node pool - this prevents the node pool from scaling to 0";
    }

    # Calculate percentages
    my $cpu_util = $capacity_cpu_total > 0 ? sprintf("%.0f", ($allocated_cpu * 100 / $capacity_cpu_total)) : "0";
    my $mem_util = $total_mem > 0 ? sprintf("%.0f", ($used_mem * 100 / $total_mem)) : "0";
    my $cpu_inuse_percent = $allocated_cpu > 0 ? sprintf("%.0f", ($in_use_cpu * 100 / $allocated_cpu)) : "0";
    my $hpcc_percent = $allocated_cpu > 0 ? sprintf("%.0f", ($requested_cpu * 100 / $allocated_cpu)) : "0";

    # Generate messages
    if ($requested_cpu < $allocated_cpu) {
        push @messages, "$pool: Some pods lack CPU resource requests - this prevents proper scheduling and resource guarantees";
    }

    if (@not_ready_pods) {
        my $not_ready_list = join('; ', @not_ready_pods);
        push @messages, "$pool: Not ready pods: $not_ready_list";
    }

    if (@thor_conflicts) {
        foreach my $conflict (@thor_conflicts) {
            push @messages, "$pool: $conflict";
        }
    }

    # Check for wasted resources
    if ($pool =~ /(thorpool|miscpool|roxiepool|spraypool)/) {
        if ($hpcc_percent == 0 && $cpu_inuse_percent > 0) {
            my $pool_hourly_cost = sprintf("%.2f", $node_count * $vm_hourly_cost);
            push @messages, "$pool: This node pool has no active HPCC workloads but nodes are still consuming ${cpu_inuse_percent}% CPU. Consider scaling down or investigating system processes to reduce unnecessary costs (\$${pool_hourly_cost}/hr)";
        }
    }

    # Calculate total hourly cost for this pool
    my $total_hourly_cost = sprintf("%.2f", $node_count * $vm_hourly_cost);

    # Output results
    printf "%-15s %10s %10.1f %10.1f %10.1f %10s %10.1f %10s %10s %10.0f %10.0f %10s %10s\n", 
           $pool, $node_count, $requested_cpu, $capacity_cpu_total, $allocated_cpu, 
           "${cpu_util}%", $in_use_cpu, "${cpu_inuse_percent}%", "${hpcc_percent}%", 
           $total_mem, $used_mem, "${mem_util}%", "\$${total_hourly_cost}";
    
    # Display detailed pod information if requested
    if ($show_pods && @pod_details) {
        print "  Pods in $pool:\n";
        printf "    %-50s %-35s %-10s %-10s %-10s %-12s %-10s %-10s %-10s\n", 
               "Pod Name", "Node", "Phase", "Status", "CPU Req", "Mem Req(Mi)", "Mem Alloc(Mi)", "Mem Used(Mi)", "Mem(%)";
        print "    " . "-" x 170 . "\n";
        
        foreach my $pod (@pod_details) {
            # Get memory allocation and usage for the specific node this pod is on
            my $node_mem_alloc = 0;
            my $node_mem_used = 0;
            
            # Find the node's memory data
            foreach my $node_info (split(/\n/, $node_data)) {
                next unless $node_info;
                my ($node_name, $cpu_alloc_raw, $cpu_capacity_raw, $mem_alloc_raw) = split(/\s+/, $node_info);
                if ($node_name eq $pod->{node}) {
                    $node_mem_alloc = parse_memory($mem_alloc_raw);
                    
                    # Find usage for this node
                    foreach my $usage_line (split(/\n/, $usage_data)) {
                        if ($usage_line =~ /\Q$node_name\E/) {
                            my @fields = split(/\s+/, $usage_line);
                            if (@fields >= 4) {
                                $node_mem_used = parse_memory($fields[3]);
                                last;
                            }
                        }
                    }
                    last;
                }
            }
            
            my $node_mem_percent = $node_mem_alloc > 0 ? sprintf("%.0f", ($node_mem_used * 100 / $node_mem_alloc)) : "0";
            
            printf "    %-50s %-35s %-10s %-10s %-10s %-12s %-12.0f %-12.0f %-10s\n", 
                   $pod->{name}, $pod->{node}, $pod->{phase}, $pod->{ready}, 
                   $pod->{cpu_req}, $pod->{mem_req}, $node_mem_alloc, $node_mem_used, "${node_mem_percent}%";
        }
        print "\n";
    }
    
    # Display detailed node information if requested
    if ($show_nodes && @node_details) {
        print "  Nodes in $pool:\n";
        printf "    %-35s %-12s %-12s %-12s %-8s %-12s %-12s %-12s %-8s %-15s\n", 
               "Node Name", "CPU Capacity", "CPU Alloc", "CPU Used", "CPU(%)", "Mem Cap(Mi)", "Mem Alloc(Mi)", "Mem Used(Mi)", "Mem(%)", "VM Size";
        print "    " . "-" x 160 . "\n";
        
        foreach my $node (@node_details) {
            printf "    %-35s %-12s %-12s %-12s %-8s %-12s %-12s %-12s %-8s %-15s\n", 
                   $node->{name}, $node->{cpu_capacity}, $node->{cpu_allocatable}, 
                   $node->{cpu_used}, $node->{cpu_percent}, $node->{mem_capacity},
                   $node->{mem_allocatable}, $node->{mem_used}, $node->{mem_percent}, 
                   $node->{vm_size};
            
            # Show pods running on this node
            if (@{$node->{pods}}) {
                print "      Pods on this node (all namespaces):\n";
                printf "        %-20s %-40s %-10s %-10s %-10s %-12s\n", 
                       "Namespace", "Pod Name", "Phase", "Status", "CPU Req", "Mem Req(Mi)";
                print "        " . "-" x 100 . "\n";
                
                foreach my $pod (@{$node->{pods}}) {
                    printf "        %-20s %-40s %-10s %-10s %-10s %-12s\n", 
                           $pod->{namespace}, $pod->{name}, $pod->{phase}, $pod->{ready}, 
                           $pod->{cpu_req}, $pod->{mem_req};
                }
                print "\n";
            } else {
                print "      No pods running on this node.\n\n";
            }
        }
    }
}

# Display messages if any
if (@messages) {
    print "\n";
    print "Messages:\n";
    foreach my $message (@messages) {
        print "  - $message\n";
    }
}
