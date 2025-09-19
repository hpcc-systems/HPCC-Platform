#!/bin/bash

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 -g RESOURCE_GROUP --name CLUSTER_NAME --namespace NAMESPACE [--show-pods] [--ea-discount PERCENT] [--ds_v4_family_discount PERCENT]

Required options:
  -g, --resource-group RESOURCE_GROUP   Azure resource group name
  --name, --cluster-name CLUSTER_NAME   AKS cluster name
  --namespace NAMESPACE                 Kubernetes namespace

Optional options:
  --show-pods                           Show pods in the output
  --ea-discount PERCENT                 Enterprise Agreement discount percentage (default: 10)
  --ds_v4_family_discount PERCENT       Dds_v4 family discount percentage for D16ds v4 and larger (default: 10)
  -h, --help                            Show this help message

Example:
  $0 -g app-hpcc-dev-eastus-73130 --name catest-aks-1 --namespace catest
  $0 --resource-group app-hpcc-dev-eastus-73130 --cluster-name catest-aks-1 --namespace catest --show-pods
  $0 -g app-hpcc-dev-eastus-73130 --name catest-aks-1 --namespace catest --ea-discount 25 --ds_v4_family_discount 75

EOF
}

# Initialize variables
RESOURCE_GROUP=""
CLUSTER_NAME=""
NAMESPACE=""
SHOW_PODS=false
EA_DISCOUNT=10
DS_V4_FAMILY_DISCOUNT=10

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -g|--resource-group)
            RESOURCE_GROUP="$2"
            shift 2
            ;;
        --name|--cluster-name)
            CLUSTER_NAME="$2"
            shift 2
            ;;
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --show-pods)
            SHOW_PODS=true
            shift
            ;;
        --ea-discount)
            EA_DISCOUNT="$2"
            shift 2
            ;;
        --ds_v4_family_discount)
            DS_V4_FAMILY_DISCOUNT="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Error: Unknown option $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$RESOURCE_GROUP" || -z "$CLUSTER_NAME" || -z "$NAMESPACE" ]]; then
    echo "Error: Missing required parameters."
    echo ""
    show_usage
    exit 1
fi

# Get credentials for the AKS cluster
az aks get-credentials --resource-group "$RESOURCE_GROUP" --name "$CLUSTER_NAME" --overwrite-existing

# Get node pool names and VM sizes
NODE_POOLS=$(az aks nodepool list --resource-group "$RESOURCE_GROUP" --cluster-name "$CLUSTER_NAME" --query "[].name" -o tsv)

# Function to get estimated hourly cost for Azure VM sizes (East US pricing as of 2024)
# Includes configurable Enterprise Agreement and Dds_v4 family discounts
get_vm_hourly_cost() {
    local vm_size="$1"
    local ea_multiplier=$(echo "scale=4; (100 - $EA_DISCOUNT) / 100" | bc -l)
    local ds_v4_family_multiplier=$(echo "scale=4; (100 - $DS_V4_FAMILY_DISCOUNT) / 100" | bc -l)
    
    case "$vm_size" in
        # Standard D-series v5 (EA discount applied)
        "Standard_D2s_v5") echo "scale=3; 0.096 * $ea_multiplier" | bc -l ;;
        "Standard_D4s_v5") echo "scale=3; 0.192 * $ea_multiplier" | bc -l ;;
        "Standard_D8s_v5") echo "scale=3; 0.384 * $ea_multiplier" | bc -l ;;
        "Standard_D16s_v5") echo "scale=3; 0.768 * $ea_multiplier" | bc -l ;;
        "Standard_D32s_v5") echo "scale=3; 1.536 * $ea_multiplier" | bc -l ;;
        "Standard_D48s_v5") echo "scale=3; 2.304 * $ea_multiplier" | bc -l ;;
        "Standard_D64s_v5") echo "scale=3; 3.072 * $ea_multiplier" | bc -l ;;
        
        # Standard D-series v4 (smaller sizes - EA discount applied)
        "Standard_D2s_v4") echo "scale=3; 0.096 * $ea_multiplier" | bc -l ;;
        "Standard_D4s_v4") echo "scale=3; 0.192 * $ea_multiplier" | bc -l ;;
        "Standard_D8s_v4") echo "scale=3; 0.384 * $ea_multiplier" | bc -l ;;
        
        # Standard Dds_v4 family (D16ds v4 and larger, Linux) - Dds_v4 family discount applied
        "Standard_D16s_v4") echo "scale=3; 0.768 * $ds_v4_family_multiplier" | bc -l ;;
        "Standard_D32s_v4") echo "scale=3; 1.536 * $ds_v4_family_multiplier" | bc -l ;;
        "Standard_D48s_v4") echo "scale=3; 2.304 * $ds_v4_family_multiplier" | bc -l ;;
        "Standard_D64s_v4") echo "scale=3; 3.072 * $ds_v4_family_multiplier" | bc -l ;;
        
        # Standard E-series v5 (memory optimized - EA discount applied)
        "Standard_E2s_v5") echo "scale=3; 0.126 * $ea_multiplier" | bc -l ;;
        "Standard_E4s_v5") echo "scale=3; 0.252 * $ea_multiplier" | bc -l ;;
        "Standard_E8s_v5") echo "scale=3; 0.504 * $ea_multiplier" | bc -l ;;
        "Standard_E16s_v5") echo "scale=3; 1.008 * $ea_multiplier" | bc -l ;;
        "Standard_E32s_v5") echo "scale=3; 2.016 * $ea_multiplier" | bc -l ;;
        "Standard_E48s_v5") echo "scale=3; 3.024 * $ea_multiplier" | bc -l ;;
        "Standard_E64s_v5") echo "scale=3; 4.032 * $ea_multiplier" | bc -l ;;
        
        # Standard F-series v2 (compute optimized - EA discount applied)
        "Standard_F2s_v2") echo "scale=3; 0.085 * $ea_multiplier" | bc -l ;;
        "Standard_F4s_v2") echo "scale=3; 0.169 * $ea_multiplier" | bc -l ;;
        "Standard_F8s_v2") echo "scale=3; 0.338 * $ea_multiplier" | bc -l ;;
        "Standard_F16s_v2") echo "scale=3; 0.676 * $ea_multiplier" | bc -l ;;
        "Standard_F32s_v2") echo "scale=3; 1.352 * $ea_multiplier" | bc -l ;;
        "Standard_F48s_v2") echo "scale=3; 2.028 * $ea_multiplier" | bc -l ;;
        "Standard_F64s_v2") echo "scale=3; 2.704 * $ea_multiplier" | bc -l ;;
        "Standard_F72s_v2") echo "scale=3; 3.042 * $ea_multiplier" | bc -l ;;
        
        # Standard B-series (burstable - EA discount applied)
        "Standard_B2s") echo "scale=3; 0.041 * $ea_multiplier" | bc -l ;;
        "Standard_B2ms") echo "scale=3; 0.083 * $ea_multiplier" | bc -l ;;
        "Standard_B4ms") echo "scale=3; 0.166 * $ea_multiplier" | bc -l ;;
        "Standard_B8ms") echo "scale=3; 0.333 * $ea_multiplier" | bc -l ;;
        "Standard_B12ms") echo "scale=3; 0.499 * $ea_multiplier" | bc -l ;;
        "Standard_B16ms") echo "scale=3; 0.666 * $ea_multiplier" | bc -l ;;
        "Standard_B20ms") echo "scale=3; 0.832 * $ea_multiplier" | bc -l ;;
        
        # Default fallback - estimate based on CPU count if available (EA discount applied)
        *) 
            # Try to extract CPU count from VM size name and estimate
            if [[ "$vm_size" =~ ([0-9]+) ]]; then
                local cpu_estimate="${BASH_REMATCH[1]}"
                echo "scale=3; $cpu_estimate * 0.048 * $ea_multiplier" | bc -l  # ~$0.048 per vCPU base estimate
            else
                echo "scale=3; 0.100 * $ea_multiplier" | bc -l  # Default base estimate
            fi
            ;;
    esac
}

# Header
if $SHOW_PODS; then
    printf "%-15s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %-s\n" "Node Pool" "NumNodes" "CPUs" "CPU" "CPUs" "CPUs" "CPUs" "CPUs" "CPUs" "Mem(%)" "Cost/Hr" "Pods ($NAMESPACE)"
else
    printf "%-15s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s\n" "Node Pool" "NumNodes" "CPUs" "CPU" "CPUs" "CPUs" "CPUs" "CPUs" "CPUs" "Mem(%)" "Cost/Hr"
fi
printf "%26s %10s %10s %10s %10s %10s %10s %10s %10s\n" "" "Requested" "Capacity" "Allocated" "Alloc(%)" "In Use" "InUse(%)" "HPCC(%)" "USD"
echo "------------------------------------------------------------------------------------------------------------------------------------------------------"

# Array to store messages
MESSAGES=()

for POOL in $NODE_POOLS; do
    ALLOCATED_CPU=0
    IN_USE_CPU=0
    TOTAL_MEM=0
    USED_MEM=0
    REQUESTED_CPU=0
    ALL_REQUESTED_CPU=0
    NODE_COUNT=0
    CAPACITY_CPU_TOTAL=0
    POD_LIST=()
    NOT_READY_PODS=()
    THOR_CONFLICTS=()

    # Get VM size for this node pool
    VM_SIZE=$(az aks nodepool show --resource-group "$RESOURCE_GROUP" --cluster-name "$CLUSTER_NAME" --name "$POOL" --query "vmSize" -o tsv 2>/dev/null || echo "Unknown")
    
    # Get hourly cost per VM
    VM_HOURLY_COST=$(get_vm_hourly_cost "$VM_SIZE")

    # Get all node data for this pool in a single call
    NODE_DATA=$(kubectl get nodes -l agentpool=$POOL -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.allocatable.cpu}{" "}{.status.capacity.cpu}{" "}{.status.allocatable.memory}{"\n"}{end}')
    # Get all usage data for this pool in a single call
    USAGE_DATA=$(kubectl top nodes -l agentpool=$POOL --no-headers)

    # Get all pods in catest namespace with their node assignments and CPU requests in a single call
    POD_DATA=$(kubectl get pods --namespace=$NAMESPACE -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.nodeName}{" "}{range .spec.containers[*]}{.resources.requests.cpu}{" "}{end}{"\n"}{end}')

    # Get simplified pod data for conflict detection (just pod name and node)
    CONFLICT_POD_DATA=$(kubectl get pods --namespace=$NAMESPACE -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.nodeName}{"\n"}{end}')

    # Get all pods in ALL namespaces with their node assignments and CPU requests in a single call
    ALL_POD_DATA=$(kubectl get pods --all-namespaces -o jsonpath='{range .items[*]}{.metadata.namespace}{" "}{.metadata.name}{" "}{.spec.nodeName}{" "}{range .spec.containers[*]}{.resources.requests.cpu}{" "}{end}{"\n"}{end}')

    # Get pod readiness status and container status for catest namespace
    POD_STATUS_DATA=$(kubectl get pods --namespace=$NAMESPACE -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.nodeName}{" "}{.status.phase}{" "}{.status.conditions[?(@.type=="Ready")].status}{" "}{range .status.containerStatuses[*]}{.state.waiting.reason}{","}{.state.terminated.reason}{","}{end}{range .status.initContainerStatuses[*]}{.state.waiting.reason}{","}{.state.terminated.reason}{","}{end}{"\n"}{end}')

    while IFS= read -r node_info; do
        if [[ -n "$node_info" ]]; then
            read -r NODE_NAME CPU_ALLOC_RAW CPU_CAPACITY_RAW MEM_ALLOC_RAW <<< "$node_info"
            NODE_COUNT=$((NODE_COUNT + 1))

            # Process allocatable CPU
            if echo "$CPU_ALLOC_RAW" | grep -q 'm$'; then
                CPU_ALLOC=$(echo "$CPU_ALLOC_RAW" | sed 's/m//' | awk '{print $1 / 1000}')
            else
                CPU_ALLOC="$CPU_ALLOC_RAW"
            fi

            # Process capacity CPU
            if echo "$CPU_CAPACITY_RAW" | grep -q 'm$'; then
                CPU_CAPACITY=$(echo "$CPU_CAPACITY_RAW" | sed 's/m//' | awk '{print $1 / 1000}')
            else
                CPU_CAPACITY="$CPU_CAPACITY_RAW"
            fi

            # Process allocatable memory
            MEM_ALLOC_MI=$(echo "$MEM_ALLOC_RAW" | sed 's/Ki//' | awk '{print $1 / 1024}')

            # Get usage data for this specific node from the usage data
            CPU_USED=0
            MEM_USED=0
            while IFS= read -r usage_line; do
                if [[ "$usage_line" == *"$NODE_NAME"* ]]; then
                    CPU_USED_RAW=$(echo "$usage_line" | awk '{print $2}')
                    MEM_USED_RAW=$(echo "$usage_line" | awk '{print $4}')
                    
                    if echo "$CPU_USED_RAW" | grep -q 'm$'; then
                        CPU_USED=$(echo "$CPU_USED_RAW" | sed 's/m//' | awk '{print $1 / 1000}')
                    else
                        CPU_USED="$CPU_USED_RAW"
                    fi
                    
                    MEM_USED=$(echo "$MEM_USED_RAW" | sed 's/Mi//')
                    break
                fi
            done <<< "$USAGE_DATA"

            ALLOCATED_CPU=$(echo "$ALLOCATED_CPU + $CPU_ALLOC" | bc -l)
            IN_USE_CPU=$(echo "$IN_USE_CPU + $CPU_USED" | bc -l)
            TOTAL_MEM=$(echo "$TOTAL_MEM + $MEM_ALLOC_MI" | bc -l)
            USED_MEM=$(echo "$USED_MEM + $MEM_USED" | bc -l)
            CAPACITY_CPU_TOTAL=$(echo "$CAPACITY_CPU_TOTAL + $CPU_CAPACITY" | bc -l)

            # Process pods for this node from the pre-fetched pod data
            while IFS= read -r pod_info; do
                if [[ -n "$pod_info" ]]; then
                    read -r POD_NAME POD_NODE_NAME CPU_REQUESTS <<< "$pod_info"
                    
                    # Only process pods that are on this specific node
                    if [[ "$POD_NODE_NAME" == "$NODE_NAME" ]]; then
                        # Process CPU requests for this pod
                        for req in $CPU_REQUESTS; do
                            if [[ -n "$req" ]]; then
                                if echo "$req" | grep -q 'm$'; then
                                    REQ_CPU=$(echo "$req" | sed 's/m//' | awk '{print $1 / 1000}')
                                else
                                    REQ_CPU="$req"
                                fi
                                REQUESTED_CPU=$(echo "$REQUESTED_CPU + $REQ_CPU" | bc -l)
                            fi
                        done
                        
                        if $SHOW_PODS; then
                            POD_LIST+=("$POD_NAME")
                        fi
                    fi
                fi
            done <<< "$POD_DATA"

            # Process ALL pods for this node to get total resource usage
            while IFS= read -r all_pod_info; do
                if [[ -n "$all_pod_info" ]]; then
                    read -r POD_NAMESPACE POD_NAME POD_NODE_NAME CPU_REQUESTS <<< "$all_pod_info"
                    
                    # Only process pods that are on this specific node
                    if [[ "$POD_NODE_NAME" == "$NODE_NAME" ]]; then
                        # Process CPU requests for this pod
                        for req in $CPU_REQUESTS; do
                            if [[ -n "$req" ]]; then
                                if echo "$req" | grep -q 'm$'; then
                                    REQ_CPU=$(echo "$req" | sed 's/m//' | awk '{print $1 / 1000}')
                                else
                                    REQ_CPU="$req"
                                fi
                                ALL_REQUESTED_CPU=$(echo "$ALL_REQUESTED_CPU + $REQ_CPU" | bc -l)
                            fi
                        done
                    fi
                fi
            done <<< "$ALL_POD_DATA"

            # Check pod readiness status for this node
            while IFS= read -r status_info; do
                if [[ -n "$status_info" ]]; then
                    read -r POD_NAME POD_NODE_NAME POD_PHASE READY_STATUS CONTAINER_REASONS <<< "$status_info"
                    
                    # Only process pods that are on this specific node
                    if [[ "$POD_NODE_NAME" == "$NODE_NAME" ]]; then
                        if [[ "$READY_STATUS" != "True" ]]; then
                            # Build a detailed reason for why the pod is not ready
                            REASON_TEXT=""
                            
                            # Parse comma-separated container reasons
                            if [[ -n "$CONTAINER_REASONS" ]]; then
                                # Split by comma and process each reason
                                IFS=',' read -ra REASON_ARRAY <<< "$CONTAINER_REASONS"
                                declare -A UNIQUE_REASONS
                                for reason in "${REASON_ARRAY[@]}"; do
                                    # Skip empty reasons and whitespace
                                    reason=$(echo "$reason" | xargs)  # trim whitespace
                                    if [[ -n "$reason" && "$reason" != "" && "$reason" != "null" ]]; then
                                        UNIQUE_REASONS["$reason"]=1
                                    fi
                                done
                                
                                # Prioritize reasons - show most critical first, skip less important ones
                                if [[ -n "${UNIQUE_REASONS[ImagePullBackOff]}" ]]; then
                                    REASON_TEXT="ImagePullBackOff"
                                elif [[ -n "${UNIQUE_REASONS[ErrImagePull]}" ]]; then
                                    REASON_TEXT="ErrImagePull"
                                elif [[ -n "${UNIQUE_REASONS[CrashLoopBackOff]}" ]]; then
                                    REASON_TEXT="CrashLoopBackOff"
                                elif [[ -n "${UNIQUE_REASONS[PodInitializing]}" ]]; then
                                    REASON_TEXT="PodInitializing"
                                else
                                    # Build the reason text from all unique reasons if none of the priority ones
                                    for reason in "${!UNIQUE_REASONS[@]}"; do
                                        if [[ -n "$REASON_TEXT" ]]; then
                                            REASON_TEXT="$REASON_TEXT, $reason"
                                        else
                                            REASON_TEXT="$reason"
                                        fi
                                    done
                                fi
                            fi
                            
                            # If no container reasons, use pod phase
                            if [[ -z "$REASON_TEXT" && -n "$POD_PHASE" && "$POD_PHASE" != "Running" ]]; then
                                REASON_TEXT="Pod phase: $POD_PHASE"
                            fi
                            
                            # Final fallback
                            if [[ -z "$REASON_TEXT" ]]; then
                                REASON_TEXT="Ready status: $READY_STATUS"
                            fi
                            
                            NOT_READY_PODS+=("$POD_NAME ($REASON_TEXT)")
                        fi
                    fi
                fi
            done <<< "$POD_STATUS_DATA"


        fi
    done <<< "$NODE_DATA"

    # Check for thor worker/manager conflicts with agents at the node pool level
    THOR_AGENTS_IN_POOL=()
    THOR_WORKERS_MANAGERS_IN_POOL=()
    
    # Get list of nodes in this pool for filtering
    POOL_NODES=($(echo "$NODE_DATA" | awk '{print $1}' | tr '\n' ' '))
    
    # Collect all thor agents and workers/managers in this node pool
    while IFS= read -r conflict_pod_info; do
        if [[ -n "$conflict_pod_info" ]]; then
            read -r POD_NAME POD_NODE_NAME <<< "$conflict_pod_info"
            
            # Only process pods that are actually running on nodes in this pool
            POD_IN_THIS_POOL=false
            for pool_node in "${POOL_NODES[@]}"; do
                if [[ "$POD_NODE_NAME" == "$pool_node" ]]; then
                    POD_IN_THIS_POOL=true
                    break
                fi
            done
            
            if [[ "$POD_IN_THIS_POOL" == true ]]; then
                # Check for thor agents (eclagent and thoragent)
                if [[ "$POD_NAME" == *"eclagent"* || "$POD_NAME" == *"thoragent"* ]]; then
                    THOR_AGENTS_IN_POOL+=("$POD_NAME")
                # Check for thor workers and managers (including job-based names)
                elif [[ "$POD_NAME" == *"thorworker"* || "$POD_NAME" == *"thormanager"* ]]; then
                    THOR_WORKERS_MANAGERS_IN_POOL+=("$POD_NAME")
                fi
            fi
        fi
    done <<< "$CONFLICT_POD_DATA"
    
    # Check for conflicts: if both agents and workers/managers exist in the same node pool
    if [ ${#THOR_AGENTS_IN_POOL[@]} -gt 0 ] && [ ${#THOR_WORKERS_MANAGERS_IN_POOL[@]} -gt 0 ]; then
        THOR_CONFLICTS+=("Thor agents and workers/managers running in same node pool - this prevents the node pool from scaling to 0")
    fi

    #CPU_UTIL=$(echo "scale=0; $IN_USE_CPU / $ALLOCATED_CPU * 100" | bc -l)
    CPU_UTIL=$(printf "%.0f" $(echo "scale=2; $ALLOCATED_CPU * 100 / $CAPACITY_CPU_TOTAL" | bc -l))
    MEM_UTIL=$(printf "%.0f" $(echo "scale=2; $USED_MEM * 100 / $TOTAL_MEM" | bc -l))

    # Calculate CPU In Use percentage
    if (( $(echo "$ALLOCATED_CPU > 0" | bc -l) )); then
        CPU_INUSE_PERCENT=$(printf "%.0f" $(echo "scale=2; $IN_USE_CPU * 100 / $ALLOCATED_CPU" | bc -l))
    else
        CPU_INUSE_PERCENT="0"
    fi

    # Calculate HPCC namespace CPU usage percentage
    if (( $(echo "$ALLOCATED_CPU > 0" | bc -l) )); then
        HPCC_PERCENT=$(printf "%.0f" $(echo "scale=2; $REQUESTED_CPU * 100 / $ALLOCATED_CPU" | bc -l))
    else
        HPCC_PERCENT="0"
    fi

    # Check if requested CPU is less than allocated CPU
    if (( $(echo "$REQUESTED_CPU < $ALLOCATED_CPU" | bc -l) )); then
        MESSAGES+=("$POOL: Some pods lack CPU resource requests - this prevents proper scheduling and resource guarantees")
    fi

    # Check if any pods are not ready
    if [ ${#NOT_READY_PODS[@]} -gt 0 ]; then
        NOT_READY_LIST=$(IFS='; ' ; echo "${NOT_READY_PODS[*]}")
        MESSAGES+=("$POOL: Not ready pods: $NOT_READY_LIST")
    fi

    # Check for thor conflicts
    if [ ${#THOR_CONFLICTS[@]} -gt 0 ]; then
        for conflict in "${THOR_CONFLICTS[@]}"; do
            MESSAGES+=("$POOL: $conflict")
        done
    fi

    # Check for active nodes with no HPCC workloads (wasted resources)
    if [[ "$POOL" == *"thorpool"* || "$POOL" == *"miscpool"* || "$POOL" == *"roxiepool"* || "$POOL" == *"spraypool"* ]]; then
        if (( $(echo "$HPCC_PERCENT == 0" | bc -l) )) && (( $(echo "$CPU_INUSE_PERCENT > 0" | bc -l) )); then
            POOL_HOURLY_COST=$(printf "%.2f" $(echo "scale=2; $NODE_COUNT * $VM_HOURLY_COST" | bc -l))
            MESSAGES+=("$POOL: This node pool has no active HPCC workloads but nodes are still consuming ${CPU_INUSE_PERCENT}% CPU. Consider scaling down or investigating system processes to reduce unnecessary costs (\$${POOL_HOURLY_COST}/hr)")
        fi
    fi

    # Calculate total hourly cost for this pool
    TOTAL_HOURLY_COST=$(printf "%.2f" $(echo "scale=2; $NODE_COUNT * $VM_HOURLY_COST" | bc -l))

    if $SHOW_PODS; then
        PODS_JOINED=$(IFS=, ; echo "${POD_LIST[*]}")
        printf "%-15s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %-s\n" "$POOL" "$NODE_COUNT" "$REQUESTED_CPU" "$CAPACITY_CPU_TOTAL" "$ALLOCATED_CPU" "$CPU_UTIL%" "$IN_USE_CPU" "$CPU_INUSE_PERCENT%" "$HPCC_PERCENT%" "$MEM_UTIL%" "\$${TOTAL_HOURLY_COST}" "$PODS_JOINED"
    else
        printf "%-15s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s\n" "$POOL" "$NODE_COUNT" "$REQUESTED_CPU" "$CAPACITY_CPU_TOTAL" "$ALLOCATED_CPU" "$CPU_UTIL%" "$IN_USE_CPU" "$CPU_INUSE_PERCENT%" "$HPCC_PERCENT%" "$MEM_UTIL%" "\$${TOTAL_HOURLY_COST}"
    fi
done

# Display messages if any
if [ ${#MESSAGES[@]} -gt 0 ]; then
    echo ""
    echo "Messages:"
    for message in "${MESSAGES[@]}"; do
        echo "  - $message"
    done
fi
