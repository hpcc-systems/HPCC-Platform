import * as React from "react";
import { Button, SelectTabData, SelectTabEvent, Spinner, Tab, TabList, makeStyles, tokens } from "@fluentui/react-components";
import { ChevronDownRegular, ChevronRightRegular } from "@fluentui/react-icons";
import { TopologyService, WsTopology } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { useTreeExpansion } from "../hooks/useTreeExpansion";
import { goBack, pushUrl } from "../util/history";
import { HolyGrail } from "../layouts/HolyGrail";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "./CommandBarV9";
import { ComponentFileViewer, ComponentFileQueryParams } from "./ComponentFileViewer";
import { ServerLogViewer } from "./ServerLogViewer";
import { PreflightDialog, PreflightMode, PreflightTarget } from "./controls/PreflightDialog";
import { DetailDrawer } from "./controls/DetailDrawer";
import { PreflightResults, PreflightQueryParams } from "./PreflightResults";

const logger = scopedLogger("src-react/components/Topology.tsx");
const service = new TopologyService({ baseUrl: "" });

type ViewMode = "clusters" | "servers" | "processes";
type DetailTab = "summary" | "configuration" | "logs";

interface TopologyNode {
    id: string;
    label: string;
    depth: number;
    summaryProps: Array<{ key: string; value: string }>;
    bindingUrl?: string;
    canConfig: boolean;
    configParams?: ComponentFileQueryParams;
    canPreflight: boolean;
    canLogs: boolean;
    logsNetAddress?: string;
    logsLogDirectory?: string;
    isRoxieCluster: boolean;
    hasChildren: boolean;
    children: TopologyNode[];
    preflightTarget?: PreflightTarget;
}

const SKIP_PROPS = new Set(["Port", "Path", "ProcessNumber"]);

function buildSummaryProps(obj: Record<string, unknown>): Array<{ key: string; value: string }> {
    return Object.entries(obj)
        .filter(([key, val]) =>
            !key.startsWith("__") &&
            !SKIP_PROPS.has(key) &&
            val !== null &&
            val !== undefined &&
            typeof val !== "object"
        )
        .map(([key, val]) => ({ key, value: String(val) }));
}

function parseTpMachines(
    machines: WsTopology.TpMachine[],
    parentId: string,
    parentName: string,
    parentLogDirectory: string | undefined,
    depth: number
): TopologyNode[] {
    return machines.map((machine, idx) => ({
        id: `${parentId}_machine_${machine.Type}_${machine.Netaddress ?? idx}`,
        label: machine.Netaddress ? `[${machine.Netaddress}] ${machine.Name ?? ""}` : (machine.Name ?? ""),
        depth,
        summaryProps: buildSummaryProps(machine as Record<string, unknown>),
        canConfig: !!(machine.Netaddress && machine.Directory),
        configParams: machine.Netaddress && machine.Directory ? {
            CompType: machine.Type,
            CompName: parentName,
            NetAddress: machine.Netaddress,
            Directory: machine.Directory,
            FileType: "cfg",
            OsType: machine.OS
        } : undefined,
        canLogs: !!(machine.Netaddress && parentLogDirectory),
        canPreflight: false,
        logsNetAddress: machine.Netaddress,
        logsLogDirectory: parentLogDirectory,
        isRoxieCluster: false,
        hasChildren: false,
        children: []
    }));
}

function parseTargetClusters(response: WsTopology.TpTargetClusterQueryResponse): TopologyNode[] {
    const rawClusters = response?.TpTargetClusters?.TpTargetCluster ?? [];

    const groups = new Map<string, WsTopology.TpTargetCluster[]>();
    rawClusters.forEach(cluster => {
        const type = cluster.Type ?? "Unknown";
        if (!groups.has(type)) groups.set(type, []);
        groups.get(type)!.push(cluster);
    });

    return Array.from(groups.entries()).map(([type, items]) => {
        const clusterChildren: TopologyNode[] = items.map(cluster => {
            const children: TopologyNode[] = [];

            (cluster.TpClusters?.TpCluster ?? []).forEach(c => {
                const machines = c.TpMachines?.TpMachine ?? [];
                const machineChildren = parseTpMachines(machines, `tc_${cluster.Name}_cluster_${c.Name}`, c.Name ?? "", c.LogDirectory, 3);
                children.push({
                    id: `tc_${cluster.Name}_cluster_${c.Name}`,
                    label: `[${c.Type ?? "Cluster"}] ${c.Name ?? ""}`,
                    depth: 2,
                    summaryProps: buildSummaryProps(c as Record<string, unknown>),
                    canConfig: false,
                    canLogs: !!(machines[0]?.Netaddress && c.LogDirectory),
                    canPreflight: false,
                    logsNetAddress: machines[0]?.Netaddress,
                    logsLogDirectory: c.LogDirectory,
                    isRoxieCluster: type === "RoxieCluster",
                    hasChildren: machineChildren.length > 0,
                    children: machineChildren
                });
            });

            [
                { prefix: "eclcc", servers: cluster.TpEclCCServers?.TpEclServer ?? [], typeLabel: "EclCCServerProcess" },
                { prefix: "eclserver", servers: cluster.TpEclServers?.TpEclServer ?? [], typeLabel: "EclServerProcess" },
            ].forEach(({ prefix, servers, typeLabel }) => {
                servers.forEach(s => {
                    const machines = s.TpMachines?.TpMachine ?? [];
                    const machineChildren = parseTpMachines(machines, `tc_${cluster.Name}_${prefix}_${s.Name}`, s.Name ?? "", s.LogDirectory, 3);
                    children.push({
                        id: `tc_${cluster.Name}_${prefix}_${s.Name}`,
                        label: `[${s.Type ?? typeLabel}] ${s.Name ?? ""}`,
                        depth: 2,
                        summaryProps: buildSummaryProps(s as Record<string, unknown>),
                        canConfig: false,
                        canLogs: !!(machines[0]?.Netaddress && s.LogDirectory),
                        canPreflight: false,
                        logsNetAddress: machines[0]?.Netaddress,
                        logsLogDirectory: s.LogDirectory,
                        isRoxieCluster: false,
                        hasChildren: machineChildren.length > 0,
                        children: machineChildren
                    });
                });
            });

            (cluster.TpEclAgents?.TpEclAgent ?? []).forEach(a => {
                const machines = a.TpMachines?.TpMachine ?? [];
                const machineChildren = parseTpMachines(machines, `tc_${cluster.Name}_eclagent_${a.Name}`, a.Name ?? "", a.LogDir, 3);
                children.push({
                    id: `tc_${cluster.Name}_eclagent_${a.Name}`,
                    label: `[${a.Type ?? "EclAgentProcess"}] ${a.Name ?? ""}`,
                    depth: 2,
                    summaryProps: buildSummaryProps(a as Record<string, unknown>),
                    canConfig: false,
                    canLogs: !!(machines[0]?.Netaddress && a.LogDir),
                    canPreflight: false,
                    logsNetAddress: machines[0]?.Netaddress,
                    logsLogDirectory: a.LogDir,
                    isRoxieCluster: false,
                    hasChildren: machineChildren.length > 0,
                    children: machineChildren
                });
            });

            (cluster.TpEclSchedulers?.TpEclScheduler ?? []).forEach(s => {
                const machines = s.TpMachines?.TpMachine ?? [];
                const machineChildren = parseTpMachines(machines, `tc_${cluster.Name}_scheduler_${s.Name}`, s.Name ?? "", s.LogDirectory, 3);
                children.push({
                    id: `tc_${cluster.Name}_scheduler_${s.Name}`,
                    label: `[${s.Type ?? "EclSchedulerProcess"}] ${s.Name ?? ""}`,
                    depth: 2,
                    summaryProps: buildSummaryProps(s as Record<string, unknown>),
                    canConfig: false,
                    canLogs: !!(machines[0]?.Netaddress && s.LogDirectory),
                    canPreflight: false,
                    logsNetAddress: machines[0]?.Netaddress,
                    logsLogDirectory: s.LogDirectory,
                    isRoxieCluster: false,
                    hasChildren: machineChildren.length > 0,
                    children: machineChildren
                });
            });

            return {
                id: `tc_${cluster.Name}`,
                label: cluster.Name ?? "",
                depth: 1,
                summaryProps: buildSummaryProps({ Name: cluster.Name, Type: cluster.Type, Prefix: cluster.Prefix }),
                canConfig: false,
                canLogs: false,
                canPreflight: true,
                isRoxieCluster: type === "RoxieCluster",
                hasChildren: children.length > 0,
                children,
                preflightTarget: { Name: cluster.Name ?? "", Type: cluster.Type }
            };
        });

        return {
            id: `tcType_${type}`,
            label: type,
            depth: 0,
            summaryProps: [{ key: nlsHPCC.Type, value: type }],
            canConfig: false,
            canLogs: false,
            canPreflight: false,
            isRoxieCluster: false,
            hasChildren: clusterChildren.length > 0,
            children: clusterChildren
        };
    });
}

function parseSystemServers(response: WsTopology.TpServiceQueryResponse): TopologyNode[] {
    const serviceList = response?.ServiceList;
    if (!serviceList) return [];

    const serviceListAny = serviceList as Record<string, Record<string, unknown[]>>;
    const nodes: TopologyNode[] = [];

    Object.keys(serviceListAny).forEach(groupKey => {
        const group = serviceListAny[groupKey];
        if (!group || typeof group !== "object") return;

        const innerKey = Object.keys(group)[0];
        if (!innerKey) return;
        const items = group[innerKey];
        if (!Array.isArray(items)) return;

        const displayType = groupKey.replace(/^Tp/, "");

        const serviceChildren: TopologyNode[] = items.map((item: any) => {
            const machines: WsTopology.TpMachine[] = item?.TpMachines?.TpMachine ?? [];
            const bindings: WsTopology.TpBinding[] = item?.TpBindings?.TpBinding ?? [];
            const firstBinding = bindings[0];

            const machineChildren: TopologyNode[] = machines.map((machine, idx) => {
                const netaddress = machine.Netaddress ?? "";
                const bindingUrl = firstBinding && netaddress
                    ? `${firstBinding.Protocol}://${netaddress}:${firstBinding.Port}/`
                    : undefined;
                return {
                    id: `ss_${item.Name}_machine_${machine.Name ?? idx}`,
                    label: netaddress ? `[${netaddress}] ${machine.Name ?? ""}` : (machine.Name ?? ""),
                    depth: 2,
                    summaryProps: buildSummaryProps(machine as Record<string, unknown>),
                    bindingUrl,
                    canConfig: !!(machine.Netaddress && machine.Directory),
                    configParams: machine.Netaddress && machine.Directory ? { CompType: machine.Type, CompName: item.Name, NetAddress: machine.Netaddress, Directory: machine.Directory, FileType: "cfg", OsType: machine.OS } : undefined,
                    canLogs: !!(machine.Netaddress && item.LogDirectory),
                    canPreflight: true,
                    logsNetAddress: machine.Netaddress,
                    logsLogDirectory: item.LogDirectory,
                    isRoxieCluster: false,
                    hasChildren: false,
                    children: [],
                    preflightTarget: {
                        Name: item.Name ?? "",
                        Type: machine.Type ?? "",
                        Netaddress: machine.Netaddress ?? "",
                        MachineType: machine.Type ?? "",
                        ParentName: item.Name ?? "",
                        ParentDirectory: machine.Directory ?? ""
                    }
                };
            });

            return {
                id: `ss_${displayType}_${item.Name}`,
                label: item.Name ?? "",
                depth: 1,
                summaryProps: buildSummaryProps({ Name: item.Name, Type: item.Type ?? displayType, Description: item.Description, Queue: item.Queue }),
                canConfig: false,
                canLogs: !!(machines[0]?.Netaddress && item.LogDirectory),
                canPreflight: false,
                logsNetAddress: machines[0]?.Netaddress,
                logsLogDirectory: item.LogDirectory,
                isRoxieCluster: false,
                hasChildren: machineChildren.length > 0,
                children: machineChildren
            };
        });

        nodes.push({
            id: `ssType_${displayType}`,
            label: displayType,
            depth: 0,
            summaryProps: [{ key: nlsHPCC.Type, value: displayType }],
            canConfig: false,
            canLogs: false,
            canPreflight: false,
            isRoxieCluster: false,
            hasChildren: serviceChildren.length > 0,
            children: serviceChildren
        });
    });

    return nodes;
}

interface MachineEntry {
    machine: WsTopology.TpMachine;
    services: TopologyNode[];
}

function ipSortKey(addr: string): number {
    const parts = addr.split(".").map(Number);
    return ((parts[0] ?? 0) * 0x1000000) + ((parts[1] ?? 0) * 0x10000) + ((parts[2] ?? 0) * 0x100) + (parts[3] ?? 0);
}

function addToMachineMap(
    machineMap: Map<string, MachineEntry>,
    machines: WsTopology.TpMachine[],
    serviceLabel: string,
    serviceSummaryProps: Array<{ key: string; value: string }>,
    logDirectory?: string,
    preflightBase?: Omit<PreflightTarget, "Netaddress">
): void {
    machines.forEach(machine => {
        const addr = machine.Netaddress ?? "";
        if (!machineMap.has(addr)) {
            machineMap.set(addr, { machine, services: [] });
        }
        const entry = machineMap.get(addr)!;
        // Deduplicate by label (type+name) per machine — the same process can be
        // referenced by multiple target clusters but should only appear once here.
        const serviceNodeId = `svc_${serviceLabel}_on_${addr}`;
        if (!entry.services.some(s => s.id === serviceNodeId)) {
            entry.services.push({
                id: serviceNodeId,
                label: serviceLabel,
                depth: 1,
                summaryProps: serviceSummaryProps,
                canConfig: false,
                canLogs: !!(addr && logDirectory),
                canPreflight: !!preflightBase,
                logsNetAddress: addr,
                logsLogDirectory: logDirectory,
                isRoxieCluster: false,
                hasChildren: false,
                children: [],
                preflightTarget: preflightBase ? { ...preflightBase, Netaddress: addr } : undefined
            });
        }
    });
}

function parseClusterProcessMachines(
    clusterResponse: WsTopology.TpTargetClusterQueryResponse,
    serviceResponse: WsTopology.TpServiceQueryResponse
): TopologyNode[] {
    const machineMap = new Map<string, MachineEntry>();

    const clusters = clusterResponse?.TpTargetClusters?.TpTargetCluster ?? [];
    clusters.forEach(cluster => {
        (cluster.TpClusters?.TpCluster ?? []).forEach(c => {
            addToMachineMap(machineMap, c.TpMachines?.TpMachine ?? [],
                `[${c.Type ?? "Cluster"}] ${c.Name ?? ""}`,
                buildSummaryProps(c as Record<string, unknown>),
                c.LogDirectory,
                { Name: c.Name ?? "", Type: c.Type ?? "", MachineType: c.Type ?? "", ParentName: c.Name ?? "", ParentDirectory: c.LogDirectory ?? "" }
            );
        });
        (cluster.TpEclCCServers?.TpEclServer ?? []).forEach(s => {
            addToMachineMap(machineMap, s.TpMachines?.TpMachine ?? [],
                `[${s.Type ?? "EclCCServerProcess"}] ${s.Name ?? ""}`,
                buildSummaryProps(s as Record<string, unknown>),
                s.LogDirectory,
                { Name: s.Name ?? "", Type: s.Type ?? "", MachineType: s.Type ?? "", ParentName: s.Name ?? "", ParentDirectory: s.LogDirectory ?? "" }
            );
        });
        (cluster.TpEclServers?.TpEclServer ?? []).forEach(s => {
            addToMachineMap(machineMap, s.TpMachines?.TpMachine ?? [],
                `[${s.Type ?? "EclServerProcess"}] ${s.Name ?? ""}`,
                buildSummaryProps(s as Record<string, unknown>),
                s.LogDirectory,
                { Name: s.Name ?? "", Type: s.Type ?? "", MachineType: s.Type ?? "", ParentName: s.Name ?? "", ParentDirectory: s.LogDirectory ?? "" }
            );
        });
        (cluster.TpEclAgents?.TpEclAgent ?? []).forEach(a => {
            addToMachineMap(machineMap, a.TpMachines?.TpMachine ?? [],
                `[${a.Type ?? "EclAgentProcess"}] ${a.Name ?? ""}`,
                buildSummaryProps(a as Record<string, unknown>),
                a.LogDir,
                { Name: a.Name ?? "", Type: a.Type ?? "", MachineType: a.Type ?? "", ParentName: a.Name ?? "", ParentDirectory: a.LogDir ?? "" }
            );
        });
        (cluster.TpEclSchedulers?.TpEclScheduler ?? []).forEach(s => {
            addToMachineMap(machineMap, s.TpMachines?.TpMachine ?? [],
                `[${s.Type ?? "EclSchedulerProcess"}] ${s.Name ?? ""}`,
                buildSummaryProps(s as Record<string, unknown>),
                s.LogDirectory,
                { Name: s.Name ?? "", Type: s.Type ?? "", MachineType: s.Type ?? "", ParentName: s.Name ?? "", ParentDirectory: s.LogDirectory ?? "" }
            );
        });
    });

    const serviceList = serviceResponse?.ServiceList;
    if (serviceList) {
        const serviceListAny = serviceList as Record<string, Record<string, unknown[]>>;
        Object.keys(serviceListAny).forEach(groupKey => {
            const group = serviceListAny[groupKey];
            if (!group || typeof group !== "object") return;
            const innerKey = Object.keys(group)[0];
            if (!innerKey) return;
            const items = group[innerKey];
            if (!Array.isArray(items)) return;

            items.forEach((item: any) => {
                const machines: WsTopology.TpMachine[] = item?.TpMachines?.TpMachine ?? [];
                const serviceLabel = `[${item.Type ?? groupKey.replace(/^Tp/, "")}] ${item.Name ?? ""}`;
                addToMachineMap(machineMap, machines,
                    serviceLabel,
                    buildSummaryProps({ Name: item.Name, Type: item.Type ?? groupKey.replace(/^Tp/, ""), Description: item.Description }),
                    item.LogDirectory ?? item.LogDir,
                    { Name: item.Name ?? "", Type: item.Type ?? "", MachineType: item.Type ?? "", ParentName: item.Name ?? "", ParentDirectory: item.LogDirectory ?? item.LogDir ?? "" }
                );
            });
        });
    }

    return Array.from(machineMap.entries())
        .sort(([a], [b]) => ipSortKey(a) - ipSortKey(b))
        .map(([addr, { machine, services }]) => ({
            id: `machine_${addr}`,
            label: addr ? `[${addr}] ${machine.Name ?? ""}` : (machine.Name ?? ""),
            depth: 0,
            summaryProps: buildSummaryProps(machine as Record<string, unknown>),
            canConfig: !!(machine.Netaddress && machine.Directory),
            configParams: machine.Netaddress && machine.Directory ? {
                CompType: machine.Type,
                CompName: machine.Name,
                NetAddress: machine.Netaddress,
                Directory: machine.Directory,
                FileType: "cfg",
                OsType: machine.OS
            } : undefined,
            canLogs: false,
            canPreflight: false,
            isRoxieCluster: false,
            hasChildren: services.length > 0,
            children: services
        }));
}

const useStyles = makeStyles({
    viewTabs: {
        flexShrink: 0
    },
    treePane: {
        height: "100%",
        overflowY: "auto",
        overflowX: "hidden"
    },
    treeRow: {
        display: "flex",
        alignItems: "center",
        padding: "2px 4px",
        cursor: "pointer",
        userSelect: "none",
        ":hover": {
            backgroundColor: tokens.colorNeutralBackground1Hover
        }
    },
    treeRowSelected: {
        backgroundColor: tokens.colorNeutralBackground1Selected,
        ":hover": {
            backgroundColor: tokens.colorNeutralBackground1Selected
        }
    },
    treeLabel: {
        flex: 1,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap"
    },
    detailPane: {
        height: "100%",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden"
    },
    detailTabs: {
        flexShrink: 0
    },
    detailContent: {
        flex: 1,
        overflow: "auto",
        padding: "8px"
    },
    summaryTable: {
        borderCollapse: "collapse",
        width: "100%"
    },
    summaryCell: {
        padding: "2px 6px",
        verticalAlign: "top"
    },
    summaryCellKey: {
        fontWeight: tokens.fontWeightSemibold,
        whiteSpace: "nowrap"
    },
    placeholder: {
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: "100%",
        color: tokens.colorNeutralForeground3
    },
    loadingContainer: {
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: "100%"
    }
});

interface TreePaneProps {
    nodes: TopologyNode[];
    loading: boolean;
    selectedId: string | undefined;
    onSelect: (node: TopologyNode) => void;
    storageKey: string;
}

const TreePane: React.FunctionComponent<TreePaneProps> = ({ nodes, loading, selectedId, onSelect, storageKey }) => {
    const styles = useStyles();
    const { expandedItems, toggle } = useTreeExpansion(storageKey);

    const toggleExpand = React.useCallback((id: string) => {
        toggle(id);
    }, [toggle]);

    const renderNode = React.useCallback((node: TopologyNode): React.ReactNode => {
        const isExpanded = expandedItems.has(node.id);
        const isSelected = node.id === selectedId;
        const indent = node.depth * 16;

        return (
            <React.Fragment key={node.id}>
                <div
                    data-testid="topology-tree-row"
                    className={`${styles.treeRow}${isSelected ? ` ${styles.treeRowSelected}` : ""}`}
                    style={{ paddingLeft: `${indent + 4}px` }}
                    onClick={() => onSelect(node)}
                >
                    {node.hasChildren ? (
                        <Button
                            appearance="subtle"
                            size="small"
                            icon={isExpanded ? <ChevronDownRegular /> : <ChevronRightRegular />}
                            onClick={e => { e.stopPropagation(); toggleExpand(node.id); }}
                        />
                    ) : (
                        <div style={{ width: "24px", flexShrink: 0 }} />
                    )}
                    <span className={styles.treeLabel}>{node.label}</span>
                </div>
                {isExpanded && node.children.map(child => renderNode(child))}
            </React.Fragment>
        );
    }, [expandedItems, selectedId, onSelect, toggleExpand, styles.treeRow, styles.treeRowSelected, styles.treeLabel]);

    if (loading) {
        return <div className={styles.loadingContainer}><Spinner label={nlsHPCC.Loading} /></div>;
    }

    return <div className={styles.treePane}>{nodes.map(n => renderNode(n))}</div>;
};

interface DetailPaneProps {
    node: TopologyNode | undefined;
    detailTab: DetailTab;
    onTabSelect: (tab: DetailTab) => void;
}

const DetailPane: React.FunctionComponent<DetailPaneProps> = ({ node, detailTab, onTabSelect }) => {
    const styles = useStyles();

    const handleTabSelect = React.useCallback((_evt: SelectTabEvent, data: SelectTabData) => {
        onTabSelect(data.value as DetailTab);
    }, [onTabSelect]);

    if (!node) {
        return <div className={styles.placeholder}>{nlsHPCC.PleaseSelectATopologyItem}</div>;
    }

    const showConfig = node.canConfig;
    const showLogs = node.canLogs;

    return <div className={styles.detailPane}>
        <TabList className={styles.detailTabs} selectedValue={detailTab} onTabSelect={handleTabSelect} size="small">
            <Tab value="summary">{nlsHPCC.Summary}</Tab>
            <Tab value="configuration" disabled={!showConfig}>{nlsHPCC.Configuration}</Tab>
            <Tab value="logs" disabled={!showLogs}>{nlsHPCC.Logs}</Tab>
        </TabList>
        <div className={styles.detailContent}>
            {detailTab === "summary" && (
                <table className={styles.summaryTable}>
                    <tbody>
                        {node.summaryProps.map(({ key, value }) => (
                            <tr key={key}>
                                <td className={`${styles.summaryCell} ${styles.summaryCellKey}`}>{key}:</td>
                                <td className={styles.summaryCell}>{value}</td>
                            </tr>
                        ))}
                        {node.bindingUrl && (
                            <tr>
                                <td className={`${styles.summaryCell} ${styles.summaryCellKey}`}>{nlsHPCC.URL}:</td>
                                <td className={styles.summaryCell}>
                                    <a href={node.bindingUrl} target="_blank" rel="noreferrer">{node.bindingUrl}</a>
                                </td>
                            </tr>
                        )}
                    </tbody>
                </table>
            )}
            {detailTab === "configuration" && showConfig && (
                <div style={{ height: "100%", minHeight: "200px" }}>
                    <ComponentFileViewer queryParams={node.configParams} />
                </div>
            )}
            {detailTab === "logs" && showLogs && (
                <div style={{ height: "100%", minHeight: "200px" }}>
                    <ServerLogViewer netAddress={node.logsNetAddress} logDirectory={node.logsLogDirectory} />
                </div>
            )}
        </div>
    </div>;
};

function findNodeById(nodes: TopologyNode[], id: string): TopologyNode | undefined {
    for (const n of nodes) {
        if (n.id === id) return n;
        if (n.hasChildren) {
            const found = findNodeById(n.children, id);
            if (found) return found;
        }
    }
    return undefined;
}

const VIEW_MODES: ViewMode[] = ["clusters", "servers", "processes"];

const PREFLIGHT_MODE: Record<ViewMode, PreflightMode> = {
    clusters: "targetCluster",
    servers: "systemServer",
    processes: "clusterProcess"
};

interface TopologyProps {
    tab?: string;
    node?: string;
    detailTab?: string;
    preflightDrawerParams?: PreflightQueryParams;
}

export const Topology: React.FunctionComponent<TopologyProps> = ({ tab, node, detailTab, preflightDrawerParams }) => {
    const styles = useStyles();
    const viewMode: ViewMode = VIEW_MODES.includes(tab as ViewMode) ? (tab as ViewMode) : "clusters";
    const [nodes, setNodes] = React.useState<TopologyNode[]>([]);
    const [loading, setLoading] = React.useState(true);
    const [selectedNode, setSelectedNode] = React.useState<TopologyNode | undefined>();
    const [preflightEnabled, setPreflightEnabled] = React.useState(false);
    const [showPreflightDialog, setShowPreflightDialog] = React.useState(false);
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();

    const activeDetailTab: DetailTab = detailTab as DetailTab ?? "summary";

    const onViewTabSelect = React.useCallback((_evt: SelectTabEvent, data: SelectTabData) => {
        pushUrl(`/operations/topology/${data.value as string}`);
        setSelectedNode(undefined);
        setPreflightEnabled(false);
    }, []);

    const onDetailTabSelect = React.useCallback((newTab: DetailTab) => {
        pushUrl(`/operations/topology/${viewMode}/${selectedNode?.id}/${newTab}`);
    }, [viewMode, selectedNode?.id]);

    const refresh = React.useCallback(() => {
        setLoading(true);
        setSelectedNode(undefined);
        setPreflightEnabled(false);
        let fetcher: Promise<TopologyNode[]>;
        switch (viewMode) {
            case "clusters":
                fetcher = service.TpTargetClusterQuery({ Type: "ROOT" })
                    .then(r => parseTargetClusters(r as WsTopology.TpTargetClusterQueryResponse));
                break;
            case "servers":
                fetcher = service.TpServiceQuery({ Type: "ALLSERVICES" })
                    .then(r => parseSystemServers(r as WsTopology.TpServiceQueryResponse));
                break;
            case "processes":
                fetcher = Promise.all([
                    service.TpTargetClusterQuery({ Type: "ROOT" }),
                    service.TpServiceQuery({ Type: "ALLSERVICES" })
                ]).then(([clusterResp, serviceResp]) =>
                    parseClusterProcessMachines(
                        clusterResp as WsTopology.TpTargetClusterQueryResponse,
                        serviceResp as WsTopology.TpServiceQueryResponse
                    )
                );
                break;
        }
        fetcher
            .then(result => setNodes(result))
            .catch(err => { logger.error(err); setNodes([]); })
            .finally(() => setLoading(false));
    }, [viewMode]);

    React.useEffect(() => { refresh(); }, [refresh]);

    React.useEffect(() => {
        if (!node || nodes.length === 0) return;
        const found = findNodeById(nodes, node);
        if (found) {
            setSelectedNode(found);
            setPreflightEnabled(found.canPreflight);
        }
    }, [nodes, node]);

    React.useEffect(() => {
        if (dockpanel) {
            //  Should only happen once on startup  ---
            const layout: any = dockpanel.layout();
            if (Array.isArray(layout?.main?.sizes) && layout.main.sizes.length === 2) {
                layout.main.sizes = [0.3, 0.7];
                dockpanel.layout(layout).lazyRender();
            }
        }
    }, [dockpanel]);

    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => { refresh(); }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "preflight", text: nlsHPCC.Preflight, disabled: !preflightEnabled,
            onClick: () => setShowPreflightDialog(true)
        },
    ], [preflightEnabled, refresh]);

    return <HolyGrail
        header={<>
            <TabList className={styles.viewTabs} selectedValue={viewMode} onTabSelect={onViewTabSelect} size="medium">
                <Tab value="clusters">{nlsHPCC.TargetClusters}</Tab>
                <Tab value="servers">{nlsHPCC.SystemServers}</Tab>
                <Tab value="processes">{nlsHPCC.ClusterProcesses}</Tab>
            </TabList>
            <CommandBar items={buttons} />
        </>}
        main={<>
            <DockPanel hideSingleTabs onCreate={setDockpanel}>
                <DockPanelItem key="topologyTree" title={nlsHPCC.Topology}>
                    <TreePane
                        nodes={nodes}
                        loading={loading}
                        selectedId={selectedNode?.id}
                        storageKey={`Topology_${viewMode}_ExpandedItems`}
                        onSelect={(node) => {
                            pushUrl(`/operations/topology/${viewMode}/${node.id}/${activeDetailTab}`);
                            setPreflightEnabled(node.canPreflight);
                            setSelectedNode(node);
                        }}
                    />
                </DockPanelItem>
                <DockPanelItem key="topologyDetail" title={nlsHPCC.Details} padding={4} location="split-right" relativeTo="topologyTree">
                    <DetailPane node={selectedNode} detailTab={activeDetailTab} onTabSelect={onDetailTabSelect} />
                </DockPanelItem>
            </DockPanel>
            <PreflightDialog
                open={showPreflightDialog}
                onClose={() => setShowPreflightDialog(false)}
                mode={PREFLIGHT_MODE[viewMode]}
                selectedTargets={selectedNode?.preflightTarget ? [selectedNode.preflightTarget] : []}
                url={`/operations/topology/${viewMode}/${selectedNode?.id}/preflight`}
            />
            <DetailDrawer
                open={!!preflightDrawerParams}
                title={nlsHPCC.title_PreflightResults}
                onClose={() => goBack()}
            >
                <PreflightResults queryParams={preflightDrawerParams} />
            </DetailDrawer>
        </>}
    />;
};
