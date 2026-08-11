import * as React from "react";
import { Button, Checkbox, createTableColumn, Link, Spinner, TableColumnDefinition, TableColumnSizingOptions, makeStyles, tokens } from "@fluentui/react-components";
import { ChevronDownRegular, ChevronRightRegular, SettingsRegular, DocumentQueueRegular } from "@fluentui/react-icons";
import { TopologyService, WsTopology } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "../CommandBarV9";
import nlsHPCC from "src/nlsHPCC";
import { useTreeExpansion } from "../../hooks/useTreeExpansion";
import { useTreeTableSelection } from "../../hooks/useTreeTableSelection";
import { HolyGrail } from "../../layouts/HolyGrail";
import { TreeDataGrid } from "./TreeDataGrid";
import { goBack } from "../../util/history";
import { ComponentFileViewer, ComponentFileQueryParams } from "../ComponentFileViewer";
import { ServerLogViewer, ServerLogViewerQueryParams } from "../ServerLogViewer";
import { PreflightResults, PreflightQueryParams } from "../PreflightResults";
import { DetailDrawer } from "./DetailDrawer";
import { PreflightDialog, PreflightTarget } from "./PreflightDialog";

const logger = scopedLogger("src-react/components/controls/ClusterProcessesQueryTable.tsx");

const service = new TopologyService({ baseUrl: "" });

const useStyles = makeStyles({
    nameCell: {
        display: "flex",
        alignItems: "center",
        gap: "8px",
        overflow: "hidden"
    },
    iconButton: {
        cursor: "pointer",
        fontSize: "16px",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        ":hover": {
            color: tokens.colorBrandForeground1
        }
    },
    loadingContainer: {
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        height: "200px"
    },
});

export interface ClusterProcess {
    hpcc_id: string;
    Name: string;
    displayName: string;
    type: "clusterProcess" | "machine";
    // Both
    Platform?: string;
    Directory?: string;
    OS?: number;
    // clusterProcess fields
    Type?: string;
    Component?: string;
    LogDirectory?: string;
    Path?: string;
    Configuration?: boolean;
    // machine fields
    Domain?: string;
    Netaddress?: string;
    ProcessNumber?: number;
    Channel?: number;
    ParentName?: string;
    ParentLogDirectory?: string;
    // tree
    hasChildren?: boolean;
    children?: ClusterProcess[];
    childrenLoaded?: boolean;
    childrenLoading?: boolean;
}

interface ClusterProcessesQueryTableProps {
    selectedItems?: Set<string>;
    onSelectionChange?: (selectedIds: Set<string>) => void;
    onRowDoubleClick?: (item: ClusterProcess) => void;
    configDrawerParams?: ComponentFileQueryParams;
    configDrawerTitle?: string;
    logsDrawerParams?: ServerLogViewerQueryParams;
    logsDrawerTitle?: string;
    preflightDrawerParams?: PreflightQueryParams;
}

const platformForOS = (os?: number): string => {
    switch (os) {
        case 0: return "Windows";
        case 1: return "Solaris";
        case 2: return "Linux";
        default: return "Linux";
    }
};

const getMachineQueryType = (clusterType?: string): WsTopology.TpMachineType | undefined => {
    switch (clusterType) {
        case "RoxieCluster": return WsTopology.TpMachineType.ROXIEMACHINES;
        case "ThorCluster": return WsTopology.TpMachineType.THORMACHINES;
        default: return undefined;
    }
};

export const ClusterProcessesQueryTable: React.FunctionComponent<ClusterProcessesQueryTableProps> = ({
    selectedItems: selectedItemsProp,
    onSelectionChange,
    onRowDoubleClick,
    configDrawerParams,
    configDrawerTitle,
    logsDrawerParams,
    logsDrawerTitle,
    preflightDrawerParams
}) => {
    const styles = useStyles();
    const { expandedItems, toggle } = useTreeExpansion();
    const [clusters, setClusters] = React.useState<ClusterProcess[]>([]);
    const [loading, setLoading] = React.useState(true);
    const [showPreflightDialog, setShowPreflightDialog] = React.useState(false);

    const loadMachineChildren = React.useCallback((cluster: ClusterProcess) => {
        if (cluster.childrenLoaded || cluster.childrenLoading) return;

        setClusters(prev => prev.map(c => c.hpcc_id === cluster.hpcc_id ? { ...c, childrenLoading: true } : c));

        const machineType = getMachineQueryType(cluster.Type);
        service.TpMachineQuery({
            ...(machineType !== undefined && { Type: machineType }),
            Cluster: cluster.Name,
            Path: cluster.Path,
            Directory: cluster.Directory,
            LogDirectory: cluster.LogDirectory
        }).then((response: WsTopology.TpMachineQueryResponse) => {
            const machines = response?.TpMachines?.TpMachine ?? [];
            const children: ClusterProcess[] = machines.map(machine => ({
                hpcc_id: `${machine.Name}_${machine.Netaddress}_${machine.Directory}_${machine.ProcessNumber}`,
                Name: machine.Name ?? "",
                displayName: `${machine.Netaddress} - ${machine.Name}`,
                type: "machine",
                Domain: machine.Domain ?? "",
                Platform: platformForOS(machine.OS),
                ProcessNumber: machine.ProcessNumber,
                Channel: machine.Channels,
                Directory: machine.Directory ?? "",
                OS: machine.OS,
                Netaddress: machine.Netaddress ?? "",
                ParentName: cluster.Name,
                ParentLogDirectory: cluster.LogDirectory,
                hasChildren: false
            }));

            setClusters(prev => prev.map(c => c.hpcc_id === cluster.hpcc_id
                ? { ...c, children, childrenLoaded: true, childrenLoading: false }
                : c
            ));
        }).catch(err => {
            logger.error(err);
            setClusters(prev => prev.map(c => c.hpcc_id === cluster.hpcc_id
                ? { ...c, children: [], childrenLoaded: true, childrenLoading: false }
                : c
            ));
        });
    }, []);

    const handleExpansionToggle = React.useCallback((item: ClusterProcess) => {
        if (item.type !== "clusterProcess") return;
        const isExpanding = !expandedItems.has(item.hpcc_id);
        toggle(item.hpcc_id);
        if (isExpanding) {
            loadMachineChildren(item);
        }
    }, [expandedItems, toggle, loadMachineChildren]);

    const getIndentLevel = React.useCallback((level: number) => ({
        paddingLeft: `${level * 20}px`
    }), []);

    const isSelectable = React.useCallback((item: ClusterProcess) => item.type === "machine", []);

    const { selectedItems, clearSelection: selectionClear, getVisibleItems, handleSelectionToggle, handleSelectAll, handleRowClick, handleDoubleClick } = useTreeTableSelection({
        selectedItems: selectedItemsProp,
        onSelectionChange,
        onRowDoubleClick,
        isSelectable,
        items: clusters,
        expandedItems
    });

    const refreshTable = React.useCallback((clearSelection = false) => {
        if (clearSelection) {
            selectionClear();
        }

        setLoading(true);
        return service.TpClusterQuery({ Type: "ROOT" })
            .then((response: WsTopology.TpClusterQueryResponse) => {
                const rawClusters = response?.TpClusters?.TpCluster ?? [];
                const mappedClusters: ClusterProcess[] = rawClusters.map(cluster => ({
                    hpcc_id: cluster.Name ?? "",
                    Name: cluster.Name ?? "",
                    displayName: `${cluster.Type} - ${cluster.Name}`,
                    type: "clusterProcess",
                    Type: cluster.Type,
                    Component: cluster.Type,
                    Directory: cluster.Directory ?? "",
                    LogDirectory: cluster.LogDirectory ?? "",
                    Path: cluster.Path ?? "",
                    OS: cluster.OS,
                    Platform: platformForOS(cluster.OS),
                    Configuration: true,
                    hasChildren: true,
                    children: [],
                    childrenLoaded: false,
                    childrenLoading: false
                }));
                setClusters(mappedClusters);
            }).catch(err => {
                logger.error(err);
                setClusters([]);
            }).finally(() => {
                setLoading(false);
            });
    }, [selectionClear]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => { void refreshTable(); }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "preflight", text: nlsHPCC.Preflight, disabled: !selectedItems.size,
            onClick: () => {
                setShowPreflightDialog(true);
            }
        },
    ], [refreshTable, selectedItems]);

    const selectedProcesses = React.useMemo((): PreflightTarget[] => {
        const result: PreflightTarget[] = [];
        clusters.forEach(cluster => {
            (cluster.children ?? []).forEach(machine => {
                if (selectedItems.has(machine.hpcc_id)) {
                    result.push({
                        Name: cluster.Name,
                        Type: cluster.Type,
                        Netaddress: machine.Netaddress,
                        MachineType: cluster.Type,
                        ParentName: cluster.Name,
                        ParentDirectory: cluster.Directory
                    });
                }
            });
        });
        return result;
    }, [clusters, selectedItems]);

    React.useEffect(() => {
        refreshTable();
    }, [refreshTable]);

    const configurationHref = React.useCallback((item: ClusterProcess): string => {
        const params = new URLSearchParams();
        params.set("CompName", item.Name);
        params.set("CompType", item.Type ?? "");
        params.set("Directory", item.Directory ?? "");
        params.set("FileType", "cfg");
        if (item.OS !== undefined) {
            params.set("OsType", item.OS.toString());
        }
        return `#/operations/processes/${encodeURIComponent(item.Name)}/config?${params.toString()}`;
    }, []);

    const logsHref = React.useCallback((item: ClusterProcess): string => {
        const params = new URLSearchParams();
        params.set("NetAddress", item.Netaddress ?? "");
        params.set("LogDirectory", item.ParentLogDirectory ?? "");
        return `#/operations/processes/${encodeURIComponent(item.ParentName ?? "")}/logs?${params.toString()}`;
    }, []);

    const visibleItems = getVisibleItems();
    const selectableItems = visibleItems.filter(item => item.type === "machine");
    const allSelectableSelected = selectableItems.length > 0 && selectableItems.every(item => selectedItems.has(item.hpcc_id));

    const columnSizingOptions = React.useMemo<TableColumnSizingOptions>(() => ({
        selection: { minWidth: 24, idealWidth: 24 },
        configuration: { minWidth: 16, idealWidth: 16 },
        logs: { minWidth: 18, idealWidth: 18 },
        name: { minWidth: 220, idealWidth: 360 },
        domain: { minWidth: 100, idealWidth: 160 },
        platform: { minWidth: 60, idealWidth: 80 },
        processNumber: { minWidth: 80, idealWidth: 100 },
        channel: { minWidth: 80, idealWidth: 100 },
        directory: { minWidth: 220, idealWidth: 320 },
        logDirectory: { minWidth: 220, idealWidth: 320 }
    }), []);

    const columns = React.useMemo<TableColumnDefinition<ClusterProcess>[]>(() => [
        createTableColumn<ClusterProcess>({
            columnId: "selection",
            renderHeaderCell: () => selectableItems.length > 0 ? (
                <Checkbox checked={allSelectableSelected} onChange={handleSelectAll} />
            ) : null,
            renderCell: (item) => item.type === "machine" ? (
                <Checkbox checked={selectedItems.has(item.hpcc_id)} onChange={() => handleSelectionToggle(item)} />
            ) : <div style={{ width: "24px" }} />
        }),
        createTableColumn<ClusterProcess>({
            columnId: "configuration",
            renderHeaderCell: () => <div title={nlsHPCC.Configuration}><SettingsRegular /></div>,
            renderCell: (item) => item.type === "clusterProcess" ? (
                <Link className={styles.iconButton} href={configurationHref(item)} title={nlsHPCC.Configuration}>
                    <SettingsRegular />
                </Link>
            ) : null
        }),
        createTableColumn<ClusterProcess>({
            columnId: "logs",
            renderHeaderCell: () => <div title={nlsHPCC.Logs}><DocumentQueueRegular /></div>,
            renderCell: (item) => item.type === "machine" ? (
                <Link className={styles.iconButton} href={logsHref(item)} title={nlsHPCC.Logs}>
                    <DocumentQueueRegular />
                </Link>
            ) : null
        }),
        createTableColumn<ClusterProcess>({
            columnId: "name",
            renderHeaderCell: () => nlsHPCC.Name,
            renderCell: (item) => (
                <div className={styles.nameCell} style={getIndentLevel(item.type === "clusterProcess" ? 0 : 1)}>
                    {item.type === "clusterProcess" ? (
                        item.childrenLoading ? (
                            <Spinner size="tiny" />
                        ) : (
                            <Button
                                appearance="subtle"
                                size="small"
                                icon={expandedItems.has(item.hpcc_id) ? <ChevronDownRegular /> : <ChevronRightRegular />}
                                onClick={() => handleExpansionToggle(item)}
                            />
                        )
                    ) : (
                        <div style={{ width: "24px" }} />
                    )}
                    <span>{item.displayName || item.Name}</span>
                </div>
            )
        }),
        createTableColumn<ClusterProcess>({
            columnId: "domain",
            renderHeaderCell: () => nlsHPCC.Domain,
            renderCell: (item) => <span>{item.Domain ?? ""}</span>
        }),
        createTableColumn<ClusterProcess>({
            columnId: "platform",
            renderHeaderCell: () => nlsHPCC.Platform,
            renderCell: (item) => <span>{item.Platform ?? ""}</span>
        }),
        createTableColumn<ClusterProcess>({
            columnId: "processNumber",
            renderHeaderCell: () => nlsHPCC.SlaveNumber,
            renderCell: (item) => <span>{item.ProcessNumber !== undefined ? item.ProcessNumber : ""}</span>
        }),
        createTableColumn<ClusterProcess>({
            columnId: "channel",
            renderHeaderCell: () => nlsHPCC.Channel,
            renderCell: (item) => <span>{item.Channel !== undefined ? item.Channel : ""}</span>
        }),
        createTableColumn<ClusterProcess>({
            columnId: "directory",
            renderHeaderCell: () => nlsHPCC.Directory,
            renderCell: (item) => <span>{item.Directory ?? ""}</span>
        }),
        createTableColumn<ClusterProcess>({
            columnId: "logDirectory",
            renderHeaderCell: () => nlsHPCC.LogDirectory,
            renderCell: (item) => <span>{item.type === "clusterProcess" ? (item.LogDirectory ?? "") : ""}</span>
        })
    ], [allSelectableSelected, configurationHref, expandedItems, getIndentLevel, handleExpansionToggle, handleSelectAll, handleSelectionToggle, logsHref, selectableItems.length, selectedItems, styles.iconButton, styles.nameCell]);

    if (loading) {
        return <div className={styles.loadingContainer}>
            <Spinner label={nlsHPCC.Loading} />
        </div>;
    }

    return <>
        <HolyGrail
            header={<CommandBar items={buttons} farItems={[]} />}
            main={
                <TreeDataGrid
                    items={visibleItems}
                    columns={columns}
                    columnSizingOptions={columnSizingOptions}
                    isSelectable={isSelectable}
                    onRowClick={handleRowClick}
                    onRowDoubleClick={handleDoubleClick}
                />
            }
        />
        <PreflightDialog
            open={showPreflightDialog}
            onClose={() => setShowPreflightDialog(false)}
            mode="clusterProcess"
            selectedTargets={selectedProcesses}
            url="/operations/processes/preflight"
        />
        <DetailDrawer
            open={!!configDrawerParams}
            title={configDrawerTitle ?? ""}
            onClose={() => goBack()}
        >
            <ComponentFileViewer queryParams={configDrawerParams} />
        </DetailDrawer>
        <DetailDrawer
            open={!!logsDrawerParams}
            title={logsDrawerTitle ?? ""}
            onClose={() => goBack()}
        >
            <ServerLogViewer netAddress={logsDrawerParams?.NetAddress} logDirectory={logsDrawerParams?.LogDirectory} />
        </DetailDrawer>
        <DetailDrawer
            open={!!preflightDrawerParams}
            title={nlsHPCC.title_PreflightResults}
            onClose={() => goBack()}
        >
            <PreflightResults queryParams={preflightDrawerParams} />
        </DetailDrawer>
    </>;
};
