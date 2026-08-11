import * as React from "react";
import { Button, Checkbox, createTableColumn, Link, Spinner, TableColumnDefinition, TableColumnSizingOptions, makeStyles, tokens } from "@fluentui/react-components";
import { ChevronDownRegular, ChevronRightRegular, SettingsRegular, DatabaseRegular, DocumentQueueRegular } from "@fluentui/react-icons";
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
import { PreflightDialog } from "./PreflightDialog";

const logger = scopedLogger("src-react/components/controls/TargetClustersQueryTable.tsx");

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

export interface TargetCluster {
    hpcc_id: string;
    Name: string;
    displayName: string;
    CompName?: string;
    type: "targetClusterProcess" | "targetClusterComponent";
    Configuration?: boolean;
    DaliServer?: boolean;
    Node?: string;
    Platform?: string;
    Directory?: string;
    Netaddress?: string;
    OS?: number;
    Type?: string;
    ParentName?: string;
    LogDirectory?: string;
    LogDir?: string;
    hasChildren?: boolean;
    children?: TargetCluster[];
}

interface TargetClustersQueryTableProps {
    selectedItems?: Set<string>;
    onSelectionChange?: (selectedIds: Set<string>) => void;
    onRowDoubleClick?: (item: TargetCluster) => void;
    persistExpansion?: boolean;
    configDrawerParams?: ComponentFileQueryParams;
    configDrawerTitle?: string;
    logsDrawerParams?: ServerLogViewerQueryParams;
    logsDrawerTitle?: string;
    preflightDrawerParams?: PreflightQueryParams;
}

const EXPANSION_STORAGE_KEY = "TargetClusters_ExpandedItems";

export const TargetClustersQueryTable: React.FunctionComponent<TargetClustersQueryTableProps> = ({
    selectedItems: selectedItemsProp,
    onSelectionChange,
    onRowDoubleClick,
    persistExpansion = true,
    configDrawerParams,
    configDrawerTitle,
    logsDrawerParams,
    logsDrawerTitle,
    preflightDrawerParams
}) => {
    const styles = useStyles();
    const { expandedItems, toggle } = useTreeExpansion(persistExpansion ? EXPANSION_STORAGE_KEY : undefined);
    const [clusters, setClusters] = React.useState<TargetCluster[]>([]);
    const [loading, setLoading] = React.useState(true);
    const [showPreflightDialog, setShowPreflightDialog] = React.useState(false);

    const platformForOS = React.useCallback((os?: number): string => {
        switch (os) {
            case 0:
                return "Windows";
            case 1:
                return "Solaris";
            case 2:
                return "Linux";
            default:
                return "Linux";
        }
    }, []);

    const toComponents = React.useCallback((cluster: any): TargetCluster[] => {
        const children: TargetCluster[] = [];

        Object.keys(cluster ?? {}).forEach(key => {
            if (key === "TpEclServers") return;
            const section = cluster[key];
            if (!section || typeof section !== "object") return;

            Object.keys(section).forEach(sectionKey => {
                const items = section[sectionKey];
                if (!Array.isArray(items)) return;

                items.forEach(item => {
                    const machine = item?.TpMachines?.TpMachine?.[0];
                    children.push({
                        hpcc_id: `${cluster.Name}_${item.Name}`,
                        Name: item.Name,
                        displayName: `${item.Type} - ${item.Name}`,
                        CompName: item.Name,
                        Type: item.Type,
                        ParentName: cluster.Name,
                        DaliServer: !!item.DaliServer,
                        Directory: machine?.Directory ?? "",
                        LogDir: item?.LogDir,
                        LogDirectory: item?.LogDirectory,
                        OS: machine?.OS,
                        Platform: machine ? platformForOS(machine.OS) : "",
                        Configuration: !!item?.TpMachines,
                        Node: machine?.Name ?? "",
                        Netaddress: machine?.Netaddress ?? "",
                        type: "targetClusterComponent"
                    });
                });
            });
        });

        return children;
    }, [platformForOS]);

    const handleExpansionToggle = React.useCallback((item: TargetCluster) => {
        if (item.type !== "targetClusterProcess") return;
        toggle(item.hpcc_id);
    }, [toggle]);

    const getIndentLevel = React.useCallback((level: number) => ({
        paddingLeft: `${level * 20}px`
    }), []);

    const isSelectable = React.useCallback((item: TargetCluster) => item.type === "targetClusterProcess", []);

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
        return service.TpTargetClusterQuery({ Type: "ROOT" })
            .then((response: WsTopology.TpTargetClusterQueryResponse) => {
                const rawClusters = response?.TpTargetClusters?.TpTargetCluster ?? [];
                const mappedClusters: TargetCluster[] = rawClusters.map((cluster: any) => {
                    const children = toComponents(cluster);
                    return {
                        hpcc_id: cluster.Name,
                        Name: cluster.Name,
                        displayName: cluster.Name,
                        Type: cluster.Type,
                        type: "targetClusterProcess",
                        hasChildren: children.length > 0,
                        children
                    };
                });

                setClusters(mappedClusters);
            }).catch(err => {
                logger.error(err);
                setClusters([]);
            }).finally(() => {
                setLoading(false);
            });
    }, [selectionClear, toComponents]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => {
                void refreshTable();
            }
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider },
        {
            key: "preflight", text: nlsHPCC.Preflight, disabled: !selectedItems.size,
            onClick: () => {
                setShowPreflightDialog(true);
            }
        },
    ], [refreshTable, selectedItems]);

    const selectedTargetClusters = React.useMemo(() => {
        return clusters.filter(cluster => selectedItems.has(cluster.hpcc_id));
    }, [clusters, selectedItems]);

    React.useEffect(() => {
        refreshTable();
    }, [refreshTable]);

    const configurationHref = React.useCallback((item: TargetCluster): string => {
        const params = new URLSearchParams();
        params.set("CompName", item.CompName ?? item.Name);
        params.set("CompType", item.Type ?? "");
        params.set("Directory", item.Directory ?? "");
        params.set("FileType", "cfg");
        params.set("NetAddress", item.Netaddress ?? "");
        if (item.OS !== undefined) {
            params.set("OsType", item.OS.toString());
        }
        return `#/operations/clusters/${encodeURIComponent(item.ParentName ?? "")}/config?${params.toString()}`;
    }, []);

    const logsHref = React.useCallback((item: TargetCluster): string => {
        const params = new URLSearchParams();
        params.set("CompName", item.CompName ?? "");
        params.set("NetAddress", item.Netaddress ?? "");
        params.set("LogDirectory", item.LogDirectory ?? item.LogDir ?? "");
        return `#/operations/clusters/${encodeURIComponent(item.ParentName ?? "")}/logs?${params.toString()}`;
    }, []);

    const visibleItems = getVisibleItems();
    const selectableItems = visibleItems.filter(item => item.type === "targetClusterProcess");
    const allSelectableSelected = selectableItems.length > 0 && selectableItems.every(item => selectedItems.has(item.hpcc_id));

    const columnSizingOptions = React.useMemo<TableColumnSizingOptions>(() => ({
        selection: { minWidth: 24, idealWidth: 24 },
        configuration: { minWidth: 16, idealWidth: 16 },
        dali: { minWidth: 16, idealWidth: 16 },
        logs: { minWidth: 18, idealWidth: 18 },
        name: { minWidth: 220, idealWidth: 360 },
        node: { minWidth: 140, idealWidth: 220 },
        platform: { minWidth: 60, idealWidth: 60 },
        directory: { minWidth: 220, idealWidth: 320 }
    }), []);

    const columns = React.useMemo<TableColumnDefinition<TargetCluster>[]>(() => [
        createTableColumn<TargetCluster>({
            columnId: "selection",
            renderHeaderCell: () => selectableItems.length > 0 ? (
                <Checkbox
                    checked={allSelectableSelected}
                    onChange={handleSelectAll}
                />
            ) : null,
            renderCell: (item) => item.type === "targetClusterProcess" ? (
                <Checkbox
                    checked={selectedItems.has(item.hpcc_id)}
                    onChange={() => handleSelectionToggle(item)}
                />
            ) : <div style={{ width: "24px" }} />
        }),
        createTableColumn<TargetCluster>({
            columnId: "configuration",
            renderHeaderCell: () => <div title={nlsHPCC.Configuration}><SettingsRegular /></div>,
            renderCell: (item) => (item.Configuration && item.type === "targetClusterComponent") ? (
                <Link className={styles.iconButton} href={configurationHref(item)} title={nlsHPCC.Configuration}>
                    <SettingsRegular />
                </Link>
            ) : null
        }),
        createTableColumn<TargetCluster>({
            columnId: "dali",
            renderHeaderCell: () => <div title={nlsHPCC.Dali}><DatabaseRegular /></div>,
            renderCell: (item) => item.DaliServer ? <span title={nlsHPCC.Dali}><DatabaseRegular /></span> : null
        }),
        createTableColumn<TargetCluster>({
            columnId: "logs",
            renderHeaderCell: () => <div title={nlsHPCC.Logs}><DocumentQueueRegular /></div>,
            renderCell: (item) => (item.Configuration && item.type === "targetClusterComponent") ? (
                <Link className={styles.iconButton} href={logsHref(item)} title={nlsHPCC.Logs}>
                    <DocumentQueueRegular />
                </Link>
            ) : null
        }),
        createTableColumn<TargetCluster>({
            columnId: "name",
            renderHeaderCell: () => nlsHPCC.Name,
            renderCell: (item) => (
                <div className={styles.nameCell} style={getIndentLevel(item.type === "targetClusterProcess" ? 0 : 1)}>
                    {item.type === "targetClusterProcess" && item.children && item.children.length > 0 ? (
                        <Button
                            appearance="subtle"
                            size="small"
                            icon={expandedItems.has(item.hpcc_id) ? <ChevronDownRegular /> : <ChevronRightRegular />}
                            onClick={() => handleExpansionToggle(item)}
                        />
                    ) : (
                        <div style={{ width: "24px" }} />
                    )}
                    <span>{item.displayName || item.Name}</span>
                </div>
            )
        }),
        createTableColumn<TargetCluster>({
            columnId: "node",
            renderHeaderCell: () => nlsHPCC.Node,
            renderCell: (item) => <span>{item.Node || ""}</span>
        }),
        createTableColumn<TargetCluster>({
            columnId: "platform",
            renderHeaderCell: () => nlsHPCC.Platform,
            renderCell: (item) => <span>{item.Platform || ""}</span>
        }),
        createTableColumn<TargetCluster>({
            columnId: "directory",
            renderHeaderCell: () => nlsHPCC.Directory,
            renderCell: (item) => <span>{item.Directory || ""}</span>
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
            selectedTargets={selectedTargetClusters}
            url="/operations/clusters/preflight"
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
