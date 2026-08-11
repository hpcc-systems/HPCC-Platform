import * as React from "react";
import { Button, Checkbox, createTableColumn, Link, Spinner, Table, TableBody, TableCell, TableHeader, TableHeaderCell, TableRow, TableColumnDefinition, TableColumnSizingOptions, makeStyles, tokens } from "@fluentui/react-components";
import { ChevronDownRegular, ChevronRightRegular, SettingsRegular, DocumentQueueRegular, InfoRegular } from "@fluentui/react-icons";
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

const logger = scopedLogger("src-react/components/controls/SystemServersQueryTable.tsx");

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
    bindingsTable: {
        width: "100%",
        margin: "0 17px",
        "& th": {
            fontWeight: "bold"
        }
    }
});

export interface SystemServer {
    hpcc_id: string;
    Name: string;
    displayName: string;
    type: "systemServer" | "machine";
    // systemServer (parent) fields
    ServiceType?: string;
    Queue?: string;
    LogDirectory?: string;
    AuditLogDirectory?: string;
    TpBindings?: WsTopology.TpBindings;
    // machine (child) fields
    Computer?: string;
    Netaddress?: string;
    NetaddressWithPort?: string;
    OS?: number;
    Directory?: string;
    MachineType?: string;
    Configuration?: boolean;
    Informational?: boolean;
    AuditLog?: boolean;
    Log?: boolean;
    ParentName?: string;
    ParentLogDirectory?: string;
    ParentAuditLogDirectory?: string;
    ParentTpBindings?: WsTopology.TpBindings;
    // tree
    hasChildren?: boolean;
    children?: SystemServer[];
}

interface SystemServersQueryTableProps {
    selectedItems?: Set<string>;
    onSelectionChange?: (selectedIds: Set<string>) => void;
    onRowDoubleClick?: (item: SystemServer) => void;
    persistExpansion?: boolean;
    configDrawerParams?: ComponentFileQueryParams;
    configDrawerTitle?: string;
    logsDrawerParams?: ServerLogViewerQueryParams;
    logsDrawerTitle?: string;
    preflightDrawerParams?: PreflightQueryParams;
}

const EXPANSION_STORAGE_KEY = "SystemServers_ExpandedItems";

const getMachinePort = (port?: number): string => {
    return port && port > 0 ? `:${port}` : "";
};

const parseServiceList = (serviceList: WsTopology.ServiceList): SystemServer[] => {
    const servers: SystemServer[] = [];
    const serviceListAny = serviceList as Record<string, Record<string, any[]>>;

    Object.keys(serviceListAny).forEach(key => {
        const section = serviceListAny[key];
        if (!section || typeof section !== "object") return;

        const innerKey = Object.keys(section)[0];
        if (!innerKey) return;
        const items: any[] = section[innerKey];
        if (!Array.isArray(items)) return;

        // Strip leading "Tp" to get a human-readable category label (e.g. "TpDalis" → "Dalis")
        const serviceType = key.replace(/^Tp/, "");

        items.forEach(item => {
            const machines: WsTopology.TpMachine[] = item?.TpMachines?.TpMachine ?? [];
            const parentName: string = item.Name ?? "";

            const children: SystemServer[] = machines.map((machine, idx) => {
                const netaddress = machine.Netaddress ?? "";
                const port = machine.Port as number | undefined;
                return {
                    hpcc_id: `${parentName}_${machine.Name}_${idx}`,
                    Name: parentName,
                    displayName: parentName,
                    type: "machine",
                    Computer: machine.Name ?? "",
                    Netaddress: netaddress,
                    NetaddressWithPort: `${netaddress}${getMachinePort(port)}`,
                    OS: machine.OS as number | undefined,
                    Directory: machine.Directory ?? "",
                    MachineType: machine.Type ?? "",
                    Configuration: !!(machine.Directory && machine.Type && machine.Type !== "FTSlaveProcess"),
                    Informational: machine.Type === "SparkThorProcess" || machine.Type === "EspProcess",
                    AuditLog: !!item.AuditLogDirectory,
                    Log: !!item.LogDirectory,
                    ParentName: parentName,
                    ParentLogDirectory: item.LogDirectory ?? "",
                    ParentAuditLogDirectory: item.AuditLogDirectory ?? "",
                    ParentTpBindings: item.TpBindings,
                    Queue: item.Queue ?? "",
                    hasChildren: false
                };
            });

            servers.push({
                hpcc_id: `${serviceType}_${parentName}`,
                Name: serviceType,
                displayName: serviceType,
                type: "systemServer",
                ServiceType: serviceType,
                Queue: item.Queue ?? "",
                LogDirectory: item.LogDirectory ?? "",
                AuditLogDirectory: item.AuditLogDirectory ?? "",
                TpBindings: item.TpBindings,
                hasChildren: children.length > 0,
                children
            });
        });
    });

    return servers;
};

export const SystemServersQueryTable: React.FunctionComponent<SystemServersQueryTableProps> = ({
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
    const [servers, setServers] = React.useState<SystemServer[]>([]);
    const [loading, setLoading] = React.useState(true);
    const [showPreflightDialog, setShowPreflightDialog] = React.useState(false);
    const [bindingsItem, setBindingsItem] = React.useState<SystemServer | null>(null);

    const handleExpansionToggle = React.useCallback((item: SystemServer) => {
        if (item.type !== "systemServer") return;
        toggle(item.hpcc_id);
    }, [toggle]);

    const getIndentLevel = React.useCallback((level: number) => ({
        paddingLeft: `${level * 20}px`
    }), []);

    const isSelectable = React.useCallback((item: SystemServer) => item.type === "machine" && item.MachineType !== "LDAPServerProcess", []);

    const { selectedItems, clearSelection: selectionClear, getVisibleItems, handleSelectionToggle, handleSelectAll, handleRowClick, handleDoubleClick } = useTreeTableSelection({
        selectedItems: selectedItemsProp,
        onSelectionChange,
        onRowDoubleClick,
        isSelectable,
        items: servers,
        expandedItems
    });

    const refreshTable = React.useCallback((clearSelection = false) => {
        if (clearSelection) {
            selectionClear();
        }

        setLoading(true);
        return service.TpServiceQuery({ Type: "ALLSERVICES" })
            .then((response: WsTopology.TpServiceQueryResponse) => {
                const serviceList = response?.ServiceList;
                if (!serviceList) {
                    setServers([]);
                    return;
                }
                setServers(parseServiceList(serviceList));
            }).catch(err => {
                logger.error(err);
                setServers([]);
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
            onClick: () => { setShowPreflightDialog(true); }
        },
    ], [refreshTable, selectedItems]);

    const selectedProcesses = React.useMemo((): PreflightTarget[] => {
        const result: PreflightTarget[] = [];
        servers.forEach(server => {
            (server.children ?? []).forEach(machine => {
                if (selectedItems.has(machine.hpcc_id)) {
                    result.push({
                        Name: machine.ParentName ?? "",
                        Type: machine.MachineType,
                        Netaddress: machine.Netaddress,
                        MachineType: machine.MachineType,
                        ParentName: machine.ParentName,
                        ParentDirectory: machine.Directory
                    });
                }
            });
        });
        return result;
    }, [servers, selectedItems]);

    React.useEffect(() => {
        refreshTable();
    }, [refreshTable]);

    const configurationHref = React.useCallback((item: SystemServer): string => {
        const params = new URLSearchParams();
        params.set("CompName", item.ParentName ?? item.Name);
        params.set("CompType", item.MachineType ?? "");
        params.set("Directory", item.Directory ?? "");
        params.set("FileType", "cfg");
        params.set("NetAddress", item.Netaddress ?? "");
        if (item.OS !== undefined) {
            params.set("OsType", item.OS.toString());
        }
        return `#/operations/servers/${encodeURIComponent(item.ParentName ?? "")}/config?${params.toString()}`;
    }, []);

    const auditLogsHref = React.useCallback((item: SystemServer): string => {
        const params = new URLSearchParams();
        params.set("NetAddress", item.Netaddress ?? "");
        params.set("LogDirectory", item.ParentAuditLogDirectory ?? "");
        return `#/operations/servers/${encodeURIComponent(item.ParentName ?? "")}/logs?${params.toString()}`;
    }, []);

    const componentLogsHref = React.useCallback((item: SystemServer): string => {
        const params = new URLSearchParams();
        params.set("NetAddress", item.Netaddress ?? "");
        params.set("LogDirectory", item.ParentLogDirectory ?? "");
        return `#/operations/servers/${encodeURIComponent(item.ParentName ?? "")}/logs?${params.toString()}`;
    }, []);

    const visibleItems = getVisibleItems();
    const selectableItems = visibleItems.filter(item => item.type === "machine" && item.MachineType !== "LDAPServerProcess");
    const allSelectableSelected = selectableItems.length > 0 && selectableItems.every(item => selectedItems.has(item.hpcc_id));

    const columnSizingOptions = React.useMemo<TableColumnSizingOptions>(() => ({
        selection: { minWidth: 24, idealWidth: 24 },
        configuration: { minWidth: 16, idealWidth: 16 },
        informational: { minWidth: 16, idealWidth: 16 },
        auditLogs: { minWidth: 18, idealWidth: 18 },
        componentLogs: { minWidth: 18, idealWidth: 18 },
        name: { minWidth: 180, idealWidth: 280 },
        queue: { minWidth: 80, idealWidth: 120 },
        node: { minWidth: 100, idealWidth: 140 },
        networkAddress: { minWidth: 140, idealWidth: 200 },
        directory: { minWidth: 200, idealWidth: 280 }
    }), []);

    const columns = React.useMemo<TableColumnDefinition<SystemServer>[]>(() => [
        createTableColumn<SystemServer>({
            columnId: "selection",
            renderHeaderCell: () => selectableItems.length > 0 ? (
                <Checkbox checked={allSelectableSelected} onChange={handleSelectAll} />
            ) : null,
            renderCell: (item) => (item.type === "machine" && item.MachineType !== "LDAPServerProcess") ? (
                <Checkbox checked={selectedItems.has(item.hpcc_id)} onChange={() => handleSelectionToggle(item)} />
            ) : <div style={{ width: "24px" }} />
        }),
        createTableColumn<SystemServer>({
            columnId: "configuration",
            renderHeaderCell: () => <div title={nlsHPCC.Configuration}><SettingsRegular /></div>,
            renderCell: (item) => (item.type === "machine" && item.Configuration) ? (
                <Link className={styles.iconButton} href={configurationHref(item)} title={nlsHPCC.Configuration}>
                    <SettingsRegular />
                </Link>
            ) : null
        }),
        createTableColumn<SystemServer>({
            columnId: "informational",
            renderHeaderCell: () => <div title={nlsHPCC.Informational}><InfoRegular /></div>,
            renderCell: (item) => item.Informational ? (
                <Link className={styles.iconButton} title={nlsHPCC.Informational} onClick={() => setBindingsItem(item)}>
                    <InfoRegular />
                </Link>
            ) : null
        }),
        createTableColumn<SystemServer>({
            columnId: "auditLogs",
            renderHeaderCell: () => <div title={nlsHPCC.AuditLogs}><DocumentQueueRegular /></div>,
            renderCell: (item) => (item.type === "machine" && item.AuditLog) ? (
                <Link className={styles.iconButton} href={auditLogsHref(item)} title={nlsHPCC.AuditLogs}>
                    <DocumentQueueRegular />
                </Link>
            ) : null
        }),
        createTableColumn<SystemServer>({
            columnId: "componentLogs",
            renderHeaderCell: () => <div title={nlsHPCC.ComponentLogs}><DocumentQueueRegular /></div>,
            renderCell: (item) => (item.type === "machine" && item.Log) ? (
                <Link className={styles.iconButton} href={componentLogsHref(item)} title={nlsHPCC.ComponentLogs}>
                    <DocumentQueueRegular />
                </Link>
            ) : null
        }),
        createTableColumn<SystemServer>({
            columnId: "name",
            renderHeaderCell: () => nlsHPCC.Name,
            renderCell: (item) => (
                <div className={styles.nameCell} style={getIndentLevel(item.type === "systemServer" ? 0 : 1)}>
                    {item.type === "systemServer" ? (
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
        createTableColumn<SystemServer>({
            columnId: "queue",
            renderHeaderCell: () => nlsHPCC.Queue,
            renderCell: (item) => <span>{item.Queue ?? ""}</span>
        }),
        createTableColumn<SystemServer>({
            columnId: "node",
            renderHeaderCell: () => nlsHPCC.Node,
            renderCell: (item) => <span>{item.Computer ?? ""}</span>
        }),
        createTableColumn<SystemServer>({
            columnId: "networkAddress",
            renderHeaderCell: () => nlsHPCC.NetworkAddress,
            renderCell: (item) => <span>{item.NetaddressWithPort ?? ""}</span>
        }),
        createTableColumn<SystemServer>({
            columnId: "directory",
            renderHeaderCell: () => nlsHPCC.Directory,
            renderCell: (item) => <span>{item.Directory ?? ""}</span>
        })
    ], [allSelectableSelected, auditLogsHref, componentLogsHref, configurationHref, expandedItems, getIndentLevel, handleExpansionToggle, handleSelectAll, handleSelectionToggle, selectableItems.length, selectedItems, styles.iconButton, styles.nameCell]);

    const bindingRows = bindingsItem?.ParentTpBindings?.TpBinding ?? [];

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
        <DetailDrawer
            open={bindingsItem !== null}
            title={nlsHPCC.ESPBindings}
            onClose={() => setBindingsItem(null)}
        >
            <Table className={styles.bindingsTable} size="small">
                <TableHeader>
                    <TableRow>
                        <TableHeaderCell>{nlsHPCC.ServiceName}</TableHeaderCell>
                        <TableHeaderCell>{nlsHPCC.ServiceType}</TableHeaderCell>
                        <TableHeaderCell>{nlsHPCC.Protocol}</TableHeaderCell>
                        <TableHeaderCell>{nlsHPCC.Port}</TableHeaderCell>
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {bindingRows.map((binding, idx) => (
                        <TableRow key={idx}>
                            <TableCell>{binding.Name ?? ""}</TableCell>
                            <TableCell>{binding.ServiceType ?? ""}</TableCell>
                            <TableCell>{binding.Protocol ?? ""}</TableCell>
                            <TableCell>{binding.Port ?? ""}</TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </DetailDrawer>
        <PreflightDialog
            open={showPreflightDialog}
            onClose={() => setShowPreflightDialog(false)}
            mode="systemServer"
            selectedTargets={selectedProcesses}
            url="/operations/servers/preflight"
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
