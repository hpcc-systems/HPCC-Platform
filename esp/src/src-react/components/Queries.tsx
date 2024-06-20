import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Link } from "@fluentui/react";
import { TableCellLayout, TableColumnDefinition, TableRowId, createTableColumn, Tooltip } from "@fluentui/react-components";
import { CheckmarkCircleFilled, ImportantFilled, ImportantRegular, PauseFilled, WarningRegular } from "@fluentui/react-icons";
import { WsWorkunits as HPCCWsWorkunits } from "@hpcc-js/comms";
import { SizeMe } from "react-sizeme";
import * as WsWorkunits from "src/WsWorkunits";
import * as ESPQuery from "src/ESPQuery";
import { ColumnMap } from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useConfirm } from "../hooks/confirm";
import { useUserTheme } from "../hooks/theme";
import { useMyAccount } from "../hooks/user";
import { useHasFocus, useIsMounted } from "../hooks/util";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { FluentPagedDataGrid, FluentPagedFooter, useCopyButtons, useFluentStoreState } from "./controls/Grid";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { ShortVerticalDivider } from "./Common";

type Query = HPCCWsWorkunits.QuerySetQuery;

const FilterFields: Fields = {
    "QueryID": { type: "string", label: nlsHPCC.ID, placeholder: nlsHPCC.QueryIDPlaceholder },
    "Priority": { type: "queries-priority", label: nlsHPCC.Priority },
    "QueryName": { type: "string", label: nlsHPCC.Name, placeholder: nlsHPCC.QueryNamePlaceholder },
    "PublishedBy": { type: "string", label: nlsHPCC.PublishedBy, placeholder: nlsHPCC.PublishedBy },
    "WUID": { type: "string", label: nlsHPCC.WUID, placeholder: "W20130222-171723" },
    "QuerySetName": { type: "target-cluster", label: nlsHPCC.Cluster },
    "FileName": { type: "string", label: nlsHPCC.FileName, placeholder: nlsHPCC.TargetNamePlaceholder },
    "LibraryName": { type: "string", label: nlsHPCC.LibraryName },
    "SuspendedFilter": { type: "queries-suspend-state", label: nlsHPCC.Suspended },
    "Activated": { type: "queries-active-state", label: nlsHPCC.Activated }
};

function formatQuery(filter: any): { [id: string]: any } {
    const retVal = {
        ...filter,
        PriorityLow: filter.Priority,
        PriorityHigh: filter.Priority
    };
    delete retVal.Priority;
    return retVal;
}

const defaultUIState = {
    hasSelection: false,
    isSuspended: false,
    isNotSuspended: false,
    isActive: false,
    isNotActive: false,
};

interface QueriesProps {
    wuid?: string;
    filter?: { [id: string]: any };
    sort?: QuerySortItem;
    store?: any;
    page?: number;
}

const emptyFilter = {};
const defaultSort = { attribute: undefined, descending: false };

export const Queries: React.FunctionComponent<QueriesProps> = ({
    wuid,
    filter = emptyFilter,
    sort = defaultSort,
    page = 1,
    store
}) => {

    const { themeV9 } = useUserTheme();
    const footerStyles = React.useMemo(() => {
        return {
            zIndex: 2,
            background: themeV9.colorNeutralBackground1,
            borderTop: `1px solid ${themeV9.colorNeutralStroke1}`
        };
    }, [themeV9]);

    const [showFilter, setShowFilter] = React.useState(false);
    const { currentUser } = useMyAccount();
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const {
        selection, setSelection,
        pageNum, setPageNum,
        pageSize, setPageSize,
        total, setTotal,
        refreshTable } = useFluentStoreState({ page });

    const [selectedRows, setSelectedRows] = React.useState(new Set<TableRowId>());
    const onSelectionChange = (items, rowIds) => {
        setSelectedRows(rowIds);
        setSelection(items);
    };

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    //  Refresh on focus  ---
    const isMounted = useIsMounted();
    const hasFocus = useHasFocus();
    React.useEffect(() => {
        if (isMounted && hasFocus) {
            refreshTable.call();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [hasFocus]);

    //  Grid ---
    const gridStore = React.useMemo(() => {
        return store || ESPQuery.CreateQueryStore({});
    }, [store]);

    const query = React.useMemo(() => {
        return formatQuery(filter);
    }, [filter]);

    const columnSizingOptions = React.useMemo(() => {
        return {
            Suspended: { minWidth: 16, defaultWidth: 16 },
            ErrorCount: { minWidth: 16, defaultWidth: 16 },
            MixedNodeStates: { minWidth: 16, defaultWidth: 16 },
            Activated: { minWidth: 16, defaultWidth: 16 },
        };
    }, []);

    const columns: TableColumnDefinition<Query>[] = React.useMemo(() => [
        createTableColumn<Query>({
            columnId: "Suspended",
            renderHeaderCell: () => <Tooltip content={nlsHPCC.Suspended} relationship="label"><PauseFilled fontSize={18} /></Tooltip>,
            renderCell: (cell) => cell.Suspended ? <PauseFilled /> : "",
        }),
        createTableColumn<Query>({
            columnId: "ErrorCount",
            renderHeaderCell: () => <Tooltip content={nlsHPCC.ErrorWarnings} relationship="label"><WarningRegular fontSize={18} /></Tooltip>,
            renderCell: (cell) => cell.Clusters?.ClusterQueryState[0]?.Errors ? <WarningRegular /> : "",
        }),
        createTableColumn<Query>({
            columnId: "MixedNodeStates",
            renderHeaderCell: () => <Tooltip content={nlsHPCC.MixedNodeStates} relationship="label"><ImportantFilled title={nlsHPCC.MixedNodeStates} fontSize={18} /></Tooltip>,
            renderCell: (cell) => cell.Clusters?.ClusterQueryState[0]?.MixedNodeStates ? <ImportantRegular /> : "",
        }),
        createTableColumn<Query>({
            columnId: "Activated",
            renderHeaderCell: () => <Tooltip content={nlsHPCC.Activated} relationship="label"><CheckmarkCircleFilled fontSize={18} /></Tooltip>,
            renderCell: (cell) => cell.Activated ? <CheckmarkCircleFilled /> : "",
        }),
        createTableColumn<Query>({
            columnId: "Id",
            compare: (a, b) => a.Id.localeCompare(b.Id),
            renderHeaderCell: () => nlsHPCC.ID,
            renderCell: (cell) => <TableCellLayout><Link href={`#/queries/${cell.QuerySetId}/${cell.Id}`}>{cell.Id}</Link></TableCellLayout>,
        }),
        createTableColumn<Query>({
            columnId: "priority",
            compare: (a, b) => a.priority.localeCompare(b.priority),
            renderHeaderCell: () => nlsHPCC.Priority,
            renderCell: (cell) => <TableCellLayout>{cell.priority}</TableCellLayout>,
        }),
        createTableColumn<Query>({
            columnId: "Name",
            compare: (a, b) => a.Name.localeCompare(b.Name),
            renderHeaderCell: () => nlsHPCC.Name,
            renderCell: (cell) => <TableCellLayout>{cell.Name}</TableCellLayout>,
        }),
        createTableColumn<Query>({
            columnId: "QuerySetId",
            compare: (a, b) => a.QuerySetId.localeCompare(b.QuerySetId),
            renderHeaderCell: () => nlsHPCC.Target,
            renderCell: (cell) => <TableCellLayout>{cell.QuerySetId}</TableCellLayout>,
        }),
        createTableColumn<Query>({
            columnId: "Wuid",
            compare: (a, b) => a.Wuid.localeCompare(b.Wuid),
            renderHeaderCell: () => nlsHPCC.WUID,
            renderCell: (cell) => <TableCellLayout><Link href={`#/workunits/${cell.Wuid}`}>{cell.Wuid}</Link></TableCellLayout>,
        }),
        createTableColumn<Query>({
            columnId: "Dll",
            compare: (a, b) => a.Dll.localeCompare(b.Dll),
            renderHeaderCell: () => nlsHPCC.CompileCost,
            renderCell: (cell) => <TableCellLayout>{cell.Dll}</TableCellLayout>,
        }),
        createTableColumn<Query>({
            columnId: "PublishedBy",
            compare: (a, b) => a.PublishedBy.localeCompare(b.PublishedBy),
            renderHeaderCell: () => nlsHPCC.PublishedBy,
            renderCell: (cell) => <TableCellLayout>{cell.PublishedBy}</TableCellLayout>,
        }),
        createTableColumn<Query>({
            columnId: "Status",
            compare: (a, b) => {
                let statusA = "";
                let statusB = "";
                if (a.Suspended) {
                    statusA = nlsHPCC.SuspendedByUser;
                }
                a?.Clusters?.ClusterQueryState?.some(state => {
                    if (state.Errors || state.State !== "Available") {
                        statusA = nlsHPCC.SuspendedByCluster;
                    } else if (state.MixedNodeStates) {
                        statusA = nlsHPCC.MixedNodeStates;
                    }
                });
                b?.Clusters?.ClusterQueryState?.some(state => {
                    if (state.Errors || state.State !== "Available") {
                        statusB = nlsHPCC.SuspendedByCluster;
                    } else if (state.MixedNodeStates) {
                        statusB = nlsHPCC.MixedNodeStates;
                    }
                });
                return statusA.localeCompare(statusB);
            },
            renderHeaderCell: () => nlsHPCC.Status,
            renderCell: (cell) => {
                let statusMsg = "";
                if (cell.Suspended) {
                    statusMsg = nlsHPCC.SuspendedByUser;
                }
                cell?.Clusters?.ClusterQueryState?.some(state => {
                    if (state.Errors || state.State !== "Available") {
                        statusMsg = nlsHPCC.SuspendedByCluster;
                    } else if (state.MixedNodeStates) {
                        statusMsg = nlsHPCC.MixedNodeStates;
                    }
                });
                return <TableCellLayout>{(statusMsg)}</TableCellLayout>;
            },
        }),
    ], []);

    const columnMap: ColumnMap = React.useMemo(() => {
        const retVal: ColumnMap = {};
        columns.forEach((col, idx) => {
            const columnId = col.columnId.toString();
            retVal[columnId] = {
                id: `${columnId}_${idx}`,
                field: columnId,
                label: columnId
            };
        });
        return retVal;
    }, [columns]);

    const copyButtons = useCopyButtons(columnMap, selection, "roxiequeries");

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedQueries,
        items: selection.map(s => s.Id),
        onSubmit: React.useCallback(() => {
            WsWorkunits.WUQuerysetQueryAction(selection, "Delete").then(() => refreshTable.call(true));
        }, [refreshTable, selection])
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable.call()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/queries/${selection[0].QuerySetId}/${selection[0].Id}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/queries/${selection[i].QuerySetId}/${selection[i].Id}`, "_blank");
                    }
                }
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection, iconProps: { iconName: "Delete" },
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "Suspend", text: nlsHPCC.Suspend, disabled: !uiState.isSuspended,
            onClick: () => {
                WsWorkunits.WUQuerysetQueryAction(selection, "Suspend").then(() => refreshTable.call());
            }
        },
        {
            key: "Unsuspend", text: nlsHPCC.Unsuspend, disabled: !uiState.isNotSuspended,
            onClick: () => {
                WsWorkunits.WUQuerysetQueryAction(selection, "Unsuspend").then(() => refreshTable.call());
            }
        },
        {
            key: "Activate", text: nlsHPCC.Activate, disabled: !uiState.isActive,
            onClick: () => {
                WsWorkunits.WUQuerysetQueryAction(selection, "Activate").then(() => refreshTable.call());
            }
        },
        {
            key: "Deactivate", text: nlsHPCC.Deactivate, disabled: !uiState.isNotActive,
            onClick: () => {
                WsWorkunits.WUQuerysetQueryAction(selection, "Deactivate").then(() => refreshTable.call());
            }
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, disabled: store !== undefined || wuid !== undefined, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
        {
            key: "mine", text: nlsHPCC.Mine, disabled: !currentUser?.username || !total, iconProps: { iconName: "Contact" }, canCheck: true, checked: filter["PublishedBy"] === currentUser.username,
            onClick: () => {
                if (filter["PublishedBy"] === currentUser.username) {
                    filter["PublishedBy"] = "";
                } else {
                    filter["PublishedBy"] = currentUser.username;
                }
                pushParams(filter);
            }
        },
    ], [currentUser.username, filter, hasFilter, refreshTable, selection, setShowDeleteConfirm, store, total, uiState.hasSelection, uiState.isActive, uiState.isNotActive, uiState.isNotSuspended, uiState.isSuspended, wuid]);

    //  Filter  ---
    const filterFields: Fields = {};
    for (const field in FilterFields) {
        filterFields[field] = { ...FilterFields[field], value: filter[field] };
    }

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
            if (selection[i].Suspended !== true) {
                state.isSuspended = true;
            } else {
                state.isNotSuspended = true;
            }
            if (selection[i].Activated !== true) {
                state.isActive = true;
            } else {
                state.isNotActive = true;
            }
        }

        setUIState(state);
    }, [selection]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <SizeMe monitorHeight>{({ size }) =>
                    <div style={{ position: "relative", width: "100%", height: "100%" }}>
                        <div style={{ position: "absolute", width: "100%", height: `${size.height}px` }}>
                            <FluentPagedDataGrid
                                store={gridStore}
                                query={query}
                                sort={sort}
                                pageNum={pageNum}
                                pageSize={pageSize}
                                total={total}
                                columns={columns}
                                sizingOptions={columnSizingOptions}
                                height={"calc(100vh - 176px)"}
                                onSelect={onSelectionChange}
                                selectedItems={selectedRows}
                                setSelection={setSelection}
                                setTotal={setTotal}
                                refresh={refreshTable}
                            ></FluentPagedDataGrid>
                        </div>
                    </div>
                }</SizeMe>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <DeleteConfirm />
            </>
        }
        footer={<FluentPagedFooter
            persistID={"queries"}
            pageNum={pageNum}
            selectionCount={selection.length}
            setPageNum={setPageNum}
            setPageSize={setPageSize}
            total={total}
        ></FluentPagedFooter>}
        footerStyles={footerStyles}
    />;
};
