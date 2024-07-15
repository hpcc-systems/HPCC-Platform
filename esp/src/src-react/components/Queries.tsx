import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Icon, Link } from "@fluentui/react";
import * as WsWorkunits from "src/WsWorkunits";
import * as ESPQuery from "src/ESPQuery";
import nlsHPCC from "src/nlsHPCC";
import { QuerySortItem } from "src/store/Store";
import { useConfirm } from "../hooks/confirm";
import { useMyAccount } from "../hooks/user";
import { useHasFocus, useIsMounted } from "../hooks/util";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { FluentPagedGrid, FluentPagedFooter, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { ShortVerticalDivider } from "./Common";
import { SizeMe } from "react-sizeme";

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

    const [showFilter, setShowFilter] = React.useState(false);
    const { currentUser } = useMyAccount();
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const {
        selection, setSelection,
        pageNum, setPageNum,
        pageSize, setPageSize,
        total, setTotal,
        refreshTable } = useFluentStoreState({ page });

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

    const columns = React.useMemo((): FluentColumns => {
        return {
            col1: {
                width: 16,
                selectorType: "checkbox"
            },
            Suspended: {
                headerIcon: "Pause",
                headerTooltip: nlsHPCC.Suspended,
                width: 16,
                sortable: false,
                formatter: (suspended) => {
                    if (suspended === true) {
                        return <Icon iconName="Pause" />;
                    }
                    return "";
                }
            },
            ErrorCount: {
                headerIcon: "Warning",
                headerTooltip: nlsHPCC.ErrorWarnings,
                width: 16,
                sortable: false,
                formatter: (error) => {
                    if (error > 0) {
                        return <Icon iconName="Warning" />;
                    }
                    return "";
                }
            },
            MixedNodeStates: {
                headerIcon: "Error",
                headerTooltip: nlsHPCC.MixedNodeStates,
                width: 16,
                sortable: false,
                formatter: (mixed) => {
                    if (mixed === true) {
                        return <Icon iconName="Error" />;
                    }
                    return "";
                }
            },
            Activated: {
                headerIcon: "SkypeCircleCheck",
                headerTooltip: nlsHPCC.Active,
                width: 16,
                formatter: (activated) => {
                    if (activated === true) {
                        return <Icon iconName="SkypeCircleCheck" />;
                    }
                    return "";
                }
            },
            Id: {
                label: nlsHPCC.ID,
                formatter: (Id, row) => {
                    return <Link href={`#/queries/${row.QuerySetId}/${Id}`} >{Id}</Link>;
                }
            },
            priority: {
                label: nlsHPCC.Priority,
                width: 80,
                formatter: (priority, row) => {
                    return priority === undefined ? "" : priority;
                }
            },
            Name: { label: nlsHPCC.Name },
            QuerySetId: { label: nlsHPCC.Target, sortable: true },
            Wuid: {
                label: nlsHPCC.WUID, width: 100,
                formatter: (Wuid, idx) => {
                    return <Link href={`#/workunits/${Wuid}`}>{Wuid}</Link>;
                }
            },
            Dll: { label: nlsHPCC.Dll },
            PublishedBy: { label: nlsHPCC.PublishedBy },
            Status: { label: nlsHPCC.Status, sortable: false }
        };
    }, []);

    const copyButtons = useCopyButtons(columns, selection, "roxiequeries");

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
                            <FluentPagedGrid
                                store={gridStore}
                                query={query}
                                sort={sort}
                                pageNum={pageNum}
                                pageSize={pageSize}
                                total={total}
                                columns={columns}
                                height={`${size.height}px`}
                                setSelection={setSelection}
                                setTotal={setTotal}
                                refresh={refreshTable}
                            ></FluentPagedGrid>
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
        footerStyles={{}}
    />;
};
