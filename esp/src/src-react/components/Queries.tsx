import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Icon, Link } from "@fluentui/react";
import * as WsWorkunits from "src/WsWorkunits";
import * as ESPQuery from "src/ESPQuery";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useFluentPagedGrid } from "../hooks/grid";
import { useMyAccount } from "../hooks/user";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
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

function formatQuery(filter: any, mine, currentUser) {
    const retVal = {
        ...filter,
        PriorityLow: filter.Priority,
        PriorityHigh: filter.Priority
    };
    delete retVal.Priority;
    if (mine === true) {
        retVal.Owner = currentUser?.username;
    }
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
    filter?: object;
    store?: any;
}

const emptyFilter = {};

export const Queries: React.FunctionComponent<QueriesProps> = ({
    wuid,
    filter = emptyFilter,
    store
}) => {

    const [showFilter, setShowFilter] = React.useState(false);
    const [mine, setMine] = React.useState(false);
    const { currentUser } = useMyAccount();
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    //  Grid ---
    const gridStore = React.useMemo(() => {
        return store || ESPQuery.CreateQueryStore({});
    }, [store]);

    const query = React.useMemo(() => {
        return formatQuery(filter, mine, currentUser);
    }, [filter, mine, currentUser]);

    const { Grid, GridPagination, selection, refreshTable, copyButtons } = useFluentPagedGrid({
        persistID: "queries",
        store: gridStore,
        query,
        filename: "roxiequeries",
        columns: {
            col1: {
                width: 27,
                selectorType: "checkbox"
            },
            Suspended: {
                headerIcon: "Pause",
                label: nlsHPCC.Suspended,
                width: 25,
                sortable: false,
                formatter: function (suspended) {
                    if (suspended === true) {
                        return <Icon iconName="Pause" />;
                    }
                    return "";
                }
            },
            ErrorCount: {
                headerIcon: "Warning",
                width: 25,
                sortable: false,
                formatter: function (error) {
                    if (error > 0) {
                        return <Icon iconName="Warning" />;
                    }
                    return "";
                }
            },
            MixedNodeStates: {
                headerIcon: "Error",
                width: 25,
                sortable: false,
                formatter: function (mixed) {
                    if (mixed === true) {
                        return <Icon iconName="Error" />;
                    }
                    return "";
                }
            },
            Activated: {
                headerIcon: "SkypeCircleCheck",
                width: 25,
                formatter: function (activated) {
                    if (activated === true) {
                        return <Icon iconName="SkypeCircleCheck" />;
                    }
                    return "";
                }
            },
            Id: {
                label: nlsHPCC.ID,
                width: 380,
                formatter: function (Id, row) {
                    return <Link href={`#/queries/${row.QuerySetId}/${Id}`} >{Id}</Link>;
                }
            },
            priority: {
                label: nlsHPCC.Priority,
                width: 80,
                formatter: function (priority, row) {
                    return priority === undefined ? "" : priority;
                }
            },
            Name: {
                label: nlsHPCC.Name
            },
            QuerySetId: {
                width: 140,
                label: nlsHPCC.Target,
                sortable: true
            },
            Wuid: {
                width: 160,
                label: nlsHPCC.WUID,
                formatter: function (Wuid, idx) {
                    return <Link href={`#/workunits/${Wuid}`}>{Wuid}</Link>;
                }
            },
            Dll: {
                width: 180,
                label: nlsHPCC.Dll
            },
            PublishedBy: {
                width: 100,
                label: nlsHPCC.PublishedBy,
                sortable: false
            },
            Status: {
                width: 100,
                label: nlsHPCC.Status,
                sortable: false
            }
        }
    });

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedWorkunits,
        items: selection.map(s => s.Id),
        onSubmit: React.useCallback(() => {
            WsWorkunits.WUQuerysetQueryAction(selection, "Delete").then(() => refreshTable(true));
        }, [refreshTable, selection])
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
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
            onClick: () => { WsWorkunits.WUQuerysetQueryAction(selection, "Suspend"); }
        },
        {
            key: "Unsuspend", text: nlsHPCC.Unsuspend, disabled: !uiState.isNotSuspended,
            onClick: () => { WsWorkunits.WUQuerysetQueryAction(selection, "Unsuspend"); }
        },
        {
            key: "Activate", text: nlsHPCC.Activate, disabled: !uiState.isActive,
            onClick: () => { WsWorkunits.WUQuerysetQueryAction(selection, "Activate"); }
        },
        {
            key: "Deactivate", text: nlsHPCC.Deactivate, disabled: !uiState.isNotActive,
            onClick: () => { WsWorkunits.WUQuerysetQueryAction(selection, "Deactivate"); }
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, disabled: store !== undefined || wuid !== undefined, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
        {
            key: "mine", text: nlsHPCC.Mine, disabled: !currentUser, iconProps: { iconName: "Contact" }, canCheck: true, checked: mine,
            onClick: () => {
                setMine(!mine);
            }
        },
    ], [currentUser, hasFilter, mine, refreshTable, selection, setShowDeleteConfirm, store, uiState.hasSelection, uiState.isActive, uiState.isNotActive, uiState.isNotSuspended, uiState.isSuspended, wuid]);

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
                            <Grid height={`${size.height}px`} />
                        </div>
                    </div>
                }</SizeMe>
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <DeleteConfirm />
            </>
        }
        footer={<GridPagination />}
        footerStyles={{}}
    />;
};
