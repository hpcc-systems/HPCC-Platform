import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as WsWorkunits from "src/WsWorkunits";
import * as ESPQuery from "src/ESPQuery";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { Fields, Filter } from "./Filter";
import { ShortVerticalDivider } from "./Common";
import { DojoGrid, selector } from "./DojoGrid";

const FilterFields: Fields = {
    "QueryID": { type: "string", label: nlsHPCC.ID, placeholder: nlsHPCC.QueryIDPlaceholder },
    "QueryName": { type: "string", label: nlsHPCC.Name, placeholder: nlsHPCC.QueryNamePlaceholder },
    "PublishedBy": { type: "string", label: nlsHPCC.PublishedBy, placeholder: nlsHPCC.PublishedBy },
    "WUID": { type: "string", label: nlsHPCC.WUID, placeholder: "W20130222-171723" },
    "QuerySetName": { type: "target-cluster", label: nlsHPCC.Cluster },
    "FileName": { type: "string", label: nlsHPCC.FileName, placeholder: nlsHPCC.TargetNamePlaceholder },
    "LibraryName": { type: "string", label: nlsHPCC.LibraryName },
    "SuspendedFilter": { type: "queries-suspend-state", label: nlsHPCC.Suspended },
    "Activated": { type: "queries-active-state", label: nlsHPCC.Activated }
};

function formatQuery(filter) {
    if (filter.StartDate) {
        filter.StartDate = new Date(filter.StartDate).toISOString();
    }
    if (filter.EndDate) {
        filter.EndDate = new Date(filter.StartDate).toISOString();
    }
    return filter;
}

const defaultUIState = {
    hasSelection: false,
    isSuspended: false,
    isNotSuspended: false,
    isActive: false,
    isNotActive: false,
};

interface QueriesProps {
    filter?: object;
    store?: any;
}

const emptyFilter = {};

export const Queries: React.FunctionComponent<QueriesProps> = ({
    filter = emptyFilter,
    store
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [showFilter, setShowFilter] = React.useState(false);
    const [mine, setMine] = React.useState(false);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = [
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
            onClick: () => {
                const list = selection.map(s => s.Id);
                if (confirm(nlsHPCC.DeleteSelectedWorkunits + "\n" + list)) {
                    WsWorkunits.WUQuerysetQueryAction(selection, "Delete").then(() => refreshTable(true));
                }
            }
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
            key: "filter", text: nlsHPCC.Filter, disabled: !!store, iconProps: { iconName: "Filter" },
            onClick: () => {
                setShowFilter(true);
            }
        },
        {
            key: "mine", text: nlsHPCC.Mine, disabled: true, iconProps: { iconName: "Contact" }, canCheck: true, checked: mine,
            onClick: () => {
                setMine(!mine);
            }
        },
    ];

    const rightButtons: ICommandBarItemProps[] = [
        {
            key: "copy", text: nlsHPCC.CopyWUIDs, disabled: !uiState.hasSelection || !navigator?.clipboard?.writeText, iconOnly: true, iconProps: { iconName: "Copy" },
            onClick: () => {
                const wuids = selection.map(s => s.Wuid);
                navigator?.clipboard?.writeText(wuids.join("\n"));
            }
        },
        {
            key: "download", text: nlsHPCC.DownloadToCSV, disabled: !uiState.hasSelection, iconOnly: true, iconProps: { iconName: "Download" },
            onClick: () => {
                Utility.downloadToCSV(grid, selection.map(row => ([row.Protected, row.Wuid, row.Owner, row.Jobname, row.Cluster, row.RoxieCluster, row.State, row.TotalClusterTime])), "workunits.csv");
            }
        }
    ];

    //  Grid ---
    const gridStore = useConst(store || ESPQuery.CreateQueryStore({}));
    const gridQuery = useConst(formatQuery(filter));
    const gridSort = useConst([{ attribute: "Id" }]);
    const gridColumns = useConst({
        col1: selector({
            width: 27,
            selectorType: "checkbox"
        }),
        Suspended: {
            label: nlsHPCC.Suspended,
            renderHeaderCell: function (node) {
                node.innerHTML = Utility.getImageHTML("suspended.png", nlsHPCC.Suspended);
            },
            width: 25,
            sortable: false,
            formatter: function (suspended) {
                if (suspended === true) {
                    return Utility.getImageHTML("suspended.png");
                }
                return "";
            }
        },
        ErrorCount: {
            renderHeaderCell: function (node) {
                node.innerHTML = Utility.getImageHTML("errwarn.png", nlsHPCC.ErrorWarnings);
            },
            width: 25,
            sortable: false,
            formatter: function (error) {
                if (error > 0) {
                    return Utility.getImageHTML("errwarn.png");
                }
                return "";
            }
        },
        MixedNodeStates: {
            renderHeaderCell: function (node) {
                node.innerHTML = Utility.getImageHTML("mixwarn.png", nlsHPCC.MixedNodeStates);
            },
            width: 25,
            sortable: false,
            formatter: function (mixed) {
                if (mixed === true) {
                    return Utility.getImageHTML("mixwarn.png");
                }
                return "";
            }
        },
        Activated: {
            renderHeaderCell: function (node) {
                node.innerHTML = Utility.getImageHTML("active.png", nlsHPCC.Active);
            },
            width: 25,
            formatter: function (activated) {
                if (activated === true) {
                    return Utility.getImageHTML("active.png");
                }
                return Utility.getImageHTML("inactive.png");
            }
        },
        Id: {
            label: nlsHPCC.ID,
            width: 380,
            formatter: function (Id, row) {
                return `<a href='#/queries/${row.QuerySetId}/${Id}' class='dgrid-row-url'>${Id}</a>`;
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
                return "<a href='#' onClick='return false;' class='dgrid-row-url2'>" + Wuid + "</a>";
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
    });

    const refreshTable = (clearSelection = false) => {
        grid?.set("query", formatQuery(filter));
        if (clearSelection) {
            grid?.clearSelection();
        }
    };

    //  Filter  ---
    const filterFields: Fields = {};
    for (const field in FilterFields) {
        filterFields[field] = { ...FilterFields[field], value: filter[field] };
    }

    React.useEffect(() => {
        refreshTable();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [filter]);

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
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <>
                <DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
            </>
        }
    />;
};
