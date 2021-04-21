import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as domClass from "dojo/dom-class";
import * as WsWorkunits from "src/WsWorkunits";
import * as ESPWorkunit from "src/ESPWorkunit";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { createCopyDownloadSelection, ShortVerticalDivider } from "./Common";
import { DojoGrid, selector } from "./DojoGrid";

const FilterFields: Fields = {
    "Type": { type: "checkbox", label: nlsHPCC.ArchivedOnly },
    "Wuid": { type: "string", label: nlsHPCC.WUID, placeholder: "W20200824-060035" },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "Jobname": { type: "string", label: nlsHPCC.JobName, placeholder: nlsHPCC.log_analysis_1 },
    "Cluster": { type: "target-cluster", label: nlsHPCC.Cluster, placeholder: "" },
    "State": { type: "workunit-state", label: nlsHPCC.State, placeholder: "" },
    "ECL": { type: "string", label: nlsHPCC.ECL, placeholder: nlsHPCC.dataset },
    "LogicalFile": { type: "string", label: nlsHPCC.LogicalFile, placeholder: nlsHPCC.somefile },
    "LogicalFileSearchType": { type: "logicalfile-type", label: nlsHPCC.LogicalFileType, placeholder: "", disabled: (params: Fields) => !params.LogicalFile.value },
    "LastNDays": { type: "string", label: nlsHPCC.LastNDays, placeholder: "2" },
    "StartDate": { type: "datetime", label: nlsHPCC.FromDate, placeholder: "" },
    "EndDate": { type: "datetime", label: nlsHPCC.ToDate, placeholder: "" },
};

function formatQuery(_filter) {
    const filter = { ..._filter };
    if (filter.LastNDays) {
        const end = new Date();
        const start = new Date();
        start.setDate(end.getDate() - filter.LastNDays);
        filter.StartDate = start.toISOString();
        filter.EndDate = end.toISOString();
        delete filter.LastNDays;
    } else {
        if (filter.StartDate) {
            filter.StartDate = new Date(filter.StartDate).toISOString();
        }
        if (filter.EndDate) {
            filter.EndDate = new Date(filter.StartDate).toISOString();
        }
    }
    return filter;
}

const defaultUIState = {
    hasSelection: false,
    hasProtected: false,
    hasNotProtected: false,
    hasFailed: false,
    hasNotFailed: false,
    hasCompleted: false,
    hasNotCompleted: false
};

interface WorkunitsProps {
    filter?: object;
    store?: any;
}

const emptyFilter = {};

export const Workunits: React.FunctionComponent<WorkunitsProps> = ({
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
                    window.location.href = `#/workunits/${selection[0].Wuid}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/workunits/${selection[i].Wuid}`, "_blank");
                    }
                }
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasNotProtected, iconProps: { iconName: "Delete" },
            onClick: () => {
                const list = selection.map(s => s.Wuid);
                if (confirm(nlsHPCC.DeleteSelectedWorkunits + "\n" + list)) {
                    WsWorkunits.WUAction(selection, "Delete").then(() => refreshTable(true));
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "setFailed", text: nlsHPCC.SetToFailed, disabled: !uiState.hasNotProtected,
            onClick: () => { WsWorkunits.WUAction(selection, "SetToFailed"); }
        },
        {
            key: "abort", text: nlsHPCC.Abort, disabled: !uiState.hasNotCompleted,
            onClick: () => { WsWorkunits.WUAction(selection, "Abort"); }
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "protect", text: nlsHPCC.Protect, disabled: !uiState.hasNotProtected,
            onClick: () => { WsWorkunits.WUAction(selection, "Protect"); }
        },
        {
            key: "unprotect", text: nlsHPCC.Unprotect, disabled: !uiState.hasProtected,
            onClick: () => { WsWorkunits.WUAction(selection, "Unprotect"); }
        },
        { key: "divider_4", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
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
        ...createCopyDownloadSelection(grid, selection, "workunits.csv")
    ];

    //  Grid ---
    const gridStore = useConst(store || ESPWorkunit.CreateWUQueryStore({}));
    const gridQuery = useConst(formatQuery(filter));
    const gridSort = useConst([{ attribute: "Wuid", "descending": true }]);
    const gridColumns = useConst({
        col1: selector({
            width: 27,
            selectorType: "checkbox"
        }),
        Protected: {
            renderHeaderCell: function (node) {
                node.innerHTML = Utility.getImageHTML("locked.png", nlsHPCC.Protected);
            },
            width: 25,
            sortable: false,
            formatter: function (_protected) {
                if (_protected === true) {
                    return Utility.getImageHTML("locked.png");
                }
                return "";
            }
        },
        Wuid: {
            label: nlsHPCC.WUID, width: 180,
            formatter: function (Wuid) {
                const wu = ESPWorkunit.Get(Wuid);
                return `${wu.getStateImageHTML()}&nbsp;<a href='#/workunits/${Wuid}' class='dgrid-row-url''>${Wuid}</a>`;
            }
        },
        Owner: { label: nlsHPCC.Owner, width: 90 },
        Jobname: { label: nlsHPCC.JobName, width: 500 },
        Cluster: { label: nlsHPCC.Cluster, width: 90 },
        RoxieCluster: { label: nlsHPCC.RoxieCluster, width: 99 },
        State: { label: nlsHPCC.State, width: 90 },
        TotalClusterTime: {
            label: nlsHPCC.TotalClusterTime, width: 117,
            renderCell: function (object, value, node) {
                domClass.add(node, "justify-right");
                node.innerText = value;
            }
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
    for (const fieldID in FilterFields) {
        filterFields[fieldID] = { ...FilterFields[fieldID], value: filter[fieldID] };
    }

    React.useEffect(() => {
        refreshTable();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [filter, store?.data]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
            if (selection[i] && selection[i].Protected !== null) {
                if (selection[i].Protected !== false) {
                    state.hasProtected = true;
                } else {
                    state.hasNotProtected = true;
                }
            }
            if (selection[i] && selection[i].StateID !== null) {
                if (selection[i].StateID === 4) {
                    state.hasFailed = true;
                } else {
                    state.hasNotFailed = true;
                }
                if (WsWorkunits.isComplete(selection[i].StateID, selection[i].ActionEx)) {
                    state.hasCompleted = true;
                } else {
                    state.hasNotCompleted = true;
                }
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
