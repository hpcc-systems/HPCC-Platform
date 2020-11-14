import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as domClass from "dojo/dom-class";
import * as ESPDFUWorkunit from "src/ESPDFUWorkunit";
import * as FileSpray from "src/FileSpray";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { Fields, Filter } from "./Filter";
import { ShortVerticalDivider } from "./Common";
import { DojoGrid, selector } from "./DojoGrid";

const FilterFields: Fields = {
    "Type": { type: "checkbox", label: nlsHPCC.ArchivedOnly },
    "Wuid": { type: "string", label: nlsHPCC.WUID, placeholder: "D20201203-171723" },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "Jobname": { type: "string", label: nlsHPCC.Jobname, placeholder: nlsHPCC.log_analysis_1 },
    "Cluster": { type: "target-cluster", label: nlsHPCC.Cluster, placeholder: nlsHPCC.Owner },
    "StateReq": { type: "dfuworkunit-state", label: nlsHPCC.State, placeholder: nlsHPCC.Created },
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
    hasProtected: false,
    hasNotProtected: false,
    hasFailed: false,
    hasNotFailed: false
};

interface DFUWorkunitsProps {
    filter?: object;
    store?: any;
}

const emptyFilter = {};

export const DFUWorkunits: React.FunctionComponent<DFUWorkunitsProps> = ({
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
                    window.location.href = `#/dfuworkunits/${selection[0].ID}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/dfuworkunits/${selection[i].ID}`, "_blank");
                    }
                }
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasNotProtected, iconProps: { iconName: "Delete" },
            onClick: () => {
                const list = selection.map(s => s.Wuid);
                if (confirm(nlsHPCC.DeleteSelectedWorkunits + "\n" + list)) {
                    FileSpray.DFUWorkunitsAction(selection, nlsHPCC.Delete).then(() => refreshTable(true));
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "setFailed", text: nlsHPCC.SetToFailed, disabled: !uiState.hasNotProtected,
            onClick: () => { FileSpray.DFUWorkunitsAction(selection, "SetToFailed"); }
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "protect", text: nlsHPCC.Protect, disabled: !uiState.hasNotProtected,
            onClick: () => { FileSpray.DFUWorkunitsAction(selection, "Protect"); }
        },
        {
            key: "unprotect", text: nlsHPCC.Unprotect, disabled: !uiState.hasProtected,
            onClick: () => { FileSpray.DFUWorkunitsAction(selection, "Unprotect"); }
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
    const gridStore = useConst(store || ESPDFUWorkunit.CreateWUQueryStore({}));
    const gridQuery = useConst(formatQuery(filter));
    const gridSort = useConst([{ attribute: "Wuid", "descending": true }]);
    const gridColumns = useConst({
        col1: selector({
            width: 27,
            selectorType: "checkbox"
        }),
        isProtected: {
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
        ID: {
            label: nlsHPCC.ID,
            width: 180,
            formatter: function (ID, idx) {
                const wu = ESPDFUWorkunit.Get(ID);
                return `<img src='${wu.getStateImage()}'>&nbsp;<a href='#/dfuworkunits/${ID}' class='dgrid-row-url'>${ID}</a>`;
            }
        },
        Command: {
            label: nlsHPCC.Type,
            width: 117,
            formatter: function (command) {
                if (command in FileSpray.CommandMessages) {
                    return FileSpray.CommandMessages[command];
                }
                return "Unknown";
            }
        },
        User: { label: nlsHPCC.Owner, width: 90 },
        JobName: { label: nlsHPCC.JobName, width: 500 },
        ClusterName: { label: nlsHPCC.Cluster, width: 126 },
        StateMessage: { label: nlsHPCC.State, width: 72 },
        PercentDone: {
            label: nlsHPCC.PctComplete, width: 90, sortable: false,
            renderCell: function (object, value, node, options) {
                domClass.add(node, "justify-right");
                node.innerText = Utility.valueCleanUp(value);
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
            if (selection[i] && selection[i].isProtected && selection[i].isProtected !== false) {
                state.hasProtected = true;
            } else {
                state.hasNotProtected = true;
            }
            if (selection[i] && selection[i].State && selection[i].State === 5) {
                state.hasFailed = true;
            } else {
                state.hasNotFailed = true;
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