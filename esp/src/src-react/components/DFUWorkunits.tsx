import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import * as domClass from "dojo/dom-class";
import * as ESPDFUWorkunit from "src/ESPDFUWorkunit";
import * as FileSpray from "src/FileSpray";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useConfirm } from "../hooks/confirm";
import { useGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { Filter } from "./forms/Filter";
import { Fields } from "./forms/Fields";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";

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

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    const [showFilter, setShowFilter] = React.useState(false);
    const [mine, setMine] = React.useState(false);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store: store || ESPDFUWorkunit.CreateWUQueryStore({}),
        query: formatQuery(filter),
        sort: [{ attribute: "Wuid", "descending": true }],
        filename: "dfuworkunits",
        columns: {
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
        }
    });

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedWorkunits,
        items: selection.map(s => s.Wuid),
        onSubmit: React.useCallback(() => {
            FileSpray.DFUWorkunitsAction(selection, nlsHPCC.Delete).then(() => refreshTable(true));
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
            onClick: () => setShowDeleteConfirm(true)
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
            key: "filter", text: nlsHPCC.Filter, disabled: !!store, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
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
    ], [hasFilter, mine, refreshTable, selection, setShowDeleteConfirm, store, uiState.hasNotProtected, uiState.hasProtected, uiState.hasSelection]);

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
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <Grid />
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <DeleteConfirm />
            </>
        }
    />;
};