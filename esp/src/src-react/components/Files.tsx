import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import * as domClass from "dojo/dom-class";
import * as put from "put-selector/put";
import * as WsDfu from "src/WsDfu";
import * as ESPLogicalFile from "src/ESPLogicalFile";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { Confirm } from "./controls/Confirm";
import { AddToSuperfile } from "./forms/AddToSuperfile";
import { CopyFile } from "./forms/CopyFile";
import { DesprayFile } from "./forms/DesprayFile";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { RemoteCopy } from "./forms/RemoteCopy";
import { RenameFile } from "./forms/RenameFile";
import { ShortVerticalDivider } from "./Common";
import { selector, tree } from "./DojoGrid";

const FilterFields: Fields = {
    "LogicalName": { type: "string", label: nlsHPCC.Name, placeholder: nlsHPCC.somefile },
    "Description": { type: "string", label: nlsHPCC.Description, placeholder: nlsHPCC.SomeDescription },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "Index": { type: "checkbox", label: nlsHPCC.Index },
    "NodeGroup": { type: "target-group", label: nlsHPCC.Group, placeholder: nlsHPCC.Cluster },
    "FileSizeFrom": { type: "string", label: nlsHPCC.FromSizes, placeholder: "4096" },
    "FileSizeTo": { type: "string", label: nlsHPCC.ToSizes, placeholder: "16777216" },
    "FileType": { type: "file-type", label: nlsHPCC.FileType },
    "FirstN": { type: "string", label: nlsHPCC.FirstN, placeholder: "-1" },
    // "Sortby": { type: "file-sortby", label: nlsHPCC.FirstNSortBy, disabled: (params: Fields) => !params.FirstN.value },
    "StartDate": { type: "datetime", label: nlsHPCC.FromDate, placeholder: "" },
    "EndDate": { type: "datetime", label: nlsHPCC.ToDate, placeholder: "" },
};

function formatQuery(_filter) {
    const filter = { ..._filter };
    if (filter.Index) {
        filter.ContentType = "key";
        delete filter.Index;
    }
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
};

interface FilesProps {
    filter?: object;
    store?: any;
}

const emptyFilter = {};

export const Files: React.FunctionComponent<FilesProps> = ({
    filter = emptyFilter,
    store
}) => {

    const [showFilter, setShowFilter] = React.useState(false);
    const [showRemoteCopy, setShowRemoteCopy] = React.useState(false);
    const [showCopy, setShowCopy] = React.useState(false);
    const [showRenameFile, setShowRenameFile] = React.useState(false);
    const [showAddToSuperfile, setShowAddToSuperfile] = React.useState(false);
    const [showDesprayFile, setShowDesprayFile] = React.useState(false);
    const [mine, setMine] = React.useState(false);
    const [selectedFileList, setSelectedFileList] = React.useState("");
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    const [showConfirm, setShowConfirm] = React.useState(false);
    const [confirmTitle, setConfirmTitle] = React.useState("");
    const [confirmMessage, setConfirmMessage] = React.useState("");
    const [confirmOnSubmit, setConfirmOnSubmit] = React.useState(null);

    //  Grid ---
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store: store || ESPLogicalFile.CreateLFQueryStore({}),
        query: formatQuery(filter),
        sort: [{ attribute: "Modified", "descending": true }],
        filename: "logicalfiles",
        columns: {
            col1: selector({
                width: 27,
                disabled: function (item) {
                    return item ? item.__hpcc_isDir : true;
                },
                selectorType: "checkbox"
            }),
            IsProtected: {
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
            IsCompressed: {
                width: 25, sortable: false,
                renderHeaderCell: function (node) {
                    node.innerHTML = Utility.getImageHTML("compressed.png", nlsHPCC.Compressed);
                },
                formatter: function (compressed) {
                    if (compressed === true) {
                        return Utility.getImageHTML("compressed.png");
                    }
                    return "";
                }
            },
            __hpcc_displayName: tree({
                label: nlsHPCC.LogicalName, width: 600,
                formatter: function (name, row) {
                    if (row.__hpcc_isDir) {
                        return name;
                    }
                    const url = "#/files/" + (row.NodeGroup ? row.NodeGroup + "/" : "") + name;
                    return (row.getStateImageHTML ? row.getStateImageHTML() + "&nbsp;" : "") + "<a href='" + url + "' class='dgrid-row-url'>" + name + "</a>";
                },
                renderExpando: function (level, hasChildren, expanded, object) {
                    const dir = this.grid.isRTL ? "right" : "left";
                    let cls = ".dgrid-expando-icon";
                    if (hasChildren) {
                        cls += ".ui-icon.ui-icon-triangle-1-" + (expanded ? "se" : "e");
                    }
                    //@ts-ignore
                    const node = put("div" + cls + "[style=margin-" + dir + ": " + (level * (this.indentWidth || 9)) + "px; float: " + dir + (!object.__hpcc_isDir && level === 0 ? ";display: none" : "") + "]");
                    node.innerHTML = "&nbsp;";
                    return node;
                }
            }),
            Owner: { label: nlsHPCC.Owner, width: 75 },
            SuperOwners: { label: nlsHPCC.SuperOwner, width: 150 },
            Description: { label: nlsHPCC.Description, width: 150 },
            NodeGroup: { label: nlsHPCC.Cluster, width: 108 },
            RecordCount: {
                label: nlsHPCC.Records, width: 85,
                renderCell: function (object, value, node, options) {
                    domClass.add(node, "justify-right");
                    node.innerText = Utility.valueCleanUp(value);
                },
            },
            IntSize: {
                label: nlsHPCC.Size, width: 100,
                renderCell: function (object, value, node, options) {
                    domClass.add(node, "justify-right");
                    node.innerText = Utility.convertedSize(value);
                },
            },
            Parts: {
                label: nlsHPCC.Parts, width: 60,
                renderCell: function (object, value, node, options) {
                    domClass.add(node, "justify-right");
                    node.innerText = Utility.valueCleanUp(value);
                },
            },
            Modified: { label: nlsHPCC.ModifiedUTCGMT, width: 162 }
        }
    });

    const deleteFiles = React.useCallback(() => {
        WsDfu.DFUArrayAction(selection, "Delete").then(() => refreshTable(true));
    }, [refreshTable, selection]);

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
                    window.location.href = "#/files/" + (selection[0].NodeGroup ? selection[0].NodeGroup + "/" : "") + selection[0].Name;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open("#/files/" + (selection[i].NodeGroup ? selection[i].NodeGroup + "/" : "") + selection[i].Name, "_blank");
                    }
                }
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection, iconProps: { iconName: "Delete" },
            onClick: () => {
                setConfirmTitle(nlsHPCC.Delete);
                setConfirmMessage(nlsHPCC.DeleteSelectedFiles + "\n\n" + selectedFileList);
                setConfirmOnSubmit(() => deleteFiles);
                setShowConfirm(true);
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "remoteCopy", text: nlsHPCC.RemoteCopy,
            onClick: () => setShowRemoteCopy(true)
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "copy", text: nlsHPCC.Copy, disabled: !uiState.hasSelection,
            onClick: () => setShowCopy(true)
        },
        {
            key: "rename", text: nlsHPCC.Rename, disabled: !uiState.hasSelection,
            onClick: () => setShowRenameFile(true)
        },
        {
            key: "addToSuperfile", text: nlsHPCC.AddToSuperfile, disabled: !uiState.hasSelection,
            onClick: () => setShowAddToSuperfile(true)
        },
        {
            key: "despray", text: nlsHPCC.Despray, disabled: !uiState.hasSelection,
            onClick: () => setShowDesprayFile(true)
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
    ], [deleteFiles, mine, refreshTable, selectedFileList, selection, store, uiState.hasSelection]);

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
            //  TODO:  More State
        }
        setSelectedFileList(selection.map(s => s.Name).join("\n"));
        setUIState(state);
    }, [selection]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
        main={
            <>
                <Grid />
                <Filter showFilter={showFilter} setShowFilter={setShowFilter} filterFields={filterFields} onApply={pushParams} />
                <RemoteCopy showForm={showRemoteCopy} setShowForm={setShowRemoteCopy} refreshGrid={refreshTable} />
                <CopyFile logicalFiles={selection.map(s => s.Name)} showForm={showCopy} setShowForm={setShowCopy} refreshGrid={refreshTable} />
                <RenameFile logicalFiles={selection.map(s => s.Name)} showForm={showRenameFile} setShowForm={setShowRenameFile} />
                <AddToSuperfile logicalFiles={selection.map(s => s.Name)} showForm={showAddToSuperfile} setShowForm={setShowAddToSuperfile} />
                <DesprayFile logicalFiles={selection.map(s => s.Name)} showForm={showDesprayFile} setShowForm={setShowDesprayFile} refreshGrid={refreshTable} />
                <Confirm
                    show={showConfirm} setShow={setShowConfirm}
                    title={confirmTitle} message={confirmMessage} onSubmit={confirmOnSubmit}
                />
            </>
        }
    />;
};
