import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as domClass from "dojo/dom-class";
import * as put from "put-selector/put";
import * as WsDfu from "src/WsDfu";
import * as ESPLogicalFile from "src/ESPLogicalFile";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { Fields, Filter } from "./Filter";
import { ShortVerticalDivider } from "./Common";
import { DojoGrid, selector, tree } from "./DojoGrid";

const FilterFields: Fields = {
    "LogicalName": { type: "string", label: nlsHPCC.Name, placeholder: nlsHPCC.somefile },
    "Description": { type: "string", label: nlsHPCC.Description, placeholder: nlsHPCC.SomeDescription },
    "Owner": { type: "string", label: nlsHPCC.Owner, placeholder: nlsHPCC.jsmi },
    "Index": { type: "checkbox", label: nlsHPCC.Index },
    "NodeGroup": { type: "target-group", label: nlsHPCC.Cluster, placeholder: nlsHPCC.Owner },
    "FileSizeFrom": { type: "string", label: nlsHPCC.FromSizes, placeholder: "4096" },
    "FileSizeTo": { type: "string", label: nlsHPCC.ToSizes, placeholder: "16777216" },
    "FileType": { type: "file-type", label: nlsHPCC.FileType },
    "FirstN": { type: "string", label: nlsHPCC.FirstN, placeholder: "-1" },
    // "Sortby": { type: "file-sortby", label: nlsHPCC.FirstNSortBy, disabled: (params: Fields) => !params.FirstN.value },
    "StartDate": { type: "datetime", label: nlsHPCC.FromDate, placeholder: "" },
    "EndDate": { type: "datetime", label: nlsHPCC.ToDate, placeholder: "" },
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
                    window.location.href = `#/files/${selection[0].Name}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/files/${selection[i].Name}`, "_blank");
                    }
                }
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !uiState.hasSelection, iconProps: { iconName: "Delete" },
            onClick: () => {
                const list = selection.map(s => s.Name);
                if (confirm(nlsHPCC.DeleteSelectedFiles + "\n" + list)) {
                    WsDfu.DFUArrayAction(selection, "Delete").then(() => refreshTable(true));
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
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
            key: "copy", text: nlsHPCC.CopyLogicalFiles, disabled: !uiState.hasSelection || !navigator?.clipboard?.writeText, iconOnly: true, iconProps: { iconName: "Copy" },
            onClick: () => {
                const wuids = selection.map(s => s.Name);
                navigator?.clipboard?.writeText(wuids.join("\n"));
            }
        },
        {
            key: "download", text: nlsHPCC.DownloadToCSV, disabled: !uiState.hasSelection, iconOnly: true, iconProps: { iconName: "Download" },
            onClick: () => {
                Utility.downloadToCSV(grid, selection.map(row => ([row.IsProtected, row.IsCompressed, row.IsKeyFile, row.__hpcc_displayName, row.Owner, row.SuperOwners, row.Description, row.NodeGroup, row.RecordCount, row.IntSize, row.Parts, row.Modified])), "workunits.csv");
            }
        }
    ];

    //  Grid ---
    const gridStore = useConst(store || ESPLogicalFile.CreateLFQueryStore({}));
    const gridQuery = useConst(formatQuery(filter));
    const gridSort = useConst([{ attribute: "Modified", "descending": true }]);
    const gridColumns = useConst({
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
        IsKeyFile: {
            width: 25, sortable: false,
            renderHeaderCell: function (node) {
                node.innerHTML = Utility.getImageHTML("index.png", nlsHPCC.Index);
            },
            formatter: function (keyfile, row) {
                if (row.ContentType === "key") {
                    return Utility.getImageHTML("index.png");
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
                return (row.getStateImageHTML ? row.getStateImageHTML() + "&nbsp;" : "") + "<a href='#/files/" + name + "' class='dgrid-row-url'>" + name + "</a>";
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
            //  TODO:  More State
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
