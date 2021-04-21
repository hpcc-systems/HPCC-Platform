import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, mergeStyleSets } from "@fluentui/react";
import { useConst, useOnEvent } from "@fluentui/react-hooks";
import * as domClass from "dojo/dom-class";
import * as iframe from "dojo/request/iframe";
import * as put from "put-selector/put";
import { TpDropZoneQuery } from "src/WsTopology";
import * as FileSpray from "src/FileSpray";
import * as ESPRequest from "src/ESPRequest";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { DojoGrid, selector, tree } from "./DojoGrid";
import { Fields } from "./forms/Fields";
import { Filter } from "./forms/Filter";
import { AddFileForm } from "./forms/landing-zone/AddFileForm";
import { BlobImportForm } from "./forms/landing-zone/BlobImportForm";
import { DelimitedImportForm } from "./forms/landing-zone/DelimitedImportForm";
import { FixedImportForm } from "./forms/landing-zone/FixedImportForm";
import { JsonImportForm } from "./forms/landing-zone/JsonImportForm";
import { VariableImportForm } from "./forms/landing-zone/VariableImportForm";
import { XmlImportForm } from "./forms/landing-zone/XmlImportForm";
import { FileListForm } from "./forms/landing-zone/FileListForm";

function formatQuery(_filter) {
    return {
        DropZoneName: _filter.DropZoneName,
        Server: _filter.Server,
        NameFilter: _filter.NameFilter,
        ECLWatchVisibleOnly: true
    };
}

const FilterFields: Fields = {
    "DropZoneName": { type: "target-dropzone", label: nlsHPCC.DropZone },
    "Server": { type: "target-server", label: nlsHPCC.Server },
    "NameFilter": { type: "string", label: nlsHPCC.FileName, placeholder: nlsHPCC.somefile },
};

interface LandingZoneFilter {
    DropZoneName?: string;
    Server?: string;
    NameFilter?: string;
    ECLWatchVisibleOnly?: boolean;
    __dropZone?: any;
}

const emptyFilter: LandingZoneFilter = {};

interface LandingZoneProps {
    filter?: LandingZoneFilter;
    store?: any;
}

export const LandingZone: React.FunctionComponent<LandingZoneProps> = ({
    filter = emptyFilter,
    store
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [showFilter, setShowFilter] = React.useState(false);
    const [showAddFile, setShowAddFile] = React.useState(false);
    const [showFixed, setShowFixed] = React.useState(false);
    const [showDelimited, setShowDelimited] = React.useState(false);
    const [showXml, setShowXml] = React.useState(false);
    const [showJson, setShowJson] = React.useState(false);
    const [showVariable, setShowVariable] = React.useState(false);
    const [showBlob, setShowBlob] = React.useState(false);
    const [selection, setSelection] = React.useState([]);
    const [showDropZone, setShowDropzone] = React.useState(false);
    const [uploadFiles, setUploadFiles] = React.useState([]);
    const [showFileUpload, setShowFileUpload] = React.useState(false);
    const [targetDropzones, setTargetDropzones] = React.useState([]);

    React.useEffect(() => {
        TpDropZoneQuery({}).then(({ TpDropZoneQueryResponse }) => {
            setTargetDropzones(TpDropZoneQueryResponse?.TpDropZones?.TpDropZone || []);
        });
    }, []);

    //  Grid ---
    const gridStore = useConst(FileSpray.CreateLandingZonesStore({}));
    const gridQuery = useConst({});
    const gridSort = useConst([{ attribute: "modifiedtime", "descending": true }]);
    const gridColumns = useConst({
        col1: selector({
            width: 27,
            disabled: function (item) {
                if (item.type) {
                    switch (item.type) {
                        case "dropzone":
                        case "folder":
                        case "machine":
                            return true;
                    }
                }
                return false;
            },
            selectorType: "checkbox"
        }),
        displayName: tree({
            label: nlsHPCC.Name,
            sortable: false,
            formatter: function (_name, row) {
                let img = "";
                let name = row.displayName;
                if (row.isDir === undefined) {
                    img = Utility.getImageHTML("server.png");
                    name += " [" + row.Path + "]";
                } else if (row.isMachine) {
                    img = Utility.getImageHTML("machine.png");
                } else if (row.isDir) {
                    img = Utility.getImageHTML("folder.png");
                } else {
                    img = Utility.getImageHTML("file.png");
                }
                return img + "&nbsp;" + name;
            },
            renderExpando: function (level, hasChildren, expanded, object) {
                const dir = this.grid.isRTL ? "right" : "left";
                let cls = ".dgrid-expando-icon";
                if (hasChildren) {
                    cls += ".ui-icon.ui-icon-triangle-1-" + (expanded ? "se" : "e");
                }
                //@ts-ignore
                const node = put("div" + cls + "[style=margin-" + dir + ": " + (level * (this.indentWidth || 9)) + "px; float: " + dir + "; margin-top: 3px]");
                node.innerHTML = "&nbsp;";
                return node;
            }
        }),
        filesize: {
            label: nlsHPCC.Size, width: 100,
            renderCell: function (object, value, node, options) {
                domClass.add(node, "justify-right");
                node.innerText = Utility.convertedSize(value);
            },
        },
        modifiedtime: { label: nlsHPCC.Date, width: 162 }
    });

    const refreshTable = React.useCallback((clearSelection = false) => {
        const dropzones = targetDropzones.filter(row => row.Name === filter?.DropZoneName);
        const machines = targetDropzones[0]?.TpMachines?.TpMachine?.filter(row => row.ConfigNetaddress === filter?.Server);
        const query = {
            id: "*",
            filter: (filter?.DropZoneName && dropzones.length && machines.length) ? {
                ...formatQuery(filter),
                ECLWatchVisibleOnly: true,
                __dropZone: {
                    ...targetDropzones.filter(row => row.Name === filter?.DropZoneName)[0],
                    machine: machines[0]
                }
            } : undefined
        };
        grid?.set("query", query);
        if (clearSelection) {
            grid?.clearSelection();
        }
    }, [filter, grid, targetDropzones]);

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "preview", text: nlsHPCC.Preview, disabled: !selection.length, iconProps: { iconName: "ComplianceAudit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/landingzone/preview/${selection[0].getLogicalFile()}`;
                }
            }
        },
        { key: "divider_2", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "upload", text: nlsHPCC.Upload, iconProps: { iconName: "Upload" },
            onClick: () => {
                document.getElementById("uploaderBtn").click();
            }
        },
        {
            key: "download", text: nlsHPCC.Download, disabled: !selection.length, iconProps: { iconName: "Download" },
            onClick: () => {
                selection.forEach(item => {
                    const downloadIframeName = "downloadIframe_" + item.calculatedID;
                    const frame = iframe.create(downloadIframeName);
                    const url = ESPRequest.getBaseURL("FileSpray") + "/DownloadFile?Name=" + encodeURIComponent(item.name) + "&NetAddress=" + item.NetAddress + "&Path=" + encodeURIComponent(item.fullFolderPath) + "&OS=" + item.OS;
                    iframe.setSrc(frame, url, true);
                });
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !selection.length, iconProps: { iconName: "Delete" },
            onClick: () => {
                const list = selection.map(s => s.name);
                if (confirm(nlsHPCC.DeleteSelectedFiles + "\n" + list)) {
                    selection.forEach((item, idx) => {
                        if (item._isUserFile) {
                            gridStore.removeUserFile(item);
                            refreshTable(true);
                        } else {
                            FileSpray.DeleteDropZoneFile({
                                request: {
                                    NetAddress: item.NetAddress,
                                    Path: item.fullFolderPath,
                                    OS: item.OS,
                                    Names: item.name
                                },
                                load: function (response) {
                                    refreshTable(true);
                                }
                            });
                        }
                    });
                }
            }
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, disabled: !!store, iconProps: { iconName: "Filter" },
            onClick: () => setShowFilter(true)
        },
        { key: "divider_4", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "addFile", text: nlsHPCC.AddFile, disabled: !!store,
            onClick: () => setShowAddFile(true)
        },
        { key: "divider_5", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "fixed", text: nlsHPCC.Fixed, disabled: !selection.length,
            onClick: () => setShowFixed(true)
        },
        {
            key: "delimited", text: nlsHPCC.Delimited, disabled: !selection.length,
            onClick: () => setShowDelimited(true)
        },
        {
            key: "xml", text: nlsHPCC.XML, disabled: !selection.length,
            onClick: () => setShowXml(true)
        },
        {
            key: "json", text: nlsHPCC.JSON, disabled: !selection.length,
            onClick: () => setShowJson(true)
        },
        {
            key: "variable", text: nlsHPCC.Variable, disabled: !selection.length,
            onClick: () => setShowVariable(true)
        },
        {
            key: "blob", text: nlsHPCC.Blob, disabled: !selection.length,
            onClick: () => setShowBlob(true)
        },
        { key: "divider_6", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> }
    ], [gridStore, refreshTable, selection, store]);

    React.useEffect(() => {
        //  refreshTable changes when filter changes...
        refreshTable();
    }, [refreshTable]);

    //  Filter  ---
    const filterFields: Fields = {};
    for (const field in FilterFields) {
        filterFields[field] = { ...FilterFields[field], value: filter[field] };
    }

    const dropStyles = mergeStyleSets({
        dzWrapper: {
            position: "absolute",
            top: "118px",
            bottom: 0,
            background: "rgba(0, 0, 0, 0.12)",
            left: "240px",
            right: 0,
            display: showDropZone ? "block" : "none",
            zIndex: 1,
        },
        dzInner: {
            position: "absolute",
            background: "white",
            top: "20px",
            left: "30px",
            right: "40px",
            height: "80px",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            border: "1px solid #aaa",
            selectors: {
                p: {
                    fontSize: "1.333rem",
                    color: "#aaa"
                }
            }
        },
        displayNone: {
            display: "none"
        }
    });

    const handleFileDragEnter = React.useCallback((evt) => {
        evt.preventDefault();
        evt.stopPropagation();
        setShowDropzone(true);
    }, [setShowDropzone]);
    useOnEvent(document, "dragenter", handleFileDragEnter);

    const handleFileDragOver = React.useCallback((evt) => {
        evt.preventDefault();
        evt.stopPropagation();
        evt.dataTransfer.dropEffect = "copy";
    }, []);

    const handleFileDrop = React.useCallback((evt) => {
        evt.preventDefault();
        evt.stopPropagation();
        setShowDropzone(false);
        const files = [...evt.dataTransfer.files];
        setUploadFiles(files);
        setShowFileUpload(true);
    }, [setShowDropzone, setShowFileUpload, setUploadFiles]);

    const handleFileSelect = React.useCallback((evt) => {
        evt.preventDefault();
        evt.stopPropagation();
        const files = [...evt.target.files];
        setUploadFiles(files);
        setShowFileUpload(true);
    }, [setShowFileUpload, setUploadFiles]);

    return <HolyGrail
        header={<CommandBar items={buttons} />}
        main={
            <>
                <input
                    id="uploaderBtn" type="file" accept="*.txt, *.csv, *.json, *.xml"
                    className={dropStyles.displayNone} onChange={handleFileSelect} multiple={true}
                />
                <div className={dropStyles.dzWrapper} onDragOver={handleFileDragOver} onDrop={handleFileDrop}>
                    <div className={dropStyles.dzInner}>
                        <p>Drop file(s) to upload.</p>
                    </div>
                </div>
                <DojoGrid
                    store={gridStore} columns={gridColumns} query={gridQuery}
                    getSelected={function () {
                        if (filter?.__dropZone) {
                            return this.inherited(arguments, [FileSpray.CreateLandingZonesFilterStore({})]);
                        }
                        return this.inherited(arguments, [FileSpray.CreateFileListStore({})]);
                    }}
                    sort={gridSort} setGrid={setGrid} setSelection={setSelection}
                />
                <Filter
                    showFilter={showFilter} setShowFilter={setShowFilter}
                    filterFields={filterFields} onApply={pushParams}
                />
                { uploadFiles &&
                <FileListForm
                    formMinWidth={360} selection={uploadFiles}
                    showForm={showFileUpload} setShowForm={setShowFileUpload}
                    onSubmit={refreshTable}
                />
                }
                <AddFileForm
                    formMinWidth={620} store={gridStore} refreshGrid={refreshTable}
                    showForm={showAddFile} setShowForm={setShowAddFile}
                />
                <FixedImportForm
                    formMinWidth={620} selection={selection}
                    showForm={showFixed} setShowForm={setShowFixed}
                />
                <DelimitedImportForm
                    formMinWidth={620} selection={selection}
                    showForm={showDelimited} setShowForm={setShowDelimited}
                />
                <XmlImportForm
                    formMinWidth={620} selection={selection}
                    showForm={showXml} setShowForm={setShowXml}
                />
                <JsonImportForm
                    formMinWidth={620} selection={selection}
                    showForm={showJson} setShowForm={setShowJson}
                />
                <VariableImportForm
                    formMinWidth={620} selection={selection}
                    showForm={showVariable} setShowForm={setShowVariable}
                />
                <BlobImportForm
                    formMinWidth={620} selection={selection}
                    showForm={showBlob} setShowForm={setShowBlob}
                />
            </>
        }
    />;
};