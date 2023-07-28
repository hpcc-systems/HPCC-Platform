import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, mergeStyleSets } from "@fluentui/react";
import { TreeItemValue, TreeOpenChangeData, TreeOpenChangeEvent } from "@fluentui/react-components";
import { useOnEvent, useConst } from "@fluentui/react-hooks";
import { FileSpray as HPCCFileSpray, FileSprayService, TopologyService, WsTopology } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import * as iframe from "dojo/request/iframe";
import * as FileSpray from "src/FileSpray";
import * as ESPRequest from "src/ESPRequest";
import * as Utility from "src/Utility";
import { userKeyValStore } from "src/KeyValStore";
import nlsHPCC from "src/nlsHPCC";
import { BranchIcon, FlatItem, TreeView } from "./controls/TreeView";
import { useBuildInfo } from "../hooks/platform";
import { useConfirm } from "../hooks/confirm";
import { FluentGrid, useCopyButtons, useFluentStoreState, FluentColumns } from "./controls/Grid";
import { DockPanel, DockPanelItem, ResetableDockPanel } from "../layouts/DockPanel";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams, pushUrl } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";
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

const logger = scopedLogger("src-react/components/LandingZone.tsx");

const topologyService = new TopologyService({ baseUrl: "" });
const fsService = new FileSprayService({ baseUrl: "" });

const buttonStyles = mergeStyleSets({
    labelOnly: {
        ":hover": {
            background: "initial",
            cursor: "initial"
        }
    }
});

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
    path?: string;
}

export const LandingZone: React.FunctionComponent<LandingZoneProps> = ({
    filter = emptyFilter,
    path = ""
}) => {

    const [, { isContainer }] = useBuildInfo();

    const hasFilter = React.useMemo(() => Object.keys(filter).length > 0, [filter]);

    const [showFilter, setShowFilter] = React.useState(false);
    const [showAddFile, setShowAddFile] = React.useState(false);
    const [showFixed, setShowFixed] = React.useState(false);
    const [showDelimited, setShowDelimited] = React.useState(false);
    const [showXml, setShowXml] = React.useState(false);
    const [showJson, setShowJson] = React.useState(false);
    const [showVariable, setShowVariable] = React.useState(false);
    const [showBlob, setShowBlob] = React.useState(false);
    const [showDropZone, setShowDropzone] = React.useState(false);
    const [uploadFiles, setUploadFiles] = React.useState([]);
    const [showFileUpload, setShowFileUpload] = React.useState(false);
    const [layout, setLayout] = React.useState<object>();
    const [dockpanel, setDockpanel] = React.useState<ResetableDockPanel>();
    const [dropzones, setDropzones] = React.useState<WsTopology.TpDropZone[]>([]);
    const [selectedDropzone, setSelectedDropzone] = React.useState<WsTopology.TpDropZone>();

    const [data, setData] = React.useState<any[]>([]);
    const {
        selection, setSelection,
        setTotal,
        refreshTable } = useFluentStoreState({});

    const [userAddedFiles, setUserAddedFiles] = React.useState<any[]>([]);
    const [lzPath, setLzPath] = React.useState<string>("");
    const [pathSep, setPathSep] = React.useState<string>("/");

    const [treeItems, setTreeItems] = React.useState<FlatItem[]>([]);
    const [openItems, setOpenItems] = React.useState<Iterable<TreeItemValue>>([]);

    const store = useConst(() => userKeyValStore());

    React.useEffect(() => {
        if (dockpanel) {
            //  Should only happen once on startup  ---
            store.get("LzLayout").then(value => {
                if (!value || value === "undefined") {
                    const layout: any = dockpanel.layout();
                    if (Array.isArray(layout?.main?.sizes) && layout.main.sizes.length === 2) {
                        layout.main.sizes = [0.2, 0.8];
                        dockpanel.layout(layout).lazyRender();
                        setLayout(layout);
                        store?.set("LzLayout", JSON.stringify(layout), true);
                    }
                } else {
                    const layout = JSON.parse(value);
                    dockpanel.layout(layout);
                }
            });
        }
    }, [dockpanel, store]);

    React.useEffect(() => {
        //  Update layout prior to unmount  ---
        if (dockpanel && store) {
            return () => {
                const layout: any = dockpanel.getLayout();
                store?.set("LzLayout", JSON.stringify(layout), true);
            };
        }
    }, [dockpanel, store]);

    React.useEffect(() => {
        topologyService.TpDropZoneQuery({ ECLWatchVisibleOnly: true }).then(response => {
            const dropzones = response?.TpDropZones?.TpDropZone || [];
            if (dropzones[0]?.Path?.indexOf("\\") > -1) {
                setPathSep("\\");
            }
            setDropzones(dropzones);
            if (dropzones.length) {
                setSelectedDropzone(dropzones[0]);
            }
        });
    }, []);

    const addUserFile = React.useCallback((file) => {
        setData([...data, file]);
        setUserAddedFiles([...userAddedFiles, file]);
    }, [data, userAddedFiles]);

    const removeUserFile = React.useCallback((name) => {
        setData(data.filter(file => file.name !== name));
        setUserAddedFiles(userAddedFiles.filter(file => file.name !== name));
    }, [data, userAddedFiles]);

    React.useEffect(() => {
        if (!dropzones || !selectedDropzone?.Path) return;

        const _path = path ? path.replace(/::/g, "/") : selectedDropzone.Path;
        setLzPath(_path);

        const paths = [];
        const items = [];

        dropzones.forEach(dz => {
            items.push({
                value: dz?.Name,
                label: dz?.Name,
                icon: BranchIcon.Dropzone,
                data: {
                    DropZoneName: dz?.Name,
                    path: dz?.Path
                }
            });
            dz?.TpMachines.TpMachine.forEach(machine => {
                items.push({
                    value: machine.Directory,
                    parentValue: dz.Name,
                    label: machine.Name,
                    icon: BranchIcon.Network,
                    data: {
                        DropZoneName: dz.Name,
                        path: machine.Directory
                    }
                });
            });
        });

        const openItems = new Set<TreeItemValue>([selectedDropzone.Name, selectedDropzone.Path]);
        const requests = [];

        let pathParts = _path.split("/");
        let tempPath = _path;

        while (tempPath !== selectedDropzone?.Path) {
            paths.push(tempPath);
            pathParts = pathParts.slice(0, -1);
            tempPath = pathParts.join("/");
        }
        paths.push(selectedDropzone.Path);

        paths.reverse().forEach(path => {
            requests.push(fsService.FileList({
                DropZoneName: selectedDropzone.Name,
                Netaddr: selectedDropzone?.TpMachines?.TpMachine[0].Netaddress ?? "",
                Path: path,
                DirectoryOnly: true
            }));
            openItems.add(path);
        });

        Promise.all(requests).then(responses => {
            responses.forEach(response => {
                const files = response?.files?.PhysicalFileStruct?.sort((a, b) => {
                    if (a.name < b.name) return -1;
                    if (a.name > b.name) return 1;
                    return 0;
                }) ?? [];
                files?.forEach(file => {
                    const parentPath = (file.Path ?? response.Path + "/");
                    const itemPath = parentPath + file.name;
                    if (items.filter(item => item.value === itemPath).length === 0) {
                        items.push({
                            value: itemPath,
                            parentValue: parentPath.slice(0, -1),
                            label: file.name,
                            icon: file.isDir ? BranchIcon.Directory : BranchIcon.None,
                            data: { path: itemPath }
                        });
                    }
                    if (itemPath.length <= _path) {
                        openItems.add(itemPath);
                    }
                });
            });

            setTreeItems(items);
            setOpenItems(openItems);
        });

    }, [dropzones, path, selectedDropzone]);

    //  Grid ---
    const columns = React.useMemo((): FluentColumns => {
        return {
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
            displayName: {
                label: nlsHPCC.Name,
                formatter: function (_name, row) {
                    return row.name;
                },
            },
            filesize: {
                label: nlsHPCC.Size, width: 100, sortable: false,
                justify: "right",
                formatter: (value, row) => {
                    return Utility.convertedSize(value);
                },
            },
            modifiedtime: { label: nlsHPCC.Date, width: 162, sortable: false }
        };
    }, []);

    const copyButtons = useCopyButtons(columns, selection, "landingZones");

    const refreshData = React.useCallback(() => {
        if (!lzPath || !selectedDropzone) return;
        const request: Partial<HPCCFileSpray.FileListRequest> = { Path: lzPath, };
        if (!isContainer) {
            request.Netaddr = selectedDropzone?.TpMachines?.TpMachine[0].Netaddress;
        } else {
            request.DropZoneName = selectedDropzone?.Name;
        }
        fsService.FileList(request).then(response => {
            const files = response?.files?.PhysicalFileStruct.filter(file => !file.isDir).map(file => {
                file["NetAddress"] = selectedDropzone?.TpMachines?.TpMachine[0].Netaddress ?? "";
                file["SourcePlane"] = isContainer ? selectedDropzone?.Name : file["NetAddress"];
                file["fullPath"] = [file.Path ?? response.Path, file.name].join(pathSep);
                return file;
            }) ?? [];
            setData(files);
        });
    }, [isContainer, lzPath, pathSep, selectedDropzone]);

    React.useEffect(() => {
        refreshData();
    }, [refreshData]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedFiles,
        items: selection.map(s => s.name),
        onSubmit: React.useCallback(() => {
            selection.forEach((item, idx) => {
                if (item._isUserFile) {
                    removeUserFile(item.name);
                } else {
                    FileSpray.DeleteDropZoneFile({
                        request: {
                            DropZoneName: selectedDropzone?.Name ?? "",
                            NetAddress: item?.NetAddress ?? "",
                            Path: item?.Path ?? "",
                            OS: item?.OS ?? "",
                            Names: item?.name ?? ""
                        },
                        load: function (response) {
                            refreshData();
                        }
                    });
                }
            });
        }, [refreshData, removeUserFile, selection, selectedDropzone])
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "preview", text: nlsHPCC.Preview, disabled: !selection.length, iconProps: { iconName: "ComplianceAudit" },
            onClick: () => {
                if (selection.length === 1) {
                    const logicalFile = "~file::" + selection[0].NetAddress + FileSpray.lfEncode(selection[0].fullPath);
                    window.location.href = `#/landingzone/preview/${logicalFile}`;
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
                    const url = `${ESPRequest.getBaseURL("FileSpray")}/DownloadFile?Name=${encodeURIComponent(item.name)}&NetAddress=${item.Server}&Path=${encodeURIComponent(item.Path)}&DropZoneName=${selectedDropzone.Name}`;
                    iframe.setSrc(frame, url, true);
                });
            }
        },
        {
            key: "delete", text: nlsHPCC.Delete, disabled: !selection.length, iconProps: { iconName: "Delete" },
            onClick: () => setShowDeleteConfirm(true)
        },
        { key: "divider_3", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "filter", text: nlsHPCC.Filter, iconProps: { iconName: hasFilter ? "FilterSolid" : "Filter" },
            onClick: () => setShowFilter(true)
        },
        { key: "divider_4", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "addFile", text: nlsHPCC.AddFile,
            onClick: () => setShowAddFile(true)
        },
        { key: "divider_5", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        { key: "importLabel", text: `${nlsHPCC.Import}: `, className: buttonStyles.labelOnly },
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
    ], [hasFilter, refreshData, selection, selectedDropzone?.Name, setShowDeleteConfirm]);

    //  Filter  ---
    const filterFields: Fields = {};
    for (const field in FilterFields) {
        filterFields[field] = { ...FilterFields[field], value: filter[field] };
    }

    const dropStyles = React.useMemo(() => mergeStyleSets({
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
    }), [showDropZone]);

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
        if (files.length > 0) {
            setUploadFiles(files);
            setShowFileUpload(true);
        }
    }, [setShowFileUpload, setUploadFiles]);

    const onOpenChange = React.useCallback((evt: TreeOpenChangeEvent, data: TreeOpenChangeData) => {
        const branchData = JSON.parse(data?.target?.dataset?.tree ?? "") ?? {};
        if (data.type === "Click" || data.type === "Enter") {
            const path = branchData.path[0] === "/" ? branchData.path : selectedDropzone.Path + "/" + branchData.path;
            if (path !== lzPath) {
                pushUrl(`#/landingzone/${path.replace(/\//g, "::")}`);
            }
            return;
        } else if (data.type === "ExpandIconClick" && data.open) {
            let items = Array.from(treeItems);
            if (branchData.path) {
                fsService.FileList({
                    DropZoneName: selectedDropzone.Name,
                    Path: branchData.path,
                    DirectoryOnly: true
                }).then(response => {
                    const files = response?.files?.PhysicalFileStruct?.sort((a, b) => {
                        if (a.name < b.name) return -1;
                        if (a.name > b.name) return 1;
                        return 0;
                    }) ?? [];
                    files?.forEach(file => {
                        const itemPath = file.Path + file.name;
                        if (items.filter(item => item.value === itemPath).length === 0) {
                            items.push({
                                value: itemPath,
                                parentValue: file.Path.slice(0, -1),
                                label: file.name,
                                icon: file.isDir ? BranchIcon.Directory : BranchIcon.None,
                                data: { path: itemPath }
                            });
                            if (file.isDir) {
                                items.push({
                                    value: itemPath + "__temp",
                                    parentValue: itemPath
                                });
                            }
                        }
                    });
                    items = items.filter(item => item.value.toString() !== branchData.path + "__temp");
                    setTreeItems(items);
                }).catch(err => {
                    logger.error(err);
                });
            } else {
                items = items.filter(item => item.value.toString() !== branchData.path + "__temp");
                setTreeItems(items);
            }
        }
        setOpenItems(data.openItems);
    }, [lzPath, selectedDropzone, treeItems]);

    return <HolyGrail
        header={<CommandBar items={buttons} farItems={copyButtons} />}
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
                <DockPanel hideSingleTabs={true} layout={layout} onDockPanelCreate={setDockpanel}>
                    <DockPanelItem key="lzDirectoryTree" title={nlsHPCC.Directories}>
                        <div style={{ height: "100%", width: "100%", overflowY: "scroll" }}>
                            <TreeView openItems={openItems} onOpenChange={onOpenChange} treeItems={treeItems} ariaLabel="Landing Zone directories" />
                        </div>
                    </DockPanelItem>
                    <DockPanelItem key="lzFileList" title={nlsHPCC.Files} location="split-right" relativeTo="lzDirectoryTree" >
                        <FluentGrid
                            data={data}
                            primaryID={"__hpcc_id"}
                            sort={{ attribute: "displayName", descending: false }}
                            columns={columns}
                            setSelection={setSelection}
                            setTotal={setTotal}
                            refresh={refreshTable}
                        ></FluentGrid>
                    </DockPanelItem>
                </DockPanel>
                <Filter
                    showFilter={showFilter} setShowFilter={setShowFilter}
                    filterFields={filterFields} onApply={pushParams}
                />
                {uploadFiles &&
                    <FileListForm
                        formMinWidth={360} selection={uploadFiles}
                        showForm={showFileUpload} setShowForm={setShowFileUpload}
                        onSubmit={() => refreshData()}
                    />
                }
                <AddFileForm
                    formMinWidth={620} refreshGrid={refreshData} addUserFile={addUserFile}
                    dropzone={selectedDropzone} showForm={showAddFile} setShowForm={setShowAddFile}
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
                <DeleteConfirm />
            </>
        }
    />;
};