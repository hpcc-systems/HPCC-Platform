import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, mergeStyleSets } from "@fluentui/react";
import { useOnEvent } from "@fluentui/react-hooks";
import { FileSprayService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import * as iframe from "dojo/request/iframe";
import { TpDropZoneQuery } from "src/WsTopology";
import * as ESPRequest from "src/ESPRequest";
import nlsHPCC from "src/nlsHPCC";
import { deleteDropZoneFile, lfEncode } from "../comms/fileSpray";
import { useConfirm } from "../hooks/confirm";
import { useTreeData } from "../hooks/useLandingZoneTreeData";
import { useLandingZoneStore } from "../hooks/useLandingZoneStore";
import { HolyGrail } from "../layouts/HolyGrail";
import { pushParams } from "../util/history";
import { ShortVerticalDivider } from "./Common";
import { LandingZoneTreeTable, LandingZoneItem } from "./controls/LandingZoneTreeTable";
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
}

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

export const LandingZone: React.FunctionComponent<LandingZoneProps> = ({
    filter = emptyFilter
}) => {

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
    const [targetDropzones, setTargetDropzones] = React.useState([]);

    const [expandedNodes, setExpandedNodes] = React.useState<Set<string>>(new Set());
    const [selectedPaths, setSelectedPaths] = React.useState<string[]>([]);
    const [allDropZoneData, setAllDropZoneData] = React.useState<any[]>([]);

    const [loadingItems, setLoadingItems] = React.useState<Set<string>>(new Set());
    const [initialLoading, setInitialLoading] = React.useState(false);

    const { data: dropZoneData, loading: dataLoading, refresh: refreshDropZones } = useLandingZoneStore();

    React.useEffect(() => {
        TpDropZoneQuery({}).then(({ TpDropZoneQueryResponse }) => {
            setTargetDropzones(TpDropZoneQueryResponse?.TpDropZones?.TpDropZone || []);
        });
    }, []);

    React.useEffect(() => {
        setAllDropZoneData(dropZoneData);
    }, [dropZoneData]);

    // load specific files when a drop zone + machine is selected (via filter)
    React.useEffect(() => {
        if (filter?.DropZoneName && filter?.Server) {
            setInitialLoading(true);

            const fileSprayService = new FileSprayService({ baseUrl: "" });

            fileSprayService.DropZoneFileSearch({
                DropZoneName: filter.DropZoneName,
                Server: filter.Server,
                NameFilter: filter.NameFilter || "*",
                ECLWatchVisibleOnly: true
            }).then(response => {
                const files = response?.Files?.PhysicalFileStruct || [];
                const dropzone = targetDropzones.find(dz => dz.Name === filter.DropZoneName);
                const machine = dropzone?.TpMachines?.TpMachine?.find(m => m.ConfigNetaddress === filter.Server);

                if (machine) {
                    const data = files.map((file: any) => {
                        const fullPath = machine.Directory + "/" + (file.Path === null ? "" : (file.Path + "/")) + file.name;
                        const fullFolderPathParts = fullPath.split("/");
                        fullFolderPathParts.pop();
                        const netAddress = machine.Netaddress;

                        return {
                            ...file,
                            calculatedID: netAddress + fullPath,
                            NetAddress: netAddress,
                            OS: machine.OS,
                            fullPath,
                            fullFolderPath: fullFolderPathParts.join("/"),
                            DropZone: dropzone,
                            displayName: file.Path ? (file.Path + "/" + file.name) : file.name,
                            type: file.isDir ? "folder" : "file",
                            getLogicalFile: () => `~file::${netAddress}${file.fullPath}`
                        };
                    });

                    setAllDropZoneData(prevData => {
                        const newData = [...prevData];
                        data.forEach(file => {
                            newData.push(file);
                        });
                        return newData;
                    });

                    // auto-expand to show the selected dropzone and machine
                    const dropzone = targetDropzones.find(dz => dz.Name === filter.DropZoneName);
                    if (dropzone && filter.DropZoneName && filter.Server) {
                        const dropzoneId = `dropzone-${dropzone.Name}-${encodeURIComponent(dropzone.Path || "")}`;
                        const machineId = `machine-${filter.DropZoneName}-${filter.Server}`;
                        setExpandedNodes(new Set([dropzoneId, machineId]));
                    }
                }
                setInitialLoading(false);
            }).catch(() => {
                setInitialLoading(false);
            });
        }
    }, [filter, targetDropzones]);

    const handleExpansionChange = React.useCallback((expandedIds: Set<string>) => {
        const newlyExpanded = Array.from(expandedIds).filter(id => !expandedNodes.has(id));

        newlyExpanded.forEach(nodeId => {
            if (nodeId.startsWith("machine-")) {
                // extract dropzone and machine info from the nodeId
                // format: machine-{dropzoneName}-{machineAddress}
                const parts = nodeId.replace("machine-", "").split("-");
                if (parts.length >= 2) {
                    const dropzoneName = parts[0];
                    const machineAddress = parts.slice(1).join("-");
                    const dropzone = targetDropzones.find(dz => dz.Name === dropzoneName);
                    const machine = dropzone?.TpMachines?.TpMachine?.find(m =>
                        m.Netaddress === machineAddress || m.ConfigNetaddress === machineAddress
                    );

                    if (dropzone && machine) {
                        const machineId = `machine-${dropzoneName}-${machineAddress}`;
                        setLoadingItems(prev => new Set(prev).add(machineId));

                        const fileSprayService = new FileSprayService({ baseUrl: "" });

                        fileSprayService.FileList({
                            DropZoneName: dropzoneName,
                            Netaddr: machine.Netaddress,
                            Path: machine.Directory === "/" ? "/" : machine.Directory + "/",
                            Mask: "",
                            OS: machine.OS.toString()
                        }).then(response => {
                            const files = response?.files?.PhysicalFileStruct || [];

                            const transformedFiles = files.map((file: any) => {
                                const basePath = machine.Directory === "/" ? "" : machine.Directory;
                                const fullPath = basePath + "/" + file.name + (file.isDir ? "/" : "");

                                // fullFolderPath should be the Path field from the response (the directory containing this item)
                                const fullFolderPath = file.Path === "/" ? "/" : file.Path;

                                const netAddress = machine.Netaddress;

                                return {
                                    ...file,
                                    calculatedID: netAddress + fullPath,
                                    NetAddress: netAddress,
                                    OS: machine.OS,
                                    fullPath,
                                    fullFolderPath,
                                    DropZone: dropzone,
                                    displayName: file.name,
                                    type: file.isDir ? "folder" : "file",
                                    getLogicalFile: () => `~file::${netAddress}${lfEncode(fullPath)}`
                                };
                            });

                            setAllDropZoneData(prevData => {
                                const filteredData = prevData.filter(item =>
                                    !(item.NetAddress === machine.Netaddress && item.DropZone?.Name === dropzoneName)
                                );
                                return [...filteredData, ...transformedFiles];
                            });

                            setLoadingItems(prev => {
                                const newSet = new Set(prev);
                                newSet.delete(machineId);
                                return newSet;
                            });
                        }).catch(err => {
                            logger.error(err);
                            setLoadingItems(prev => {
                                const newSet = new Set(prev);
                                newSet.delete(machineId);
                                return newSet;
                            });
                        });
                    }
                }
            } else if (nodeId.startsWith("folder-")) {
                // the folder ID contains the machine address and full path: folder-{machineAddress}-{fullPath}
                // instead of trying to parse this complex format, find the folder data by matching the node ID
                const folderData = allDropZoneData.find(item => {
                    if (item.type !== "folder") return false;

                    const expectedId = `folder-${item.NetAddress}-${item.fullPath}`;
                    return expectedId === nodeId;
                });

                if (folderData) {
                    const folderId = nodeId; // nodeId is the folder ID
                    setLoadingItems(prev => new Set(prev).add(folderId));

                    const fileSprayService = new FileSprayService({ baseUrl: "" });

                    fileSprayService.FileList({
                        DropZoneName: folderData.DropZone.Name,
                        Netaddr: folderData.NetAddress,
                        Path: folderData.fullPath,
                        Mask: "",
                        OS: folderData.OS.toString()
                    }).then(response => {
                        const files = response?.files?.PhysicalFileStruct || [];

                        const transformedFiles = files.map((file: any) => {
                            const fullPath = folderData.fullPath + file.name + (file.isDir ? "/" : "");
                            // fullFolderPath should be the parent folder (without trailing slash)
                            const fullFolderPath = folderData.fullPath.replace(/\/$/, "");

                            return {
                                ...file,
                                calculatedID: folderData.NetAddress + fullPath,
                                NetAddress: folderData.NetAddress,
                                OS: folderData.OS,
                                fullPath,
                                fullFolderPath,
                                DropZone: folderData.DropZone,
                                displayName: file.name,
                                type: file.isDir ? "folder" : "file",
                                getLogicalFile: () => `~file::${folderData.NetAddress}${lfEncode(fullPath)}`
                            };
                        });

                        setAllDropZoneData(prevData => {
                            const folderPathWithoutSlash = folderData.fullPath.replace(/\/$/, "");
                            const filteredData = prevData.filter(item =>
                                !(item.fullFolderPath === folderPathWithoutSlash && item.NetAddress === folderData.NetAddress)
                            );
                            return [...filteredData, ...transformedFiles];
                        });

                        setLoadingItems(prev => {
                            const newSet = new Set(prev);
                            newSet.delete(folderId);
                            return newSet;
                        });
                    }).catch(err => {
                        logger.error(err);
                        setLoadingItems(prev => {
                            const newSet = new Set(prev);
                            newSet.delete(folderId);
                            return newSet;
                        });
                    });
                }
            }
        });

        setExpandedNodes(expandedIds);
    }, [expandedNodes, targetDropzones, allDropZoneData]);

    const {
        treeItems,
        selectedItemIds,
        expandedItemIds,
        onSelectionChange
    } = useTreeData({
        dropZones: targetDropzones,
        currentDropZone: filter?.DropZoneName || "",
        allData: allDropZoneData,
        selectedPaths,
        expandedNodes,
        onExpandedNodesChange: handleExpansionChange,
        onSelectionChange: setSelectedPaths
    });

    const selection = React.useMemo(() => {
        return treeItems.filter(item => selectedItemIds.has(item.id) && item.type === "file")
            .map(item => item.data);
    }, [treeItems, selectedItemIds]);

    const refreshTable = React.useCallback((clearSelection = false) => {
        if (clearSelection) {
            setSelectedPaths([]);
        }
        refreshDropZones();
    }, [refreshDropZones]);

    const refreshMachineFiles = React.useCallback((dropZoneName: string, netAddress: string) => {
        const dropzone = targetDropzones.find(dz => dz.Name === dropZoneName);
        const machine = dropzone?.TpMachines?.TpMachine?.find(m =>
            m.Netaddress === netAddress || m.ConfigNetaddress === netAddress
        );

        if (dropzone && machine) {
            const fileSprayService = new FileSprayService({ baseUrl: "" });

            fileSprayService.FileList({
                DropZoneName: dropZoneName,
                Netaddr: machine.Netaddress,
                Path: machine.Directory === "/" ? "/" : machine.Directory + "/",
                Mask: "",
                OS: machine.OS.toString()
            }).then(response => {
                const files = response?.files?.PhysicalFileStruct || [];

                const transformedFiles = files.map((file: any) => {
                    const basePath = machine.Directory === "/" ? "" : machine.Directory;
                    const fullPath = basePath + "/" + file.name + (file.isDir ? "/" : "");
                    const fullFolderPath = file.Path === "/" ? "/" : file.Path;

                    return {
                        ...file,
                        calculatedID: machine.Netaddress + fullPath,
                        NetAddress: machine.Netaddress,
                        OS: machine.OS,
                        fullPath,
                        fullFolderPath,
                        DropZone: dropzone,
                        displayName: file.name,
                        type: file.isDir ? "folder" : "file",
                        getLogicalFile: () => `~file::${machine.Netaddress}${lfEncode(fullPath)}`
                    };
                });

                setAllDropZoneData(prevData => {
                    const filteredData = prevData.filter(item =>
                        !(item.NetAddress === machine.Netaddress && item.DropZone?.Name === dropZoneName)
                    );
                    return [...filteredData, ...transformedFiles];
                });
            }).catch(err => {
                logger.error(err);
            });
        }
    }, [targetDropzones]);

    const copyButtons = React.useMemo(() => [], []);

    const handleItemAction = React.useCallback((item: LandingZoneItem, action: "download" | "delete") => {
        if (action === "download" && item.type === "file") {
            const downloadIframeName = "downloadIframe_" + item.data.calculatedID;
            const frame = iframe.create(downloadIframeName);
            const url = `${ESPRequest.getBaseURL("FileSpray")}/DownloadFile?Name=${encodeURIComponent(item.data.name)}&NetAddress=${item.data.NetAddress}&Path=${encodeURIComponent(item.data.fullFolderPath)}&OS=${item.data.OS}&DropZoneName=${item.data.DropZone.Name}`;
            iframe.setSrc(frame, url, true);
        } else if (action === "delete" && item.type === "file") {
            deleteDropZoneFile({
                DropZoneName: item.data.DropZone.Name,
                NetAddress: item.data.NetAddress,
                Path: item.data.fullFolderPath,
                OS: item.data.OS,
                Names: item.data.name
            }).then(() => {
                setSelectedPaths([]);
                refreshMachineFiles(item.data.DropZone.Name, item.data.NetAddress);
            });
        }
    }, [refreshMachineFiles, setSelectedPaths]);

    const [DeleteConfirm, setShowDeleteConfirm] = useConfirm({
        title: nlsHPCC.Delete,
        message: nlsHPCC.DeleteSelectedFiles,
        items: selection.map(s => s.name),
        onSubmit: React.useCallback(() => {
            const machineGroups = new Map<string, any[]>();
            selection.forEach((item) => {
                const key = `${item.DropZone.Name}::${item.NetAddress}`;
                if (!machineGroups.has(key)) {
                    machineGroups.set(key, []);
                }
                machineGroups.get(key).push(item);
            });

            setSelectedPaths([]);

            Array.from(machineGroups.entries()).forEach(([key, items]) => {
                const [dropZoneName, netAddress] = key.split("::");

                const deletePromises = items.map(item =>
                    deleteDropZoneFile({
                        DropZoneName: item.DropZone.Name,
                        NetAddress: item.NetAddress,
                        Path: item.fullFolderPath,
                        OS: item.OS,
                        Names: item.name
                    })
                );

                Promise.all(deletePromises).then(() => {
                    refreshMachineFiles(dropZoneName, netAddress);
                }).catch(err => {
                    logger.error(err);
                    refreshMachineFiles(dropZoneName, netAddress);
                });
            });
        }, [refreshMachineFiles, selection, setSelectedPaths])
    });

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
                    const url = `${ESPRequest.getBaseURL("FileSpray")}/DownloadFile?Name=${encodeURIComponent(item.name)}&NetAddress=${item.NetAddress}&Path=${encodeURIComponent(item.fullFolderPath)}&OS=${item.OS}&DropZoneName=${item.DropZone.Name}`;
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
    ], [hasFilter, refreshTable, selection, setShowDeleteConfirm]);

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
                <LandingZoneTreeTable
                    items={treeItems}
                    loading={dataLoading || initialLoading}
                    loadingItems={loadingItems}
                    selectedItems={selectedItemIds}
                    expandedItems={expandedItemIds}
                    onSelectionChange={onSelectionChange}
                    onExpansionChange={handleExpansionChange}
                    onItemAction={handleItemAction}
                />
                <Filter
                    showFilter={showFilter} setShowFilter={setShowFilter}
                    filterFields={filterFields} onApply={pushParams}
                />
                {uploadFiles &&
                    <FileListForm
                        formMinWidth={360} selection={uploadFiles}
                        showForm={showFileUpload} setShowForm={setShowFileUpload}
                        onSubmit={(dropzoneName, netAddress) => {
                            if (dropzoneName && netAddress) {
                                refreshMachineFiles(dropzoneName, netAddress);
                            }
                        }}
                    />
                }
                <AddFileForm
                    formMinWidth={620}
                    refreshGrid={refreshTable}
                    showForm={showAddFile} setShowForm={setShowAddFile}
                    dropzones={targetDropzones}
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