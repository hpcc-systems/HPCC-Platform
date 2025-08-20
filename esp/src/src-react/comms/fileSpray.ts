import { FileSprayService, TopologyService } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { Thenable } from "src/store/Deferred";
import { Paged } from "src/store/Paged";
import { BaseStore } from "src/store/Store";

const logger = scopedLogger("src-react/comms/fileSpray.ts");

const fileSprayService = new FileSprayService({ baseUrl: "" });
const topologyService = new TopologyService({ baseUrl: "" });

/**
 * Utility function to encode logical file paths
 * Replicates the lfEncode function from legacy FileSpray.ts
 */
export const lfEncode = (path: string): string => {
    let retVal = "";
    for (let i = 0; i < path.length; ++i) {
        switch (path[i]) {
            case "/":
            case "\\":
                retVal += "::";
                break;
            case "A":
            case "B":
            case "C":
            case "D":
            case "E":
            case "F":
            case "G":
            case "H":
            case "I":
            case "J":
            case "K":
            case "L":
            case "M":
            case "N":
            case "O":
            case "P":
            case "Q":
            case "R":
            case "S":
            case "T":
            case "U":
            case "V":
            case "W":
            case "X":
            case "Y":
            case "Z":
                retVal += "^" + path[i];
                break;
            default:
                retVal += path[i];
        }
    }
    return retVal;
};

export interface DropZoneMachine {
    Netaddress: string;
    ConfigNetaddress: string;
    Directory: string;
    OS: number;
}

export interface DropZone {
    Name: string;
    Path: string;
    Linux: string;
    machine: DropZoneMachine;
    TpMachines?: {
        TpMachine: DropZoneMachine[];
    };
}

export interface FileListItem {
    calculatedID: string;
    name: string;
    isDir: boolean;
    filesize?: number;
    modifiedtime?: string;
    NetAddress: string;
    OS: number;
    fullPath: string;
    fullFolderPath: string;
    DropZone: DropZone;
    displayName: string;
    type: string;
    _isUserFile?: boolean;
    getLogicalFile(): string;
}

export type LandingZonesStore = BaseStore<any, DropZone>;

export function CreateLandingZonesStore(): LandingZonesStore {
    const store = new Paged<any, DropZone>({
        start: "PageStartFrom",
        count: "PageSize"
    }, "calculatedID", (request, abortSignal): Thenable<{ data: DropZone[], total: number }> => {
        return topologyService.TpDropZoneQuery({
            ...request,
            ECLWatchVisibleOnly: true
        }).then(response => {
            const dropZones = response?.TpDropZones?.TpDropZone || [];
            const data = dropZones.map((zone: any): DropZone => {
                const processedZone = {
                    ...zone,
                    calculatedID: zone.Path + "_" + zone.Name,
                    displayName: zone.Name,
                    type: "dropzone",
                    OS: zone.Linux === "true" ? 2 : 0,
                    fullPath: zone.Path + (zone.Path && !zone.Path.endsWith("/") ? "/" : ""),
                    DropZone: zone
                };
                return processedZone;
            });

            return {
                data,
                total: data.length
            };
        }).catch(err => {
            const message = err?.Message ?? err?.message ?? err?.Exception?.[0]?.Message;
            if (message === nlsHPCC.GridAbortMessage) {
                logger.debug(message);
            } else {
                logger.error(err);
            }
            return {
                data: [],
                total: 0
            };
        });
    });

    const userAddedFiles: { [key: string]: FileListItem } = {};

    (store as any).addUserFile = (file: FileListItem) => {
        file._isUserFile = true;
        userAddedFiles[file.calculatedID] = file;
    };

    (store as any).removeUserFile = (file: FileListItem) => {
        delete userAddedFiles[file.calculatedID];
    };

    return store;
}

export type LandingZonesFilterStore = BaseStore<any, FileListItem>;

export function CreateLandingZonesFilterStore(dropZone: DropZone): LandingZonesFilterStore {
    const store = new Paged<any, FileListItem>({
        start: "PageStartFrom",
        count: "PageSize"
    }, "calculatedID", (request, abortSignal): Thenable<{ data: FileListItem[], total: number }> => {
        return fileSprayService.DropZoneFileSearch(request).then(response => {
            const files = response?.Files?.PhysicalFileStruct || [];
            const data = files.map((file: any): FileListItem => {
                const fullPath = dropZone.machine.Directory + "/" + (file.Path === null ? "" : (file.Path + "/")) + file.name;
                const fullFolderPathParts = fullPath.split("/");
                fullFolderPathParts.pop();
                const netAddress = dropZone.machine.Netaddress;

                return {
                    ...file,
                    calculatedID: netAddress + fullPath,
                    NetAddress: netAddress,
                    OS: dropZone.machine.OS,
                    fullPath,
                    fullFolderPath: fullFolderPathParts.join("/"),
                    DropZone: dropZone,
                    displayName: file.Path ? (file.Path + "/" + file.name) : file.name,
                    type: file.isDir ? "filteredFolder" : "file",
                    getLogicalFile: () => `~file::${netAddress}${lfEncode(fullPath)}`
                };
            });

            return {
                data,
                total: data.length
            };
        }).catch(err => {
            logger.error(err);
            return {
                data: [],
                total: 0
            };
        });
    });

    return store;
}

export interface FileListStoreParent {
    fullPath: string;
    NetAddress: string;
    OS: number;
    DropZone?: DropZone;
}

export type FileListStore = BaseStore<any, FileListItem>;

export function CreateFileListStore(parent?: FileListStoreParent): FileListStore {
    const store = new Paged<any, FileListItem>({
        start: "PageStartFrom",
        count: "PageSize"
    }, "calculatedID", (request, abortSignal): Thenable<{ data: FileListItem[], total: number }> => {
        if (!parent) {
            return Promise.resolve({ data: [], total: 0 });
        }

        const fileListRequest = {
            DropZoneName: parent.DropZone?.Name,
            Netaddr: parent.NetAddress,
            Path: parent.fullPath,
            Mask: "",
            OS: parent.OS.toString(),
            ...request
        };

        return fileSprayService.FileList(fileListRequest).then(response => {
            const files = response?.files?.PhysicalFileStruct || [];
            const data = files.map((file: any): FileListItem => {
                const fullPath = parent.fullPath + file.name + (file.isDir ? "/" : "");
                const fullFolderPathParts = fullPath.split("/");
                fullFolderPathParts.pop();

                return {
                    ...file,
                    calculatedID: parent.NetAddress + fullPath,
                    NetAddress: parent.NetAddress,
                    OS: parent.OS,
                    fullPath,
                    fullFolderPath: fullFolderPathParts.join("/"),
                    DropZone: parent.DropZone,
                    displayName: file.name,
                    type: file.isDir ? "folder" : "file",
                    getLogicalFile: () => `~file::${parent.NetAddress}${lfEncode(fullPath)}`
                };
            });

            return {
                data,
                total: data.length
            };
        }).catch(err => {
            logger.error(err);
            return {
                data: [],
                total: 0
            };
        });
    });

    return store;
}

/**
 * Delete a file from a drop zone
 * Replicates FileSpray.DeleteDropZoneFile functionality
 */
export function deleteDropZoneFile(params: {
    DropZoneName: string;
    NetAddress: string;
    Path: string;
    OS: number;
    Names: string;
}): Promise<any> {
    return fileSprayService.DeleteDropZoneFiles({
        DropZoneName: params.DropZoneName,
        NetAddress: params.NetAddress,
        Path: params.Path,
        OS: params.OS.toString(),
        Names: [params.Names] as any
    }).then(response => {
        if (response?.DFUActionResults?.DFUActionResult) {
            const result = response.DFUActionResults.DFUActionResult[0];
            const resultID = result.ID;
            const resultMessage = result.Result;

            if (resultMessage.indexOf("Success") === 0) {
                logger.info(`${nlsHPCC.Delete} ${resultID}: ${resultMessage}`);
            } else {
                logger.error(`${nlsHPCC.Delete} ${resultID}: ${resultMessage}`);
            }
        }
        return response;
    });
}

/**
 * Add a user file to the landing zone
 * This creates a file entry that will be displayed in the tree
 */
export function addUserFile(params: {
    NetAddress: string;
    fullPath: string;
    dropZone: DropZone;
}): Promise<FileListItem> {
    return new Promise((resolve, reject) => {
        try {
            let fullPathParts = params.fullPath.split("/");
            if (fullPathParts.length === 1) {
                fullPathParts = params.fullPath.split("\\");
            }
            const fileName = fullPathParts[fullPathParts.length - 1];

            const machine = params.dropZone.TpMachines?.TpMachine?.find(m =>
                m.Netaddress === params.NetAddress ||
                m.ConfigNetaddress === params.NetAddress
            );

            if (!machine) {
                reject(new Error(`${nlsHPCC.MachineNotFoundFor} NetAddress: ${params.NetAddress}`));
                return;
            }

            const fileItem: FileListItem = {
                calculatedID: params.NetAddress + params.fullPath,
                name: fileName,
                isDir: false,
                NetAddress: params.NetAddress,
                OS: machine.OS,
                fullPath: params.fullPath,
                fullFolderPath: params.fullPath.substring(0, params.fullPath.lastIndexOf("/") || params.fullPath.lastIndexOf("\\") || 0),
                DropZone: params.dropZone,
                displayName: fileName,
                type: "file",
                _isUserFile: true,
                getLogicalFile: () => `~file::${params.NetAddress}${lfEncode(params.fullPath)}`
            };

            logger.info(`${nlsHPCC.Added}: ${fileName} (${params.fullPath})`);
            resolve(fileItem);
        } catch (err) {
            logger.error(err);
            reject(err);
        }
    });
}
