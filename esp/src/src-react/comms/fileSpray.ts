import { FileSpray, FileSprayService, TopologyService, WsTopology } from "@hpcc-js/comms";
import { scopedLogger } from "@hpcc-js/util";
import nlsHPCC from "src/nlsHPCC";
import { Memory } from "src/store/Memory";

const logger = scopedLogger("src-react/comms/fileSpray.ts");

const fileSprayService = new FileSprayService({ baseUrl: "" });
const topologyService = new TopologyService({ baseUrl: "" });
const userAddedFiles: { [calculatedID: string]: FileListItem } = {};

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

export type DropZoneMachine = WsTopology.TpMachine;

export interface DropZone extends WsTopology.TpDropZone {
    machine: DropZoneMachine;
}

// union type for root tree items (drop zones or user files)
export type LandingZoneTreeItem = DropZone | FileListItem;

export interface FileListItem extends Omit<FileSpray.PhysicalFileStruct, "Server"> {
    calculatedID: string;
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

export interface LandingZonesStore extends Memory<DropZone> {
    loadData(): Promise<DropZone[]>;
}

export function CreateLandingZonesStore(): LandingZonesStore {
    const store = new Memory<DropZone>("calculatedID" as keyof DropZone) as LandingZonesStore;

    store.loadData = async () => {
        try {
            const response = await topologyService.TpDropZoneQuery({
                ECLWatchVisibleOnly: true
            });

            const dropZones = response?.TpDropZones?.TpDropZone || [];
            const data: DropZone[] = dropZones.map((zone: any): DropZone => {
                return {
                    ...zone,
                    calculatedID: zone.Path + "_" + zone.Name,
                    displayName: zone.Name,
                    type: "dropzone",
                    OS: zone.Linux === "true" ? 2 : 0,
                    fullPath: zone.Path + (zone.Path && !zone.Path.endsWith("/") ? "/" : ""),
                    DropZone: zone
                };
            });

            store.setData(data);
            return data;
        } catch (err) {
            const message = err?.Message ?? err?.message ?? err?.Exception?.[0]?.Message;
            if (message === nlsHPCC.GridAbortMessage) {
                logger.debug(message);
            } else {
                logger.error(err);
            }
            store.setData([]);
            return [];
        }
    };

    return store;
}

export interface LandingZonesFilterStore extends Memory<FileListItem> {
    loadData(): Promise<FileListItem[]>;
}

export function CreateLandingZonesFilterStore(dropZone: DropZone): LandingZonesFilterStore {
    const store = new Memory<FileListItem>("calculatedID" as keyof FileListItem) as LandingZonesFilterStore;

    store.loadData = async () => {
        try {
            const response = await fileSprayService.DropZoneFileSearch({});
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

            store.setData(data);
            return data;
        } catch (err) {
            logger.error(err);
            store.setData([]);
            return [];
        }
    };

    return store;
}

export interface FileListStoreParent {
    fullPath: string;
    NetAddress: string;
    OS: number;
    DropZone?: DropZone;
}

export interface FileListStore extends Memory<FileListItem> {
    loadData(): Promise<FileListItem[]>;
}

export function CreateFileListStore(parent?: FileListStoreParent): FileListStore {
    const store = new Memory<FileListItem>("calculatedID" as keyof FileListItem) as FileListStore;

    store.loadData = async () => {
        if (!parent) {
            store.setData([]);
            return [];
        } try {
            const fileListRequest = {
                DropZoneName: parent.DropZone?.Name,
                Netaddr: parent.NetAddress,
                Path: parent.fullPath,
                Mask: "",
                OS: parent.OS.toString()
            };

            const response = await fileSprayService.FileList(fileListRequest);
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

            const userFilesForThisDirectory = Object.values(userAddedFiles).filter(userFile => {
                const normalizedParentPath = parent.fullPath.replace(/\/$/, "");
                const matches = userFile.NetAddress === parent.NetAddress && userFile.fullFolderPath === normalizedParentPath;
                return matches;
            });
            const allData = [...data, ...userFilesForThisDirectory];
            store.setData(allData);
            return allData;
        } catch (err) {
            logger.error(err);
            store.setData([]);
            return [];
        }
    };

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
 * Replicates LandingZoneStore.addUserFile from FileSpray.ts
 * Just adds a file "reference" so it can be remotely sprayed etc.
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
                filesize: 0,
                modifiedtime: new Date().toISOString(),
                NetAddress: params.NetAddress,
                OS: machine.OS,
                fullPath: params.fullPath,
                fullFolderPath: params.fullPath.substring(0, params.fullPath.lastIndexOf("/") || params.fullPath.lastIndexOf("\\") || 0),
                DropZone: params.dropZone,
                displayName: fileName,
                type: "file",
                Path: params.fullPath.substring(0, params.fullPath.lastIndexOf("/") || params.fullPath.lastIndexOf("\\") || 0),
                Files: { PhysicalFileStruct: [] }, // empty files collection for single file
                _isUserFile: true,
                getLogicalFile: () => `~file::${params.NetAddress}${lfEncode(params.fullPath)}`
            };

            userAddedFiles[fileItem.calculatedID] = fileItem;
            resolve(fileItem);
        } catch (err) {
            logger.error(err);
            reject(err);
        }
    });
}

export function removeUserFile(file: FileListItem): void {
    delete userAddedFiles[file.calculatedID];
}

export function getUserFiles(): FileListItem[] {
    return Object.values(userAddedFiles);
}
