import * as React from "react";
import { LandingZoneItem } from "../components/controls/LandingZoneTreeTable";

const sortItems = (a, b) => {
    // sort by type first (folders before files), then alphabetically by name
    if (a.type !== b.type) {
        return a.type === "folder" ? -1 : 1;
    }
    return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: "base" });
};

const createDropZoneId = (dz: any) => `dropzone-${dz.Name}-${encodeURIComponent(dz.Path || "")}`;

const createItemId = (type: "folder" | "file", netaddress: string, fullPath: string) => {
    return `${type}-${netaddress}-${fullPath}`;
};

const addFolderContents = (
    parentItem: any,
    parentItemId: string,
    machine: any,
    dropZone: any,
    level: number,
    allData: any[],
    expandedNodes: Set<string>,
    items: LandingZoneItem[]
): void => {
    const folderContents = allData.filter(item =>
        item.NetAddress === machine.Netaddress &&
        item.DropZone?.Name === dropZone.Name &&
        item.fullFolderPath === parentItem.fullPath.replace(/\/$/, "")
    ).sort(sortItems);

    folderContents.forEach(item => {
        const itemId = createItemId(item.type, machine.Netaddress, item.fullPath);

        items.push({
            id: itemId,
            name: item.name,
            displayName: item.displayName || item.name,
            type: item.type,
            parentId: parentItemId,
            level: level,
            hasChildren: item.type === "folder",
            isExpanded: expandedNodes.has(itemId),
            size: item.filesize,
            modifiedTime: item.modifiedtime,
            data: item
        });

        // recursively add nested contents if folder is expanded
        if (item.type === "folder" && expandedNodes.has(itemId)) {
            addFolderContents(item, itemId, machine, dropZone, level + 1, allData, expandedNodes, items);
        }
    });
};

const processMachine = (
    machine: any,
    dropZone: any,
    dropZoneId: string,
    allData: any[],
    expandedNodes: Set<string>,
    items: LandingZoneItem[]
): void => {
    const machineId = `machine-${dropZone.Name}-${machine.Netaddress}`;

    items.push({
        id: machineId,
        name: machine.Netaddress,
        displayName: machine.Netaddress,
        type: "machine",
        parentId: dropZoneId,
        level: 1,
        hasChildren: true,
        isExpanded: expandedNodes.has(machineId),
        data: { machine, dropZone }
    });

    // add files and folders if machine is expanded
    if (expandedNodes.has(machineId)) {
        const allMachineItems = allData.filter(item =>
            item.NetAddress === machine.Netaddress &&
            item.DropZone?.Name === dropZone.Name
        );

        // root level items should have fullFolderPath matching the machine's directory
        const machineDirectory = machine.Directory === "/" ? "/" : machine.Directory;
        const machineDirectoryWithSlash = machineDirectory.endsWith("/") ? machineDirectory : machineDirectory + "/";

        const machineItems = allMachineItems.filter(item => {
            const folderPath = item.fullFolderPath || "";
            return folderPath === machineDirectory || folderPath === machineDirectoryWithSlash;
        }).sort(sortItems);

        machineItems.forEach(item => {
            const itemId = createItemId(item.type, machine.Netaddress, item.fullPath);

            items.push({
                id: itemId,
                name: item.name,
                displayName: item.displayName || item.name,
                type: item.type,
                parentId: machineId,
                level: 2,
                hasChildren: item.type === "folder",
                size: item.filesize,
                modifiedTime: item.modifiedtime,
                data: item
            });

            // add folder contents if folder is expanded
            if (item.type === "folder" && expandedNodes.has(itemId)) {
                addFolderContents(item, itemId, machine, dropZone, 3, allData, expandedNodes, items);
            }
        });
    }
};

interface UseLandingZoneTreeItemsOptions {
    dropZones: any[];
    currentDropZone: string;
    allData: any[];
    expandedNodes: Set<string>;
}

export const useLandingZoneTreeItems = ({
    dropZones,
    currentDropZone,
    allData,
    expandedNodes
}: UseLandingZoneTreeItemsOptions) => {

    return React.useMemo((): LandingZoneItem[] => {
        const items: LandingZoneItem[] = [];

        const processDropZone = (dz: any) => {
            const dropZoneId = createDropZoneId(dz);
            items.push({
                id: dropZoneId,
                name: dz.Name,
                displayName: dz.Name,
                type: "dropzone",
                level: 0,
                hasChildren: dz.TpMachines?.TpMachine?.length > 0,
                isExpanded: expandedNodes.has(dropZoneId),
                data: dz
            });

            // add machines and files for the dropzone if expanded
            if (expandedNodes.has(dropZoneId) && dz.TpMachines?.TpMachine) {
                dz.TpMachines.TpMachine.forEach((machine: any) => {
                    processMachine(machine, dz, dropZoneId, allData, expandedNodes, items);
                });
            }
        };

        // if no dropzone is selected, show all available dropzones
        if (!currentDropZone) {
            dropZones.forEach(processDropZone);
        } else {
            // single dropzone view, show the selected dropzone and its content
            const selectedDZ = dropZones.find(dz => dz.Name === currentDropZone);
            if (selectedDZ) {
                processDropZone(selectedDZ);
            }
        }

        const userFiles = allData.filter(item => item._isUserFile === true);

        userFiles.forEach(userFile => {
            const fileId = `userfile-${userFile.calculatedID}`;
            items.push({
                id: fileId,
                name: userFile.displayName || userFile.name,
                displayName: userFile.displayName || userFile.name,
                type: "file",
                level: 0,
                hasChildren: false,
                isExpanded: false,
                data: userFile
            });
        });

        return items;
    }, [dropZones, currentDropZone, allData, expandedNodes]);
};

interface UseTreeDataOptions {
    dropZones: any[];
    currentDropZone: string;
    allData: any[];
    selectedPaths: string[];
    expandedNodes: Set<string>;
    onExpandedNodesChange: (expanded: Set<string>) => void;
    onSelectionChange: (selection: string[]) => void;
}

export const useTreeData = ({
    dropZones,
    currentDropZone,
    allData,
    selectedPaths,
    expandedNodes,
    onExpandedNodesChange,
    onSelectionChange
}: UseTreeDataOptions) => {

    const [selectedItemIds, setSelectedItemIds] = React.useState<Set<string>>(new Set());

    const treeItems = useLandingZoneTreeItems({ dropZones, currentDropZone, allData, expandedNodes });

    React.useEffect(() => {
        const newSelection = new Set<string>();
        selectedPaths.forEach(path => {
            const matchingItem = treeItems.find(item =>
                item.type === "file" &&
                (item.data.fullPath === path || item.data.name === path)
            );
            if (matchingItem) {
                newSelection.add(matchingItem.id);
            }
        });
        setSelectedItemIds(newSelection);
    }, [selectedPaths, treeItems]);

    const handleSelectionChange = React.useCallback((selectedIds: Set<string>) => {
        setSelectedItemIds(selectedIds);

        const newPaths: string[] = [];
        selectedIds.forEach(id => {
            const item = treeItems.find(i => i.id === id);
            if (item && item.type === "file") {
                newPaths.push(item.data.fullPath || item.data.name);
            }
        });

        onSelectionChange(newPaths);
    }, [treeItems, onSelectionChange]);

    const handleExpansionChange = React.useCallback((expandedIds: Set<string>) => {
        onExpandedNodesChange(expandedIds);
    }, [onExpandedNodesChange]);

    return { treeItems, selectedItemIds, expandedItemIds: expandedNodes, onSelectionChange: handleSelectionChange, onExpansionChange: handleExpansionChange };
};
