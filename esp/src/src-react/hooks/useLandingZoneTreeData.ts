import * as React from "react";
import { LandingZoneItem } from "../components/controls/LandingZoneTreeTable";

const sortItems = (a, b) => {
    // sort by type first (folders before files), then alphabetically by name
    if (a.type !== b.type) {
        return a.type === "folder" ? -1 : 1;
    }
    return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: "base" });
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

    const treeItems = React.useMemo((): LandingZoneItem[] => {
        const items: LandingZoneItem[] = [];

        const createDropZoneId = (dz: any) => `dropzone-${dz.Name}-${encodeURIComponent(dz.Path || "")}`;

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
                    const machineId = `machine-${dz.Name}-${machine.Netaddress}`;
                    items.push({
                        id: machineId,
                        name: machine.Netaddress,
                        displayName: machine.Netaddress,
                        type: "machine",
                        parentId: dropZoneId,
                        level: 1,
                        hasChildren: true,
                        isExpanded: expandedNodes.has(machineId),
                        data: { machine, dropZone: dz }
                    });

                    // add files and folders if machine is expanded
                    if (expandedNodes.has(machineId)) {
                        const allMachineItems = allData.filter(item =>
                            item.NetAddress === machine.Netaddress &&
                            item.DropZone?.Name === dz.Name
                        );

                        // root level items should have fullFolderPath matching the machine's directory
                        const machineDirectory = machine.Directory === "/" ? "/" : machine.Directory;
                        const machineDirectoryWithSlash = machineDirectory.endsWith("/") ? machineDirectory : machineDirectory + "/";

                        const machineItems = allMachineItems.filter(item => {
                            const folderPath = item.fullFolderPath || "";
                            const matches = folderPath === machineDirectory || folderPath === machineDirectoryWithSlash;
                            return matches;
                        }).sort(sortItems);

                        machineItems.forEach(item => {
                            const itemId = item.type === "folder"
                                ? `folder-${machine.Netaddress}-${item.fullPath}`
                                : `file-${machine.Netaddress}-${item.fullPath}`;

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
                                const folderContents = allData.filter(subItem =>
                                    subItem.NetAddress === machine.Netaddress &&
                                    subItem.DropZone?.Name === dz.Name &&
                                    subItem.fullFolderPath === item.fullPath.replace(/\/$/, "")
                                ).sort(sortItems);

                                folderContents.forEach(subItem => {
                                    const subItemId = subItem.type === "folder"
                                        ? `folder-${machine.Netaddress}-${subItem.fullPath}`
                                        : `file-${machine.Netaddress}-${subItem.fullPath}`;

                                    items.push({
                                        id: subItemId,
                                        name: subItem.name,
                                        displayName: subItem.displayName || subItem.name,
                                        type: subItem.type,
                                        parentId: itemId, // this should be the folder's ID, not the machine ID
                                        level: 3,
                                        hasChildren: subItem.type === "folder",
                                        isExpanded: expandedNodes.has(subItemId),
                                        size: subItem.filesize,
                                        modifiedTime: subItem.modifiedtime,
                                        data: subItem
                                    });

                                    if (subItem.type === "folder" && expandedNodes.has(subItemId)) {
                                        const addNestedContents = (parentItem: any, parentItemId: string, currentLevel: number) => {
                                            const nestedContents = allData.filter(nestedItem =>
                                                nestedItem.NetAddress === machine.Netaddress &&
                                                nestedItem.DropZone?.Name === dz.Name &&
                                                nestedItem.fullFolderPath === parentItem.fullPath.replace(/\/$/, "")
                                            ).sort(sortItems);

                                            nestedContents.forEach(nestedItem => {
                                                const nestedItemId = nestedItem.type === "folder"
                                                    ? `folder-${machine.Netaddress}-${nestedItem.fullPath}`
                                                    : `file-${machine.Netaddress}-${nestedItem.fullPath}`;

                                                items.push({
                                                    id: nestedItemId,
                                                    name: nestedItem.name,
                                                    displayName: nestedItem.displayName || nestedItem.name,
                                                    type: nestedItem.type,
                                                    parentId: parentItemId,
                                                    level: currentLevel + 1,
                                                    hasChildren: nestedItem.type === "folder",
                                                    isExpanded: expandedNodes.has(nestedItemId),
                                                    size: nestedItem.filesize,
                                                    modifiedTime: nestedItem.modifiedtime,
                                                    data: nestedItem
                                                });

                                                if (nestedItem.type === "folder" && expandedNodes.has(nestedItemId)) {
                                                    addNestedContents(nestedItem, nestedItemId, currentLevel + 1);
                                                }
                                            });
                                        };

                                        addNestedContents(subItem, subItemId, 3);
                                    }
                                });
                            }
                        });
                    }
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

        return items;
    }, [dropZones, currentDropZone, allData, expandedNodes]);

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
