import * as React from "react";
import { FlatTree, useHeadlessFlatTree_unstable, HeadlessFlatTreeItemProps, TreeItem, TreeItemLayout, TreeItemValue, TreeOpenChangeEvent, TreeOpenChangeData, TreeCheckedChangeData } from "@fluentui/react-components";
import { FolderOpen20Regular, Folder20Regular, FolderOpen20Filled, Folder20Filled, Document20Regular, Document20Filled } from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";

export type FlatTreeItem = HeadlessFlatTreeItemProps & { content_parens?: string };

const TreeIcon: React.FC<{ itemType: "branch" | "leaf"; isOpen: boolean; isSelected: boolean }> = React.memo(({ itemType, isOpen, isSelected }) => {
    if (itemType === "branch") {
        if (isOpen) {
            return isSelected ? <FolderOpen20Filled /> : <FolderOpen20Regular />;
        }
        return isSelected ? <Folder20Filled /> : <Folder20Regular />;
    }
    return isSelected ? <Document20Filled /> : <Document20Regular />;
});
TreeIcon.displayName = "TreeIcon";

interface FlatTreeExProps {
    treeItems?: FlatTreeItem[];
    selectedTreeValue?: TreeItemValue;
    checkedTreeValues: TreeItemValue[];
    openTreeValues?: Iterable<TreeItemValue>;
    setSelectedTreeValue: React.Dispatch<React.SetStateAction<TreeItemValue>>;
    setCheckedTreeValues: React.Dispatch<React.SetStateAction<TreeItemValue[]>>;
    setOpenTreeValues?: React.Dispatch<React.SetStateAction<Set<TreeItemValue>>>;
}

export const FlatTreeEx: React.FC<FlatTreeExProps> = React.memo(({
    treeItems = [],
    selectedTreeValue,
    checkedTreeValues,
    openTreeValues = [],
    setCheckedTreeValues,
    setSelectedTreeValue,
    setOpenTreeValues
}) => {

    const treeConfig = React.useMemo(() => ({
        openItems: openTreeValues,
        onOpenChange: (evt: TreeOpenChangeEvent, data: TreeOpenChangeData) => {
            setOpenTreeValues?.(data.openItems);
        },
        selectionMode: "multiselect" as const
    }), [openTreeValues, setOpenTreeValues]);

    const flatTree = useHeadlessFlatTree_unstable(treeItems, treeConfig);

    const onClick = React.useCallback((evt: React.MouseEvent<HTMLDivElement>) => {
        const value = evt.currentTarget.dataset.fuiTreeItemValue;
        if (value) {
            setSelectedTreeValue(value);
        }
    }, [setSelectedTreeValue]);

    const onCheckedChange = React.useCallback((_evt: React.ChangeEvent<HTMLElement>, data: TreeCheckedChangeData) => {
        setCheckedTreeValues(prevCheckedValues => {
            if (data.checked) {
                return prevCheckedValues.includes(data.value)
                    ? prevCheckedValues
                    : [...prevCheckedValues, data.value];
            } else {
                return prevCheckedValues.filter(value => value !== data.value);
            }
        });
    }, [setCheckedTreeValues]);

    const onOpenChange = React.useCallback((_evt: TreeOpenChangeEvent, data: TreeOpenChangeData) => {
        if (data.type === "Click" || data.type === "Enter") {
            setSelectedTreeValue(data.value);
            return;
        }
        setOpenTreeValues?.(data.openItems);
    }, [setOpenTreeValues, setSelectedTreeValue]);

    const treeProps = React.useMemo(() => flatTree.getTreeProps(), [flatTree]);

    const renderedItems = React.useMemo(() => {
        return Array.from(flatTree.items(), (flatTreeItem, index) => {
            const treeItemProps = flatTreeItem.getTreeItemProps();
            const isSelected = selectedTreeValue === flatTreeItem.value;
            const isOpen = treeProps.openItems.has(flatTreeItem.value);

            return (
                <TreeItem
                    key={flatTreeItem.value ?? `item_${index}`}
                    {...treeItemProps}
                    onClick={onClick}
                >
                    <TreeItemLayout
                        iconBefore={
                            <TreeIcon
                                itemType={flatTreeItem.itemType}
                                isOpen={isOpen}
                                isSelected={isSelected}
                            />
                        }
                    >
                        {treeItemProps.content}
                        {treeItemProps.content_parens && ` (${treeItemProps.content_parens})`}
                    </TreeItemLayout>
                </TreeItem>
            );
        });
    }, [flatTree, selectedTreeValue, treeProps.openItems, onClick]);

    return <div style={{ height: "100%", overflow: "auto" }}>
        <FlatTree
            {...treeProps}
            size="small"
            onCheckedChange={onCheckedChange}
            checkedItems={checkedTreeValues}
            openItems={openTreeValues}
            onOpenChange={onOpenChange}
            aria-label={nlsHPCC.Helpers}
        >
            {renderedItems}
        </FlatTree>
    </div>;
});
FlatTreeEx.displayName = "FlatTreeEx";
