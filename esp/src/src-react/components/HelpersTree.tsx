import * as React from "react";
import { FlatTree, useHeadlessFlatTree_unstable, HeadlessFlatTreeItemProps, TreeItem, TreeItemLayout, TreeItemValue, TreeOpenChangeEvent, TreeOpenChangeData } from "@fluentui/react-components";
import { FolderOpen20Regular, Folder20Regular, FolderOpen20Filled, Folder20Filled, Document20Regular, Document20Filled } from "@fluentui/react-icons";
import nlsHPCC from "src/nlsHPCC";
import { convertedSize } from "src/Utility";

export type FlatItem = HeadlessFlatTreeItemProps & { fileSize?: number, content: string, url?: string };

interface HelpersTreeProps {
    checkedItems: string[];
    selectedUrl?: string;
    treeItems?: FlatItem[];
    openItems?: Iterable<TreeItemValue>;
    setOpenItems?: (openItems: any) => void;
    setCheckedItems: (prevChecked: any) => void;
    setSelectedItem: (eclId: string) => void;
}

export const HelpersTree: React.FunctionComponent<HelpersTreeProps> = ({
    checkedItems,
    selectedUrl,
    treeItems = [],
    openItems = [],
    setOpenItems,
    setCheckedItems,
    setSelectedItem
}) => {

    const handleOpenChange = (evt: TreeOpenChangeEvent, data: TreeOpenChangeData) => {
        setOpenItems(data.openItems);
    };

    const flatTree = useHeadlessFlatTree_unstable(treeItems, { openItems, onOpenChange: handleOpenChange, selectionMode: "multiselect" });

    const onClick = React.useCallback((evt) => {
        if (evt.target.nodeName === "INPUT" || evt.currentTarget.dataset.url === "") return;
        const contentUrl = `${evt.currentTarget.dataset.name}?src=${encodeURIComponent(evt.currentTarget?.dataset?.url)}`;
        setSelectedItem(contentUrl);
    }, [setSelectedItem]);

    const checkChange = React.useCallback((evt, item) => {
        const isChecked = evt.target.checked;
        setCheckedItems((prevChecked) => {
            const selectedItem = treeItems.filter(i => item.value === i.value)[0];
            if (isChecked) {
                return [...prevChecked, selectedItem.value];
            } else {
                return prevChecked.filter(value => value !== item.value);
            }
        });
    }, [treeItems, setCheckedItems]);

    const { ...treeProps } = flatTree.getTreeProps();
    return <div style={{ height: "100%", overflow: "auto" }}>
        <FlatTree {...treeProps} size="small" onCheckedChange={checkChange} checkedItems={checkedItems} openItems={openItems} aria-label={nlsHPCC.Helpers}>
            {
                Array.from(flatTree.items(), (flatTreeItem, k) => {
                    const { fileSize, content, url, ...treeItemProps } = flatTreeItem.getTreeItemProps();
                    return <TreeItem key={`item_${k}`} {...treeItemProps} onClick={onClick} data-url={url} data-name={content}>
                        <TreeItemLayout
                            iconBefore={
                                flatTreeItem.itemType === "branch" ?
                                    (treeProps.openItems.has(flatTreeItem.value) ?
                                        selectedUrl === url ? <FolderOpen20Filled /> : <FolderOpen20Regular /> :
                                        selectedUrl === url ? <Folder20Filled /> : <Folder20Regular />) :
                                    selectedUrl === url ? <Document20Filled /> : <Document20Regular />
                            }
                        >
                            {content}{fileSize ? (` (${convertedSize(fileSize)})`) : ("")}
                        </TreeItemLayout>
                    </TreeItem>;
                })
            }
        </FlatTree >
    </div>;
};
