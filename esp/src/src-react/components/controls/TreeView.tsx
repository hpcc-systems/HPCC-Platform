import * as React from "react";
import { mergeStyleSets } from "@fluentui/react";
import { FlatTree as Tree, TreeItem, TreeItemLayout, TreeItemValue, TreeOpenChangeData, TreeOpenChangeEvent, useHeadlessFlatTree_unstable, HeadlessFlatTreeItemProps, useId, tokens } from "@fluentui/react-components";
import { Database20Regular, Desktop20Regular, FolderOpenRegular, FolderRegular } from "@fluentui/react-icons";

export enum BranchIcon {
    None,
    Directory,
    Dropzone,
    Network,
}

const getIcon = (iconStr: BranchIcon, open: boolean): React.ReactElement | null => {
    switch (iconStr) {
        case BranchIcon.Directory:
            return open ? <FolderOpenRegular /> : <FolderRegular />;
        case BranchIcon.Dropzone:
            return <Database20Regular />;
        case BranchIcon.Network:
            return <Desktop20Regular />;
        case BranchIcon.None:
        default:
            return null;
    }
};

export type FlatItem = HeadlessFlatTreeItemProps & {
    label?: string,
    icon?: BranchIcon,
    data?: { [id: string]: any }
};

interface TreeViewProps {
    treeItems: FlatItem[];
    openItems: Iterable<TreeItemValue>;
    onOpenChange?: (_: TreeOpenChangeEvent, data: TreeOpenChangeData) => void;
    ariaLabel: string;
}

export const TreeView: React.FunctionComponent<TreeViewProps> = ({
    treeItems,
    openItems,
    onOpenChange,
    ariaLabel
}) => {

    const treeStyles = mergeStyleSets({
        focused: {
            background: tokens.colorBrandBackgroundInvertedPressed,
        }
    });

    const flatTree = useHeadlessFlatTree_unstable(treeItems, { onOpenChange: onOpenChange ? onOpenChange : null, openItems });
    const flatTreeProps = { ...flatTree.getTreeProps() };
    const key = useId("FileExplorer");
    if (!treeItems || treeItems.length < 1) return null;

    return <Tree key={key} aria-label={ariaLabel} {...flatTreeProps}>
        {Array.from(flatTree.items(), (item, idx) => {
            const { icon, label, data, ...treeItemProps } = item.getTreeItemProps();
            const open = flatTreeProps.openItems.has(item.value) ? true : false;
            const selected = Array.from(flatTreeProps.openItems).pop() === item.value;
            return <TreeItem key={`${label?.replace(/\s+/g, "")}_${idx}`} data-tree={JSON.stringify(data)} {...treeItemProps}>
                <TreeItemLayout className={selected ? treeStyles.focused : null} iconBefore={getIcon(icon, open)}>{label}</TreeItemLayout >
            </TreeItem>;
        })}
    </Tree>;

};