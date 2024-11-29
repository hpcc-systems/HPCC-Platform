import * as React from "react";
import { FlatTree, useHeadlessFlatTree_unstable, HeadlessFlatTreeItemProps, TreeItem, TreeItemLayout, CounterBadge } from "@fluentui/react-components";
import { FluentIconsProps, FolderOpen20Regular, Folder20Regular, FolderOpen20Filled, Folder20Filled, Document20Regular, Document20Filled, Important16Regular } from "@fluentui/react-icons";
import { Archive, isAttribute } from "../util/metricArchive";

type FlatItem = HeadlessFlatTreeItemProps & { fileTimePct?: number, content: string };

const iconStyleProps: FluentIconsProps = {
    primaryFill: "red",
};

const AsideContent = ({
    isImportant,
    messageCount,
}: {
    isImportant?: boolean;
    messageCount?: number;
}) => {
    const color = messageCount < 50 ? "brand" :
        messageCount < 70 ? "informative" :
            messageCount < 90 ? "important" :
                "danger";
    return <>
        {isImportant && <Important16Regular {...iconStyleProps} />}
        {!isNaN(messageCount) && messageCount > 0 && (
            <CounterBadge count={messageCount} color={color} size="small" />
        )}
    </>;
};

interface ECLArchiveTreeProps {
    archive?: Archive;
    selectedAttrIDs: string[];
    setSelectedItem: (eclId: string, scopeID: string[]) => void;
}

export const ECLArchiveTree: React.FunctionComponent<ECLArchiveTreeProps> = ({
    archive,
    selectedAttrIDs = [],
    setSelectedItem
}) => {

    const defaultOpenItems = React.useMemo(() => {
        return (archive?.modAttrs.filter(modAttr => modAttr.type === "Module") ?? []).map(modAttr => modAttr.id) ?? [];
    }, [archive?.modAttrs]);

    const [flatTreeItems, setFlatTreeItems] = React.useState<FlatItem[]>([]);
    const flatTree = useHeadlessFlatTree_unstable(flatTreeItems, { defaultOpenItems });

    React.useEffect(() => {
        const flatTreeItems: FlatItem[] = [];
        archive?.modAttrs.forEach(modAttr => {
            flatTreeItems.push({
                value: modAttr.id,
                parentValue: modAttr.parentId ? modAttr.parentId : undefined,
                content: modAttr.name,
                fileTimePct: isAttribute(modAttr) && Math.round((archive?.sourcePathTime(modAttr.sourcePath) / archive?.timeTotalExecute) * 100),
            });
        });
        setFlatTreeItems(flatTreeItems.sort((a, b) => a.value.toString().localeCompare(b.value.toString(), undefined, { sensitivity: "base" })));
    }, [archive, archive?.modAttrs, archive?.timeTotalExecute]);

    const onClick = React.useCallback(evt => {
        const attrId = evt.currentTarget?.dataset?.fuiTreeItemValue;
        const modAttr = archive?.modAttrs.find(modAttr => modAttr.id === attrId);
        if (modAttr?.type === "Attribute") {
            setSelectedItem(attrId, archive.metricIDs(attrId));
        }
    }, [archive, setSelectedItem]);

    const { ...treeProps } = flatTree.getTreeProps();
    return <div style={{ height: "100%", overflow: "auto" }}>
        <FlatTree {...treeProps} size="small">
            {
                Array.from(flatTree.items(), flatTreeItem => {
                    const { fileTimePct, content, ...treeItemProps } = flatTreeItem.getTreeItemProps();
                    return <TreeItem {...treeItemProps} onClick={onClick}>
                        <TreeItemLayout
                            iconBefore={
                                flatTreeItem.itemType === "branch" ?
                                    (treeProps.openItems.has(flatTreeItem.value) ?
                                        selectedAttrIDs.some(attrId => attrId.startsWith(content)) ? <FolderOpen20Filled /> : <FolderOpen20Regular /> :
                                        selectedAttrIDs.some(attrId => attrId.startsWith(content)) ? <Folder20Filled /> : <Folder20Regular />) :
                                    selectedAttrIDs.some(attrId => attrId === flatTreeItem.value) ?
                                        <Document20Filled /> :
                                        <Document20Regular />
                            }
                            aside={<AsideContent isImportant={false} messageCount={fileTimePct} />}
                        >
                            {content}
                        </TreeItemLayout>
                    </TreeItem>;
                })
            }
        </FlatTree >
    </div>;
};
