import * as React from "react";
import { FlatTree, useHeadlessFlatTree_unstable, HeadlessFlatTreeItemProps, TreeItem, TreeItemLayout, CounterBadge } from "@fluentui/react-components";
import { FluentIconsProps, FolderOpen20Regular, Folder20Regular, FolderOpen20Filled, Folder20Filled, Document20Regular, Document20Filled, Important16Regular } from "@fluentui/react-icons";
import { Archive, isAttribute, UNNAMED_QUERY } from "../util/metricArchive";

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
    filter?: string;
    matchCase?: boolean;
    selectedAttrIDs: string[];
    setSelectedItem: (eclId: string, scopeID: string[]) => void;
}

export const ECLArchiveTree: React.FunctionComponent<ECLArchiveTreeProps> = ({
    archive,
    filter = "",
    matchCase = false,
    selectedAttrIDs = [],
    setSelectedItem
}) => {

    const defaultOpenItems = React.useMemo(() => {
        return (archive?.modAttrs.filter(modAttr => modAttr.type === "Module") ?? []).map(modAttr => modAttr.id) ?? [];
    }, [archive?.modAttrs]);

    const [flatTreeItems, setFlatTreeItems] = React.useState<FlatItem[]>([]);
    const flatTree = useHeadlessFlatTree_unstable(flatTreeItems, { defaultOpenItems });

    React.useEffect(() => {
        let flatTreeItems: FlatItem[] = [];
        archive?.modAttrs.forEach(modAttr => {
            flatTreeItems.push({
                value: modAttr.id,
                parentValue: modAttr.parentId ? modAttr.parentId : undefined,
                content: modAttr.name,
                fileTimePct: isAttribute(modAttr) && Math.round((archive?.sourcePathTime(modAttr.sourcePath) / archive?.timeTotalExecute) * 100),
            });
        });
        if (archive?.query.content) {
            flatTreeItems.push({ value: UNNAMED_QUERY, parentValue: undefined, content: UNNAMED_QUERY });
        }
        if (filter !== "") {
            const matches = [];
            if (matchCase) {
                flatTreeItems.forEach(item => {
                    if (item.content.indexOf(filter) > -1) {
                        matches.push(item.content);
                        matches.push(item.parentValue?.toString());
                    }
                });
            } else {
                flatTreeItems.forEach(item => {
                    if (item.value.toString().toLowerCase().indexOf(filter) > -1) {
                        matches.push(item.value.toString());
                        matches.push(item.parentValue?.toString());
                    }
                });
            }
            flatTreeItems = flatTreeItems.filter(item => matches.includes(item.value.toString()) || matches.includes(item.content));
        }
        setFlatTreeItems(flatTreeItems.sort((a, b) => a.value.toString().localeCompare(b.value.toString(), undefined, { sensitivity: "base" })));
    }, [archive, archive?.modAttrs, archive?.timeTotalExecute, filter, matchCase]);

    const onClick = React.useCallback(evt => {
        const attrId = evt.currentTarget?.dataset?.fuiTreeItemValue;
        const modAttr = archive?.modAttrs.find(modAttr => modAttr.id === attrId);
        if (modAttr?.type === "Attribute") {
            setSelectedItem(attrId, archive.metricIDs(attrId));
        } else if (attrId === UNNAMED_QUERY) {
            setSelectedItem(attrId, []);
        }
    }, [archive, setSelectedItem]);

    const { ...treeProps } = flatTree.getTreeProps();
    return <div style={{ height: "100%", overflow: "auto", flexGrow: 1 }}>
        <FlatTree {...treeProps} size="small">
            {
                Array.from(flatTree.items(), flatTreeItem => {
                    const { fileTimePct, content, ...treeItemProps } = flatTreeItem.getTreeItemProps();
                    return <TreeItem key={flatTreeItem.value.toString()} {...treeItemProps} onClick={onClick}>
                        <TreeItemLayout
                            iconBefore={
                                flatTreeItem.itemType === "branch" ?
                                    (treeProps.openItems.has(flatTreeItem.value) ?
                                        selectedAttrIDs.some(attrId => typeof attrId === "string" && attrId.startsWith(content)) ? <FolderOpen20Filled /> : <FolderOpen20Regular /> :
                                        selectedAttrIDs.some(attrId => typeof attrId === "string" && attrId.startsWith(content)) ? <Folder20Filled /> : <Folder20Regular />) :
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
        </FlatTree>
    </div>;
};
