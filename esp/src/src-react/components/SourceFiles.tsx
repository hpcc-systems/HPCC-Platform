import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, Image, Link, ScrollablePane, Sticky } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as domClass from "dojo/dom-class";
import { AlphaNumSortMemory } from "src/Memory";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useFluentGrid } from "../hooks/grid";
import { useWorkunitSourceFiles } from "../hooks/workunit";
import { ShortVerticalDivider } from "./Common";
import { selector, tree } from "./DojoGrid";

const defaultUIState = {
    hasSelection: false
};

interface SourceFilesProps {
    wuid: string;
}

class TreeStore extends AlphaNumSortMemory {

    mayHaveChildren(item) {
        return item.IsSuperFile;
    }

    getChildren(parent, options) {
        return this.query({ __hpcc_parentName: parent.Name }, options);
    }
}

export const SourceFiles: React.FunctionComponent<SourceFilesProps> = ({
    wuid
}) => {

    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [sourceFiles, , , refreshData] = useWorkunitSourceFiles(wuid);

    //  Grid ---
    const store = useConst(new TreeStore("Name", { Name: true, Value: true }));
    const [Grid, selection, copyButtons] = useFluentGrid({
        store,
        sort: [{ attribute: "Name", "descending": false }],
        query: { __hpcc_parentName: "" },
        filename: "sourceFiles",
        columns: {
            col1: selector({
                width: 27,
                selectorType: "checkbox"
            }),
            Name: tree({
                label: "Name", sortable: true,
                formatter: function (Name, row) {
                    return <>
                        <Image src={Utility.getImageURL(row.IsSuperFile ? "folder_table.png" : "file.png")} className='iconAlign' />
                        &nbsp;
                        <Link href={`#/files/${row.FileCluster}/${Name}`}>{Name}</Link>
                    </>;
                }
            }),
            FileCluster: { label: nlsHPCC.FileCluster, width: 300, sortable: false },
            Count: {
                label: nlsHPCC.Usage, width: 72, sortable: true,
                renderCell: function (object, value, node, options) {
                    domClass.add(node, "justify-right");
                    node.innerText = Utility.valueCleanUp(value);
                },
            }
        }
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshData()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/files/${selection[0].Name}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/files/${selection[i].Name}`, "_blank");
                    }
                }
            }
        },
    ], [refreshData, selection, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        for (let i = 0; i < selection.length; ++i) {
            state.hasSelection = true;
            break;
        }
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        store.setData(sourceFiles);
    }, [store, sourceFiles]);

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        <Grid />
    </ScrollablePane>;
};
