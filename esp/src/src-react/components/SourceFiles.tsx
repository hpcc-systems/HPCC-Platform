import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import * as domClass from "dojo/dom-class";
import { AlphaNumSortMemory } from "src/Memory";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitSourceFiles } from "../hooks/Workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { createCopyDownloadSelection, ShortVerticalDivider } from "./Common";
import { DojoGrid, selector, tree } from "./DojoGrid";

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

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [variables] = useWorkunitSourceFiles(wuid);

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
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
    ];

    const rightButtons: ICommandBarItemProps[] = [
        ...createCopyDownloadSelection(grid, selection, "sourcefiles.csv")
    ];

    //  Grid ---
    const gridStore = useConst(new Observable(new TreeStore("Name", { Name: true, Value: true })));
    const gridSort = useConst([{ attribute: "Name", "descending": false }]);
    const gridQuery = useConst({ __hpcc_parentName: "" });
    const gridColumns = useConst({
        col1: selector({
            width: 27,
            selectorType: "checkbox"
        }),
        Name: tree({
            label: "Name", sortable: true,
            formatter: function (Name, row) {
                return Utility.getImageHTML(row.IsSuperFile ? "folder_table.png" : "file.png") + "&nbsp;<a href='#' onClick='return false;' class='dgrid-row-url'>" + Name + "</a>";
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
    });

    const refreshTable = (clearSelection = false) => {
        grid?.set("query", gridQuery);
        if (clearSelection) {
            grid?.clearSelection();
        }
    };

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
        gridStore.setData(variables);
        refreshTable();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gridStore, variables]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};
