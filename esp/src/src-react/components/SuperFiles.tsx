import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps, ScrollablePane, Sticky } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { useFile } from "../hooks/file";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";

const defaultUIState = {
    hasSelection: false
};

interface SuperFilesProps {
    cluster: string;
    logicalFile: string;
}

export const SuperFiles: React.FunctionComponent<SuperFilesProps> = ({
    cluster,
    logicalFile
}) => {

    const [file, , , refreshData] = useFile(cluster, logicalFile);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const store = useConst(new Observable(new Memory("Name")));
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store,
        sort: [{ attribute: "Name", "descending": false }],
        filename: "superFiles",
        columns: {
            col1: selector({
                width: 27,
                selectorType: "checkbox"
            }),
            Name: { label: nlsHPCC.Name, sortable: true, },
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
                    window.location.href = `#/files/${cluster}/${selection[0].Name}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/files/${cluster}/${selection[i].Name}`, "_blank");
                    }
                }
            }
        },
    ], [cluster, refreshData, selection, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };

        if (selection.length) {
            state.hasSelection = true;
        }
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        if (file?.Superfiles?.DFULogicalFile) {
            store?.setData(file?.Superfiles?.DFULogicalFile);
            refreshTable();
        }
    }, [file, store, refreshTable]);

    return <ScrollablePane>
        <Sticky>
            <CommandBar items={buttons} farItems={copyButtons} />
        </Sticky>
        <Grid />
    </ScrollablePane>;
};
