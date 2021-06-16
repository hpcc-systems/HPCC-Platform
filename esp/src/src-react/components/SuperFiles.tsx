import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../hooks/File";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";
import { DojoGrid, selector } from "./DojoGrid";

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

    const [file, , _refresh] = useFile(cluster, logicalFile);
    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

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
                    window.location.href = `#/files/${cluster}/${selection[0].Name}`;
                } else {
                    for (let i = selection.length - 1; i >= 0; --i) {
                        window.open(`#/files/${cluster}/${selection[i].Name}`, "_blank");
                    }
                }
            }
        },
    ];

    //  Grid ---
    const gridStore = useConst(new Observable(new Memory("Name")));
    const gridSort = useConst([{ attribute: "Name", "descending": false }]);
    const gridQuery = useConst({});
    const gridColumns = useConst({
        col1: selector({
            width: 27,
            selectorType: "checkbox"
        }),
        Name: { label: nlsHPCC.Name, sortable: true, },
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
        if (file) {
            gridStore.setData(file?.Superfiles?.DFULogicalFile);
            refreshTable();
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gridStore, file?.Superfiles?.DFULogicalFile]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={[]} />}
        main={
            <DojoGrid store={gridStore} query={gridQuery} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};
