import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useGrid } from "../hooks/grid";
import { useFile } from "../hooks/file";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";
import { selector } from "./DojoGrid";

function getStateImageName(row) {
    if (row.Complete) {
        return "workunit_completed.png";
    } else if (row.Running) {
        return "workunit_running.png";
    } else if (row.Failed) {
        return "workunit_failed.png";
    }
    return "workunit.png";
}

const defaultUIState = {
    hasSelection: false
};

interface FileDetailsGraphProps {
    cluster?: string;
    logicalFile: string;
}

export const FileDetailsGraph: React.FunctionComponent<FileDetailsGraphProps> = ({
    cluster,
    logicalFile
}) => {

    const [file, , _refresh] = useFile(cluster, logicalFile);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });

    //  Grid ---
    const store = useConst(new Observable(new Memory("Name")));
    const [Grid, selection, refreshTable, copyButtons] = useGrid({
        store,
        filename: "graphs",
        columns: {
            col1: selector({
                width: 27,
                selectorType: "checkbox"
            }),
            Name: {
                label: nlsHPCC.Name, sortable: true,
                formatter: function (Name, row) {
                    return Utility.getImageHTML(getStateImageName(row)) + `&nbsp;<a href='#/workunits/${file?.Wuid}/graphs/${Name}' onClick='return false;' class='dgrid-row-url'>${Name}</a>`;
                }
            }
        }
    });

    //  Command Bar  ---
    const buttons = React.useMemo((): ICommandBarItemProps[] => [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
        {
            key: "open", text: nlsHPCC.Open, disabled: !uiState.hasSelection, iconProps: { iconName: "WindowEdit" },
            onClick: () => {
                if (selection.length === 1) {
                    window.location.href = `#/workunits/${file?.Wuid}/graphs/${selection[0].Name}`;
                } else {
                    for (let i = 0; i < selection.length; ++i) {
                        window.open(`#/workunits/${file?.Wuid}/graphs/${selection[i].Name}`, "_blank");
                    }
                }
            }
        }
    ], [file?.Wuid, refreshTable, selection, uiState.hasSelection]);

    //  Selection  ---
    React.useEffect(() => {
        const state = { ...defaultUIState };
        state.hasSelection = selection.length > 0;
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        if (file?.Graphs?.ECLGraph) {
            store.setData(file?.Graphs?.ECLGraph.map(item => {
                return {
                    Name: item,
                    Label: "",
                    Completed: "",
                    Time: 0,
                    Type: ""
                };
            }));
            refreshTable();
        }
    }, [file?.Graphs?.ECLGraph, refreshTable, store]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={copyButtons} />}
        main={
            <Grid />
        }
    />;
};
