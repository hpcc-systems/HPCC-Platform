import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { Memory } from "src/Memory";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useFile } from "../hooks/File";
import { HolyGrail } from "../layouts/HolyGrail";
import { createCopyDownloadSelection, ShortVerticalDivider } from "./Common";
import { DojoGrid, selector } from "./DojoGrid";

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
    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    // const [helpers] = useWorkunitHelpers(wuid);

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
                    window.location.href = `#/workunits/${file?.Wuid}/graphs/${selection[0].Name}`;
                } else {
                    for (let i = 0; i < selection.length; ++i) {
                        window.open(`#/workunits/${file?.Wuid}/graphs/${selection[i].Name}`, "_blank");
                    }
                }
            }
        }
    ];

    const rightButtons: ICommandBarItemProps[] = [
        ...createCopyDownloadSelection(grid, selection, "graphs.csv")
    ];

    //  Grid ---
    const gridStore = useConst(new Observable(new Memory("Name")));
    const gridQuery = useConst({});
    const gridColumns = useConst({
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
        state.hasSelection = selection.length > 0;
        setUIState(state);
    }, [selection]);

    React.useEffect(() => {
        if (file?.Graphs?.ECLGraph) {
            gridStore.setData(file?.Graphs?.ECLGraph.map(item => {
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
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gridStore, file?.Graphs?.ECLGraph]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <DojoGrid store={gridStore} query={gridQuery} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};
