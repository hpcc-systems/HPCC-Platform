import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { AlphaNumSortMemory } from "src/Memory";
import * as Utility from "src/Utility";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitVariables } from "../hooks/Workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { ShortVerticalDivider } from "./Common";
import { DojoGrid } from "./DojoGrid";

const defaultUIState = {
    hasSelection: false
};

interface VariablesProps {
    wuid: string;
}

export const Variables: React.FunctionComponent<VariablesProps> = ({
    wuid
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
    const [uiState, setUIState] = React.useState({ ...defaultUIState });
    const [variables] = useWorkunitVariables(wuid);

    //  Command Bar  ---
    const buttons: ICommandBarItemProps[] = [
        {
            key: "refresh", text: nlsHPCC.Refresh, iconProps: { iconName: "Refresh" },
            onClick: () => refreshTable()
        },
        { key: "divider_1", itemType: ContextualMenuItemType.Divider, onRender: () => <ShortVerticalDivider /> },
    ];

    const rightButtons: ICommandBarItemProps[] = [
        {
            key: "copy", text: nlsHPCC.CopyWUIDs, disabled: !uiState.hasSelection || !navigator?.clipboard?.writeText, iconOnly: true, iconProps: { iconName: "Copy" },
            onClick: () => {
                const wuids = selection.map(s => s.Wuid);
                navigator?.clipboard?.writeText(wuids.join("\n"));
            }
        },
        {
            key: "download", text: nlsHPCC.DownloadToCSV, disabled: !uiState.hasSelection, iconOnly: true, iconProps: { iconName: "Download" },
            onClick: () => {
                Utility.downloadToCSV(grid, selection.map(row => ([row.Protected, row.Wuid, row.Owner, row.Jobname, row.Cluster, row.RoxieCluster, row.State, row.TotalClusterTime])), "workunits.csv");
            }
        }
    ];

    //  Grid ---
    const gridStore = useConst(new Observable(new AlphaNumSortMemory("__hpcc_id", { Name: true, Value: true })));
    const gridSort = useConst([{ attribute: "Wuid", "descending": true }]);
    const gridColumns = useConst({
        Type: { label: nlsHPCC.Type, width: 180 },
        Name: { label: nlsHPCC.Name, width: 360 },
        Value: { label: nlsHPCC.Value }
    });

    const refreshTable = (clearSelection = false) => {
        grid?.set("query", {});
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
        gridStore.setData(variables.map((row, idx) => {
            return {
                __hpcc_id: idx,
                ...row
            };
        }));
        refreshTable();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [gridStore, variables]);

    return <HolyGrail
        header={<CommandBar items={buttons} overflowButtonProps={{}} farItems={rightButtons} />}
        main={
            <DojoGrid store={gridStore} query={{}} sort={gridSort} columns={gridColumns} setGrid={setGrid} setSelection={setSelection} />
        }
    />;
};
