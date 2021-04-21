import * as React from "react";
import { CommandBar, ContextualMenuItemType, ICommandBarItemProps } from "@fluentui/react";
import { useConst } from "@fluentui/react-hooks";
import * as Observable from "dojo/store/Observable";
import { AlphaNumSortMemory } from "src/Memory";
import nlsHPCC from "src/nlsHPCC";
import { useWorkunitVariables } from "../hooks/Workunit";
import { HolyGrail } from "../layouts/HolyGrail";
import { createCopyDownloadSelection, ShortVerticalDivider } from "./Common";
import { DojoGrid } from "./DojoGrid";

interface VariablesProps {
    wuid: string;
}

export const Variables: React.FunctionComponent<VariablesProps> = ({
    wuid
}) => {

    const [grid, setGrid] = React.useState<any>(undefined);
    const [selection, setSelection] = React.useState([]);
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
        ...createCopyDownloadSelection(grid, selection, "variables.csv")
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
